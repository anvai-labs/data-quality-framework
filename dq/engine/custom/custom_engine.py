# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import Optional, List, Dict, Any, Tuple

from pydeequ.repository import ResultKey

from dq.engine.dq_engine import DQEngine

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.window import Window

from dq.utils import repository_utils, constants

logger = logging.getLogger(__name__)


def _violation_expression(value, threshold_min, threshold_max):
    """Build a violation predicate mirroring legacy truthiness rules.

    A threshold of ``0`` or ``None`` disables that bound, exactly like the
    previous per-row Python comparison. Null metric values never violate:
    the comparison stays null and aggregations skip it.
    """
    violation = None
    if threshold_min:
        violation = value < F.lit(threshold_min)
    if threshold_max:
        over = value > F.lit(threshold_max)
        violation = over if violation is None else violation | over
    return F.lit(False) if violation is None else violation


class CustomEngine(DQEngine):
    """Engine providing custom business-rule constraints.

    Supports four built-in constraint types:

    * ``DistinctnessByGroup`` -- validates distinct counts within groups
    * ``RateOfChange`` -- detects sudden value changes between consecutive rows
    * ``LookupBasedOnColumnNameList`` -- checks column names against a reference table
    * ``WideTablesNegativeValuesCheck`` -- finds negative values across wide tables

    Every constraint decides through distributed aggregations. The driver only
    receives bounded check summaries -- one metric per (constraint, column) and
    schema-sized name lookups -- never per-group or per-row dataset content.
    """

    def __init__(self, config, dqts: Optional[int] = None):
        self._config = config
        super().__init__(config, dqts)

    def apply(self, dataframe: DataFrame, repository=None) -> List[Dict[str, Any]]:
        """Apply custom constraint checks to the DataFrame.

        Args:
            dataframe: Spark DataFrame to validate.
            repository: Optional repository config for persisting metrics.

        Returns:
            List of metric dicts with ``check``, ``success``, ``details`` keys.
        """
        custom_checks = self._config.get("checks", {})
        self._sparkSession = dataframe.sparkSession
        _metrics_results = []
        _verification_results = []
        for check_config in custom_checks:
            columns = check_config.get("columns", None)
            group_by = check_config.get("group_by", None)
            dq_dimension = check_config.get("dq_dimension", "Compliance")
            constraint = check_config.get("constraint", None)
            min = check_config.get("min", None)
            max = check_config.get("max", None)
            level = check_config.get("level", None)
            sort_by = check_config.get("sort_by", None)
            ignore_columns = check_config.get("ignore_columns", None)
            ref_table = check_config.get("ref_table", None)
            ref_columns = check_config.get("ref_columns", None)
            source = check_config.get("source", None)
            if constraint == "DistinctnessByGroup":
                _results, _check_verification = self._check_distinctness_by_group(
                    dq_dimension,
                    constraint,
                    dataframe,
                    columns,
                    group_by,
                    min,
                    max,
                    level,
                )
            elif constraint == "RateOfChange":
                _results, _check_verification = self._check_rate_of_change(
                    dq_dimension,
                    constraint,
                    dataframe,
                    columns,
                    group_by,
                    sort_by,
                    min,
                    max,
                    level,
                )
            elif constraint == "LookupBasedOnColumnNameList":
                _results, _check_verification = self._lookupBasedOnColumnNameList(
                    dq_dimension,
                    constraint,
                    dataframe,
                    ref_table,
                    ref_columns,
                    level,
                    ignore_columns,
                    source,
                )
            elif constraint == "WideTablesNegativeValuesCheck":
                _results, _check_verification = self._check_for_negative_values(
                    dq_dimension, constraint, dataframe, level, ignore_columns, source
                )
            else:
                raise ValueError(f"Unsupported custom constraint: {constraint}")
            _metrics_results += _results
            _verification_results += _check_verification

        df_metrics_results = self._sparkSession.createDataFrame(
            _metrics_results, ["entity", "instance", "name", "value"]
        )
        df_check_verification_results = self._sparkSession.createDataFrame(
            _verification_results,
            [
                "check",
                "check_level",
                "check_status",
                "constraint",
                "constraint_status",
                "constraint_message",
            ],
        )

        if repository:
            current_milli_time = ResultKey.current_milli_time()
            repository_utils.save_to_repository(
                repository,
                df_metrics_results,
                constants.DQ_REPOSITORY_METRICS,
                current_milli_time,
            )
            repository_utils.save_to_repository(
                repository,
                df_check_verification_results,
                constants.DQ_REPOSITORY_VERIFICATIONS,
                current_milli_time,
            )
        summarymetrics = [
            {
                "check": row[2],
                "success": row[3] == 1,
                "details": {
                    "entity": row[0],
                    "instance": row[1],
                    "name": row[2],
                    "value": row[3],
                },
            }
            for row in _metrics_results
        ]

        return summarymetrics

    def _lookupBasedOnColumnNameList(
        self,
        dq_dimension,
        constraint,
        dataframe,
        ref_table=None,
        ref_columns=None,
        level="Warning",
        ignore_columns=None,
        source="timeSeries",
    ):
        """Check if DataFrame column names are present as rows in a reference table.

        Matches the schema-sized column-name list against the reference table
        through a distributed left-semi join and collects only the matched
        names, never the reference table's rows.

        Args:
            dq_dimension: Quality dimension label (e.g. ``"Accuracy"``).
            constraint: Constraint name for metric logging.
            dataframe: Spark DataFrame whose column names are validated.
            ref_table: Fully-qualified reference table name.
            ref_columns: Column in the reference table to look up against.
            level: Check severity level.
            ignore_columns: Columns to skip during validation.
            source: Source label for metric output.

        Returns:
            Tuple of (metric_results, check_verifications) lists.
        """
        logger.debug("Running LookupBasedOnColumnNameList constraint")
        if ignore_columns and len(ignore_columns) > 0:
            for col in ignore_columns:
                dataframe = dataframe.drop(col)
        reference = self._sparkSession.sql(
            "Select " + ref_columns + " from " + ref_table
        )
        reference_values = reference.select(
            F.coalesce(F.col(ref_columns).cast("string"), F.lit("None")).alias(
                "ref_value"
            )
        )
        column_names = dataframe.sparkSession.createDataFrame(
            [(name,) for name in dataframe.columns], ["column_name"]
        )
        matched_names = {
            row["column_name"]
            for row in column_names.join(
                reference_values,
                column_names["column_name"] == reference_values["ref_value"],
                "left_semi",
            ).collect()
        }
        if source == "timeSeries":
            source = ""

        _metric_results = []
        _check_verifications = []
        for column in dataframe.columns:
            if column in matched_names:
                _check_result = [
                    "MultiColumn",
                    f"{constraint} for {column} {source}",
                    dq_dimension,
                    1,
                ]
                _verification_result = [
                    constraint,
                    level,
                    "Success",
                    f"{constraint}  for {column} {source}",
                    "Success",
                    "Column found in the ref table",
                ]

            else:
                _check_result = [
                    "MultiColumn",
                    f"{constraint} for {column} {source}",
                    dq_dimension,
                    0,
                ]
                _verification_result = [
                    constraint,
                    level,
                    "Failure",
                    f"{constraint}  for {column} {source}",
                    "Failure",
                    "Column not found in the ref table",
                ]

            _check_verifications.append(_verification_result)
            _metric_results.append(_check_result)

        return _metric_results, _check_verifications

    def _check_for_negative_values(
        self,
        dq_dimension,
        constraint,
        dataframe,
        level="Warning",
        ignore_columns=None,
        source="timeSeries",
    ):
        """Check for negative values across all numeric columns in a wide table.

        Args:
            dq_dimension: Quality dimension label.
            constraint: Constraint name for metric logging.
            dataframe: Spark DataFrame to check.
            level: Check severity level.
            ignore_columns: Columns to skip.
            source: Source label for metric output.

        Returns:
            Tuple of (metric_results, check_verifications) lists.
        """
        logger.debug("Running WideTablesNegativeValuesCheck constraint")
        if ignore_columns and len(ignore_columns) > 0:
            for col in ignore_columns:
                dataframe = dataframe.drop(col)
        _metric_results = []
        _check_verifications = []
        columns_to_check = dataframe.columns
        negative_counts = (
            dataframe.select(
                [(F.sum((F.col(c) < 0).cast("int"))).alias(c) for c in columns_to_check]
            )
            .collect()[0]
            .asDict()
        )

        if source == "timeSeries":
            source = ""

        for column in dataframe.columns:
            if negative_counts[column] is not None:
                if (isinstance(negative_counts[column], int)) & (
                    negative_counts[column] > 0
                ):
                    _check_result = [
                        "MultiColumn",
                        f"{constraint} for {column} {source}",
                        dq_dimension,
                        0,
                    ]
                    _verification_result = [
                        constraint,
                        level,
                        "Failure",
                        f"{constraint}  for {column} {source}",
                        "Failure",
                        f"Negative values found for {column} {source}",
                    ]
                else:
                    _check_result = [
                        "MultiColumn",
                        f"{constraint} for {column} {source}",
                        dq_dimension,
                        1,
                    ]
                    _verification_result = [
                        constraint,
                        level,
                        "Success",
                        f"{constraint}  for {column} {source}",
                        "Success",
                        f"Rule NoNegative values passed for {column} {source}",
                    ]
            else:
                _check_result = [
                    "MultiColumn",
                    f"{constraint} for {column} {source}",
                    dq_dimension,
                    1,
                ]
                _verification_result = [
                    constraint,
                    level,
                    "Success",
                    f"{constraint}  for {column} {source}",
                    "Success",
                    f"Rule NoNegative values passed for {column} {source}",
                ]
            _check_verifications.append(_verification_result)
            _metric_results.append(_check_result)

        return _metric_results, _check_verifications

    def _check_rate_of_change(
        self,
        dq_dimension,
        constraint,
        dataframe,
        columns=None,
        group_by=None,
        sort_by=None,
        min=None,
        max=None,
        level="Warning",
    ):
        """Detect sudden rate-of-change spikes between consecutive rows.

        Consecutive-pair changes are computed with a distributed ``lag()``
        window expression and reduced to one bounded summary per column.
        A pair is not evaluable when either value is null or the baseline is
        zero; skipped pairs are reported instead of crashing the run.

        Args:
            dq_dimension: Quality dimension label.
            constraint: Constraint name for metric logging.
            dataframe: Spark DataFrame to check.
            columns: Columns to compute rate of change on.
            group_by: Partition columns for windowing.
            sort_by: Column to order rows within each partition.
            min: Minimum allowed percentage change.
            max: Maximum allowed percentage change.
            level: Check severity level.

        Returns:
            Tuple of (metric_results, check_verifications) lists.
        """
        logger.info(
            "Checking rate of change for '%s' by group '%s' and sort by '%s'",
            columns,
            group_by,
            sort_by,
        )
        window = Window.partitionBy(*group_by).orderBy(F.col(sort_by))
        projected = dataframe
        aggregate_exprs = []
        for index, column in enumerate(columns):
            has_previous = F.lag(F.lit(1)).over(window).isNotNull()
            previous = F.lag(F.col(column)).over(window)
            current = F.col(column)
            both_present = previous.isNotNull() & current.isNotNull()
            change = F.when(
                both_present & (previous != 0),
                F.abs((previous - current) / previous) * 100,
            )
            # Window expressions must materialize in a projection before the
            # global aggregation; Spark rejects them inside agg().
            projected = projected.withColumn(
                f"dq_roc_change_{index}", change
            ).withColumn(f"dq_roc_pair_{index}", has_previous.cast("int"))
            aggregate_exprs.extend(
                [
                    F.count(F.col(f"dq_roc_change_{index}")).alias(
                        f"dq_roc_evaluated_{index}"
                    ),
                    F.sum(F.col(f"dq_roc_pair_{index}")).alias(f"dq_roc_pairs_{index}"),
                    F.sum(
                        _violation_expression(
                            F.col(f"dq_roc_change_{index}"), min, max
                        ).cast("long")
                    ).alias(f"dq_roc_violations_{index}"),
                    F.min(F.col(f"dq_roc_change_{index}")).alias(f"dq_roc_min_{index}"),
                    F.max(F.col(f"dq_roc_change_{index}")).alias(f"dq_roc_max_{index}"),
                ]
            )
        summary = projected.agg(*aggregate_exprs).head()

        _metric_results = []
        _check_verifications = []
        for index, column in enumerate(columns):
            evaluated = int(summary[f"dq_roc_evaluated_{index}"] or 0)
            pairs_total = int(summary[f"dq_roc_pairs_{index}"] or 0)
            skipped = pairs_total - evaluated
            if pairs_total == 0:
                continue
            violations = int(summary[f"dq_roc_violations_{index}"] or 0)
            minimum_change = summary[f"dq_roc_min_{index}"]
            maximum_change = summary[f"dq_roc_max_{index}"]
            instance = f"{constraint} {group_by} for {column}"
            if violations:
                value = 0
                check_status = "Error"
                constraint_status = "Failure"
                constraint_message = (
                    f"{violations} violating of {evaluated} evaluated "
                    f"consecutive pairs ({minimum_change} to "
                    f"{maximum_change}%); {skipped} pairs skipped"
                )
            else:
                value = 1
                check_status = "Success"
                constraint_status = "Success"
                constraint_message = (
                    f"{evaluated} evaluated consecutive pairs within thresholds "
                    f"({minimum_change} to {maximum_change}%); "
                    f"{skipped} pairs skipped"
                )
            _check_result = ["MultiColumn", instance, dq_dimension, value]
            _verification_result = [
                constraint,
                level,
                check_status,
                instance,
                constraint_status,
                constraint_message,
            ]
            _check_verifications.append(_verification_result)
            _metric_results.append(_check_result)

        if len(_metric_results) == 0:
            _check_result = [
                "MultiColumn",
                f"{constraint} {group_by} for {columns}",
                dq_dimension,
                0,
            ]
            _verification_result = [
                constraint,
                level,
                "Success",
                f"{constraint} {group_by} for {columns}",
                "Success",
                "No suitable data to validate this rule",
            ]
            _check_verifications.append(_verification_result)
            _metric_results.append(_check_result)

        return _metric_results, _check_verifications

    def _check_distinctness_by_group(
        self,
        dq_dimension,
        constraint,
        dataframe,
        columns=None,
        group_by=None,
        min=None,
        max=None,
        level="Warning",
    ):
        """Validate distinct counts of columns within groups meet thresholds.

        Per-group distinct counts are reduced through a distributed minimum,
        maximum, and violation count, so the driver receives one bounded
        summary per column instead of one row per group.

        Args:
            dq_dimension: Quality dimension label.
            constraint: Constraint name for metric logging.
            dataframe: Spark DataFrame to check.
            columns: Columns to count distinct values for.
            group_by: Columns to group by.
            min: Minimum expected distinct count.
            max: Maximum expected distinct count.
            level: Check severity level.

        Returns:
            Tuple of (metric_results, check_verifications) lists.
        """
        logger.info(
            "Checking distinctness of columns '%s' by group '%s'", columns, group_by
        )
        grouped_counts = dataframe.groupBy(*group_by).agg(
            *[
                F.countDistinct(F.col(column)).alias(f"dq_distinct_{index}")
                for index, column in enumerate(columns)
            ]
        )
        aggregate_exprs = [F.count(F.lit(1)).alias("dq_group_count")]
        for index in range(len(columns)):
            distinct = F.col(f"dq_distinct_{index}")
            aggregate_exprs.extend(
                [
                    F.min(distinct).alias(f"dq_min_{index}"),
                    F.max(distinct).alias(f"dq_max_{index}"),
                    F.sum(_violation_expression(distinct, min, max).cast("long")).alias(
                        f"dq_violations_{index}"
                    ),
                ]
            )
        summary = grouped_counts.agg(*aggregate_exprs).head()
        group_count = int(summary["dq_group_count"] or 0)

        _metric_results = []
        _check_verifications = []
        for index, column in enumerate(columns):
            if group_count == 0:
                break
            minimum = summary[f"dq_min_{index}"]
            maximum = summary[f"dq_max_{index}"]
            violations = int(summary[f"dq_violations_{index}"] or 0)
            instance = f"{constraint} {group_by} for {column}"
            if violations:
                value = 0
                check_status = "Error"
                constraint_status = "Failure"
                if min and minimum < min:
                    constraint_message = (
                        f"{violations} of {group_count} groups below the "
                        f"threshold - {min} (lowest observed {minimum})"
                    )
                else:
                    constraint_message = (
                        f"{violations} of {group_count} groups above the "
                        f"threshold - {max} (highest observed {maximum})"
                    )
            else:
                value = 1
                check_status = "Success"
                constraint_status = "Success"
                constraint_message = (
                    f"distinct counts of {group_count} groups in "
                    f"[{minimum}, {maximum}] meet the thresholds"
                )
            _check_result = ["MultiColumn", instance, dq_dimension, value]
            _verification_result = [
                constraint,
                level,
                check_status,
                instance,
                constraint_status,
                constraint_message,
            ]
            _check_verifications.append(_verification_result)
            _metric_results.append(_check_result)

        if len(_metric_results) == 0:
            _check_result = [
                "MultiColumn",
                f"{constraint} {group_by} for {','.join(columns)}",
                dq_dimension,
                1,
            ]
            _verification_result = [
                constraint,
                level,
                "Success",
                f"{constraint} {group_by} for {','.join(columns)}",
                "Success",
                "No suitable data to validate this rule",
            ]
            _check_verifications.append(_verification_result)
            _metric_results.append(_check_result)
        return _metric_results, _check_verifications
