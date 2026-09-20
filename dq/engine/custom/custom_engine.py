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


class CustomEngine(DQEngine):
    """Engine providing custom business-rule constraints.

    Supports four built-in constraint types:

    * ``DistinctnessByGroup`` -- validates distinct counts within groups
    * ``RateOfChange`` -- detects sudden value changes between consecutive rows
    * ``LookupBasedOnColumnNameList`` -- checks column names against a reference table
    * ``WideTablesNegativeValuesCheck`` -- finds negative values across wide tables
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
        summarymetrics = []
        for check in df_metrics_results.collect():
            summarymetrics.append(
                {
                    "check": check["name"],
                    "success": check["value"] == 1,
                    "details": check.asDict(recursive=True),
                }
            )

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
        _metric_results = []
        _check_verifications = []
        df_ref = self._sparkSession.sql("Select " + ref_columns + " from " + ref_table)
        colnameList = df_ref.rdd.flatMap(lambda x: x).collect()
        colList = list(map(str, colnameList))
        if source == "timeSeries":
            source = ""

        for column in dataframe.columns:
            if column in colList:
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
        window = (
            Window.partitionBy(*group_by).orderBy(F.col(sort_by)).rowsBetween(-1, 0)
        )
        for column in columns:
            dataframe = dataframe.withColumn(
                f"{column}_list", F.collect_list(column).over(window)
            )
        logger.debug(
            "Updated dataframe contains %d rows and %d columns",
            dataframe.count(),
            len(dataframe.columns),
        )
        df_final = dataframe.collect()
        _metric_results = []
        _check_verifications = []
        for row in df_final:
            for col in columns:
                value_list = row[f"{col}_list"]
                if value_list and len(value_list) == 2:
                    change_percentile = abs(
                        ((value_list[0] - value_list[1]) / value_list[0]) * 100
                    )
                    logger.debug(
                        "%s - value list is %s and chg %% is %s",
                        col,
                        value_list,
                        change_percentile,
                    )
                    self.__checkMinMaxThreshold(
                        _check_verifications,
                        _metric_results,
                        col,
                        dq_dimension,
                        constraint,
                        change_percentile,
                        group_by,
                        level,
                        max,
                        min,
                    )
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
        funcs = [F.countDistinct]
        exprs = [f(F.col(c)).alias(c) for f in funcs for c in columns]
        group_df = dataframe.groupBy(*group_by).agg(*exprs)
        data_collect = group_df.collect()
        _metric_results = []
        _check_verifications = []
        for row in data_collect:
            for col in columns:
                distinct_count = row[col]
                self.__checkMinMaxThreshold(
                    _check_verifications,
                    _metric_results,
                    col,
                    dq_dimension,
                    constraint,
                    distinct_count,
                    group_by,
                    level,
                    max,
                    min,
                )

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

    def __checkMinMaxThreshold(
        self,
        _check_verifications,
        _metric_results,
        col,
        dq_dimension,
        constraint,
        value,
        group_by,
        level,
        max,
        min,
    ):
        if min and value < min:
            _check_result = [
                "MultiColumn",
                f"{constraint} {group_by} for {col}",
                dq_dimension,
                0,
            ]
            _verification_result = [
                constraint,
                level,
                "Error",
                f"{constraint} {group_by} for {col}",
                "Failure",
                f"{value} is below the threshold - {min}",
            ]
            _check_verifications.append(_verification_result)
        elif max and value > max:
            _check_result = [
                "MultiColumn",
                f"{constraint} {group_by} for {col}",
                dq_dimension,
                0,
            ]
            _verification_result = [
                constraint,
                level,
                "Error",
                f"{constraint} {group_by} for {col}",
                "Failure",
                f"{value} is above the threshold - {max}",
            ]
            _check_verifications.append(_verification_result)
        else:
            _check_result = [
                "MultiColumn",
                f"{constraint} {group_by} for {col}",
                dq_dimension,
                1,
            ]
            _verification_result = [
                constraint,
                level,
                "Success",
                f"{constraint} {group_by} for {col}",
                "Success",
                f"{value} meets the threshold",
            ]
            _check_verifications.append(_verification_result)
        _metric_results.append(_check_result)
