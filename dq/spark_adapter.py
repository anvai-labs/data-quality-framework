# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Native Spark adapter executing portable count plans over shared aggregations.

Counts/v1 semantics are owned by ``dq.plan``. This adapter computes only global
counts: one aggregate action per bound dataset with every required metric
combined, so duplicate metric requests share a single scan and the driver
receives exactly one aggregate row per dataset — never dataset rows.
Unsupported capabilities and missing, extra, non-DataFrame, or case-ambiguous
bindings fail before any Spark job runs. ``DatasetRef`` digests certify
caller-supplied snapshot identity; this adapter does not verify content.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING

from dq.exceptions import ConfigurationError
from dq.outcomes import CheckOutcome
from dq.plan import CapabilitySet, ExecutionPlan, MetricKind

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

ADAPTER_NAME = "spark"
ADAPTER_VERSION = "1"
CAPABILITIES = CapabilitySet(ADAPTER_NAME, ADAPTER_VERSION, frozenset(MetricKind))

_MAX_LISTED_COLUMNS = 8


def execute_plan(
    plan: ExecutionPlan, datasets: Mapping[str, DataFrame]
) -> tuple[CheckOutcome, ...]:
    """Compute the plan's metrics with Spark and evaluate exact outcomes."""
    plan.validate_for(CAPABILITIES)
    bindings = _validated_bindings(plan, datasets)
    values = {}
    for name in sorted(bindings):
        values.update(_dataset_counts(name, bindings[name], plan))
    return plan.evaluate(values, CAPABILITIES)


def _validated_bindings(plan: ExecutionPlan, datasets) -> dict[str, DataFrame]:
    if not isinstance(datasets, Mapping):
        raise ConfigurationError(
            "dataset bindings require a mapping of dataset names to DataFrames"
        )
    bindings = dict(datasets)
    expected = {metric.dataset.name for metric in plan.metrics}
    missing = sorted(expected - set(bindings))
    unknown = sorted(set(bindings) - expected)
    if missing or unknown:
        detail = []
        if missing:
            detail.append(f"missing {missing}")
        if unknown:
            detail.append(f"unknown {unknown}")
        raise ConfigurationError(
            "dataset bindings must exactly match plan datasets: " + "; ".join(detail)
        )
    from pyspark.sql import DataFrame

    for name, dataframe in bindings.items():
        if not isinstance(dataframe, DataFrame):
            raise ConfigurationError(
                f"dataset binding {name!r} must be a Spark DataFrame"
            )
    return bindings


def _dataset_counts(name: str, dataframe, plan: ExecutionPlan) -> dict:
    from pyspark.sql import functions as F
    from pyspark.sql.types import DoubleType, FloatType

    fields = dataframe.schema.fields
    fields_by_name = {field.name: field for field in fields}
    duplicates = {
        field.name
        for field in fields
        if sum(other.name == field.name for other in fields) > 1
    }
    metrics = [metric for metric in plan.metrics if metric.dataset.name == name]

    aggregates = [F.count(F.lit(1)).alias("dq_row_count")]
    aliases = {}
    for index, metric in enumerate(metrics):
        if metric.kind is not MetricKind.PRESENT_COUNT or metric.column is None:
            continue
        column = metric.column.name
        field = fields_by_name.get(column)
        if field is None:
            available = ", ".join(sorted(fields_by_name)[:_MAX_LISTED_COLUMNS])
            raise ConfigurationError(
                f"column {column!r} in dataset {name!r} requires an exact "
                f"case-sensitive match; available columns: {available}"
            )
        if column in duplicates or any(
            other.lower() == column.lower()
            for other in fields_by_name
            if other != column
        ):
            raise ConfigurationError(
                f"column {column!r} in dataset {name!r} is ambiguous: duplicate or "
                "case-insensitive colliding fields cannot be resolved"
            )
        column_expr = F.col(column)
        condition = column_expr.isNotNull()
        if isinstance(field.dataType, (FloatType, DoubleType)):
            condition = ~(column_expr.isNull() | F.isnan(column_expr))
        alias = f"dq_present_{index}"
        aggregates.append(F.count(F.when(condition, F.lit(1))).alias(alias))
        aliases[metric] = alias

    row = dataframe.agg(*aggregates).head()
    values = {}
    for metric in metrics:
        if metric.kind is MetricKind.ROW_COUNT:
            values[metric] = int(row["dq_row_count"])
        else:
            values[metric] = int(row[aliases[metric]])
    return values
