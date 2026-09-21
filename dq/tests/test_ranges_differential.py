# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Differential gate for ranges/v1: adapter versus NonNegativeColumns (ADR-005).

The ranges/v1 non-negative rule (column minimum >= 0) and the legacy
``NonNegativeColumns`` constraint must decide identically per column over
shared fixtures, including nulls, NaN-only and all-null columns, and empty
input. Infinite bounds are a documented limitation on both paths' fixtures:
they are excluded here.
"""

import pytest

from decimal import Decimal

from pyspark.sql.types import (
    DoubleType,
    StructField,
    StructType,
)

from dq.engine.custom.strategies import NoNegativeValuesStrategy
from dq.plan import (
    ColumnRef,
    Comparison,
    DatasetRef,
    ExecutionPlan,
    MetricKind,
    Predicate,
    RuleKind,
    RuleSpec,
)
from dq.spark_adapter import execute_plan

pytestmark = pytest.mark.spark

DATASET = DatasetRef("bars", "a" * 64)
_COLUMNS = ["negative", "positive", "with_null", "with_nan", "all_nan", "all_null"]
_SCHEMA = StructType(
    [
        StructField("negative", DoubleType(), True),
        StructField("positive", DoubleType(), True),
        StructField("with_null", DoubleType(), True),
        StructField("with_nan", DoubleType(), True),
        StructField("all_nan", DoubleType(), True),
        StructField("all_null", DoubleType(), True),
    ]
)
_ROWS = [
    (-5.0, 3.0, 1.0, float("nan"), float("nan"), None),
    (3.0, 7.0, None, 4.0, float("nan"), None),
]
# Expected non-negative decisions: negative False; every other column True.


def legacy_decisions(frame):
    results, _ = NoNegativeValuesStrategy().apply(
        frame, "NoNegativeValues", "Compliance", "Warning"
    )
    decisions = {column: True for column in _COLUMNS}
    for _entity, instance, _name, value in results:
        column = instance.rsplit(" for ", 1)[-1].strip()
        if column in decisions:
            decisions[column] = decisions[column] and (value == 1)
    return decisions


def portable_decisions(frame):
    rules = [
        RuleSpec(
            f"{column}.non_negative",
            DATASET,
            RuleKind.VALUE_RANGE,
            Predicate(Comparison.GE, Decimal(0)),
            ColumnRef(column),
            target=MetricKind.COLUMN_MIN,
        )
        for column in _COLUMNS
    ]
    outcomes = execute_plan(ExecutionPlan(tuple(rules)), {"bars": frame})
    decisions = {column: True for column in _COLUMNS}
    for outcome in outcomes:
        details = outcome.to_legacy()["details"]
        decisions[details["column"]] = (
            decisions[details["column"]] and outcome.to_legacy()["success"]
        )
    return decisions


def test_non_negative_decisions_identically(spark):
    frame = spark.createDataFrame(_ROWS, _SCHEMA)
    assert (
        legacy_decisions(frame)
        == portable_decisions(frame)
        == {
            "negative": False,
            "positive": True,
            "with_null": True,
            "with_nan": True,
            "all_nan": True,
            "all_null": True,
        }
    )


def test_empty_input_decides_identically(spark):
    empty = spark.createDataFrame([], _SCHEMA)
    assert (
        legacy_decisions(empty)
        == portable_decisions(empty)
        == {column: True for column in _COLUMNS}
    )
