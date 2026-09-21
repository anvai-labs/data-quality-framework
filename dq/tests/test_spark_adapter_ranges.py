# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Spark contracts for ranges/v1: exact bounds over one aggregate per dataset.

Expected values are hand-computed; nulls and NaN are excluded from counts and
bounds, and infinite bounds fail closed.
"""

from decimal import Decimal

import pytest

from pyspark.sql.types import (
    DecimalType,
    DoubleType,
    IntegerType,
    StructField,
    StructType,
)

from dq.exceptions import ConfigurationError
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

DATASET = DatasetRef("bars", "f" * 64)

_ROWS = [
    (Decimal("5.00"), 0.5, 7, None),
    (Decimal("-2.50"), float("nan"), None, float("nan")),
    (None, 2.5, 7, float("nan")),
]
_SCHEMA = StructType(
    [
        StructField("amount", DecimalType(10, 2), True),
        StructField("score", DoubleType(), True),
        StructField("n", IntegerType(), True),
        StructField("all_nan", DoubleType(), True),
    ]
)
# Excluding nulls and NaN: amount count=2 min=-2.50 max=5.00;
# score count=2 min=0.5 max=2.5; n count=2 min=7 max=7; all_nan count=0.


def bound_rules():
    return (
        RuleSpec(
            "amount_min",
            DATASET,
            RuleKind.VALUE_RANGE,
            Predicate(Comparison.GE, Decimal("-3")),
            ColumnRef("amount"),
            target=MetricKind.COLUMN_MIN,
        ),
        RuleSpec(
            "amount_max",
            DATASET,
            RuleKind.VALUE_RANGE,
            Predicate(Comparison.LE, Decimal("6")),
            ColumnRef("amount"),
            target=MetricKind.COLUMN_MAX,
        ),
        RuleSpec(
            "score_min",
            DATASET,
            RuleKind.VALUE_RANGE,
            Predicate(Comparison.GE, Decimal("0.5")),
            ColumnRef("score"),
            target=MetricKind.COLUMN_MIN,
        ),
        RuleSpec(
            "score_max",
            DATASET,
            RuleKind.VALUE_RANGE,
            Predicate(Comparison.LE, Decimal("2")),
            ColumnRef("score"),
            target=MetricKind.COLUMN_MAX,
        ),
        RuleSpec(
            "n_min",
            DATASET,
            RuleKind.VALUE_RANGE,
            Predicate(Comparison.GE, Decimal("7")),
            ColumnRef("n"),
            target=MetricKind.COLUMN_MIN,
        ),
    )


def by_rule(outcomes):
    return {
        outcome.to_legacy()["details"]["rule_id"]: outcome.to_legacy()
        for outcome in outcomes
    }


def test_executes_bounds_with_exact_counts_ignoring_nulls_and_nan(spark):
    frame = spark.createDataFrame(_ROWS, _SCHEMA)
    outcomes = execute_plan(ExecutionPlan(bound_rules()), {"bars": frame})
    rules = by_rule(outcomes)
    assert rules["amount_min"]["success"] is True
    assert rules["amount_min"]["details"]["observed"] == {
        "numerator": -5,
        "denominator": 2,
    }
    assert rules["amount_max"]["success"] is True
    assert rules["amount_max"]["details"]["observed"] == {
        "numerator": 5,
        "denominator": 1,
    }
    assert rules["score_min"]["success"] is True
    assert rules["score_max"]["success"] is False, "2.5 exceeds the bound of 2"
    assert rules["n_min"]["success"] is True
    assert rules["n_min"]["details"]["semantic_version"] == "ranges/v1"


def test_all_nan_column_passes_vacuously(spark):
    frame = spark.createDataFrame(_ROWS, _SCHEMA)
    plan = ExecutionPlan(
        (
            RuleSpec(
                "all_nan_min",
                DATASET,
                RuleKind.VALUE_RANGE,
                Predicate(Comparison.GE, Decimal("0")),
                ColumnRef("all_nan"),
                target=MetricKind.COLUMN_MIN,
            ),
        )
    )
    outcome = by_rule(execute_plan(plan, {"bars": frame}))["all_nan_min"]
    assert outcome["success"] is True
    assert outcome["details"]["reason"] == "no_values"


def test_infinite_bounds_fail_closed(spark):
    frame = spark.createDataFrame([(float("inf"),), (float("-inf"),)], "score double")
    plan = ExecutionPlan(
        (
            RuleSpec(
                "score_min",
                DATASET,
                RuleKind.VALUE_RANGE,
                Predicate(Comparison.GE, Decimal("0")),
                ColumnRef("score"),
                target=MetricKind.COLUMN_MIN,
            ),
        )
    )
    with pytest.raises(ConfigurationError, match="must be finite"):
        execute_plan(plan, {"bars": frame})


def test_range_columns_resolve_exactly(spark):
    frame = spark.createDataFrame(_ROWS, _SCHEMA)
    plan = ExecutionPlan(
        (
            RuleSpec(
                "cased",
                DATASET,
                RuleKind.VALUE_RANGE,
                Predicate(Comparison.GE, Decimal("0")),
                ColumnRef("Amount"),
                target=MetricKind.COLUMN_MIN,
            ),
        )
    )
    with pytest.raises(ConfigurationError, match="exact case-sensitive"):
        execute_plan(plan, {"bars": frame})


def test_non_numeric_bounds_fail_closed(spark):
    frame = spark.createDataFrame([("a",), ("b",)], "label string")
    plan = ExecutionPlan(
        (
            RuleSpec(
                "label_min",
                DATASET,
                RuleKind.VALUE_RANGE,
                Predicate(Comparison.GE, Decimal("0")),
                ColumnRef("label"),
                target=MetricKind.COLUMN_MIN,
            ),
        )
    )
    with pytest.raises(ConfigurationError, match="unsupported type"):
        execute_plan(plan, {"bars": frame})
