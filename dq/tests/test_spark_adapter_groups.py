# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Spark contracts for groups/v1: grouped distinct bounds over shared aggregates.

Expected values are hand-computed from the fixtures: per-group distinct counts
of id are east=3, west=2, north=1.
"""

from decimal import Decimal

import pytest

from pyspark.sql.types import StringType, StructField, StructType

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
    SEMANTIC_GROUPS_VERSION,
)
from dq.spark_adapter import GROUP_CAPABILITIES, execute_plan
from dq.tests.helpers import legacy_by_rule

pytestmark = pytest.mark.spark

DATASET = DatasetRef("grp", "e" * 64)

_ROWS = [
    ("east", "a"),
    ("east", "b"),
    ("east", "c"),
    ("west", "a"),
    ("west", "a"),
    ("west", "b"),
    ("north", "a"),
    ("north", "a"),
    ("north", "a"),
]
_SCHEMA = "region string, id string"
# Per-group distinct id counts: east=3, west=2, north=1; three groups total.


def bound_rules(group_by=(ColumnRef("region"),), column="id"):
    return (
        RuleSpec(
            "grp_min",
            DATASET,
            RuleKind.GROUPED_DISTINCT,
            Predicate(Comparison.GE, Decimal(2)),
            ColumnRef(column),
            group_by=group_by,
            target=MetricKind.GROUP_MIN_DISTINCT,
        ),
        RuleSpec(
            "grp_max",
            DATASET,
            RuleKind.GROUPED_DISTINCT,
            Predicate(Comparison.LE, Decimal(3)),
            ColumnRef(column),
            group_by=group_by,
            target=MetricKind.GROUP_MAX_DISTINCT,
        ),
    )


def test_capabilities_split_by_semantic_version():
    from dq.spark_adapter import CAPABILITIES

    assert CAPABILITIES.metrics == frozenset(
        {MetricKind.ROW_COUNT, MetricKind.PRESENT_COUNT}
    )
    assert CAPABILITIES.semantic_version == "counts/v1"
    assert GROUP_CAPABILITIES.metrics == frozenset(
        {
            MetricKind.GROUP_COUNT,
            MetricKind.GROUP_MIN_DISTINCT,
            MetricKind.GROUP_MAX_DISTINCT,
        }
    )
    assert GROUP_CAPABILITIES.semantic_version == SEMANTIC_GROUPS_VERSION


def test_executes_grouped_bounds_with_exact_counts(spark):
    frame = spark.createDataFrame(_ROWS, _SCHEMA)
    plan = ExecutionPlan(bound_rules())
    outcomes = execute_plan(plan, {"grp": frame})
    by_rule = legacy_by_rule(outcomes)
    assert by_rule["grp_min"]["success"] is False, "north holds 1 distinct id"
    assert by_rule["grp_min"]["details"]["observed"] == {
        "numerator": 1,
        "denominator": 1,
    }
    assert by_rule["grp_max"]["success"] is True, "no group exceeds 3 distinct ids"
    assert by_rule["grp_max"]["details"]["observed"] == {
        "numerator": 3,
        "denominator": 1,
    }
    assert by_rule["grp_min"]["details"]["semantic_version"] == SEMANTIC_GROUPS_VERSION


def test_grouped_empty_dataset_passes_vacuously(spark):
    empty = spark.createDataFrame([], "region string, id string")
    plan = ExecutionPlan(bound_rules())
    by_rule = legacy_by_rule(execute_plan(plan, {"grp": empty}))
    assert by_rule["grp_min"]["success"] is True
    assert by_rule["grp_min"]["details"]["reason"] == "no_groups"
    assert by_rule["grp_max"]["success"] is True


def test_grouped_multiple_groupings_each_get_correct_bounds(spark):
    frame = spark.createDataFrame(_ROWS + [("north", "b")], _SCHEMA)
    dataset = DatasetRef("grp", "e" * 64)
    plan = ExecutionPlan(
        (
            RuleSpec(
                "by_region_min",
                dataset,
                RuleKind.GROUPED_DISTINCT,
                Predicate(Comparison.GE, Decimal(2)),
                ColumnRef("id"),
                group_by=(ColumnRef("region"),),
                target=MetricKind.GROUP_MIN_DISTINCT,
            ),
            RuleSpec(
                "by_id_min",
                dataset,
                RuleKind.GROUPED_DISTINCT,
                Predicate(Comparison.GE, Decimal(2)),
                ColumnRef("region"),
                group_by=(ColumnRef("id"),),
                target=MetricKind.GROUP_MIN_DISTINCT,
            ),
        )
    )
    by_rule = legacy_by_rule(execute_plan(plan, {"grp": frame}))
    assert by_rule["by_region_min"]["success"] is True, "region groups: 3, 2, 2"
    assert by_rule["by_id_min"]["success"] is False, "id c spans only east"
    assert by_rule["by_id_min"]["details"]["observed"] == {
        "numerator": 1,
        "denominator": 1,
    }


def test_grouped_rejects_non_exact_group_columns_before_execution(spark):
    frame = spark.createDataFrame(_ROWS, _SCHEMA)
    plan = ExecutionPlan(bound_rules(group_by=(ColumnRef("Region"),)))
    with pytest.raises(ConfigurationError, match="exact case-sensitive"):
        execute_plan(plan, {"grp": frame})


def test_grouped_rejects_non_exact_distinct_columns_before_execution(spark):
    frame = spark.createDataFrame(_ROWS, _SCHEMA)
    plan = ExecutionPlan(bound_rules(column="ID"))
    with pytest.raises(ConfigurationError, match="exact case-sensitive"):
        execute_plan(plan, {"grp": frame})
