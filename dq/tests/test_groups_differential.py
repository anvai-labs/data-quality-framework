# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Differential gate for groups/v1: adapter versus the legacy constraint.

ADR-004 requires that groups/v1 rules and the legacy DistinctnessByGroup
constraint decide identically over shared fixtures, including empty input
and one-sided thresholds. Both engines run the same logical bounds and
their per-column decisions must agree; the end-to-end case routes the
translation through dq.portable_config.
"""

from decimal import Decimal
from json import dumps

import pytest
from pyhocon import ConfigFactory

from dq.engine.custom.custom_engine import CustomEngine
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
from dq.portable_config import translate_plan
from dq.spark_adapter import execute_plan

pytestmark = pytest.mark.spark

DATASET = DatasetRef("bars", "b" * 64)
_ROWS = [
    ("east", "a", 1.0),
    ("east", "b", 2.0),
    ("east", "c", 3.0),
    ("west", "a", 9.0),
    ("west", "a", 9.0),
    ("west", "b", 5.0),
    ("north", "a", 7.0),
    ("north", "a", 7.0),
    ("north", "a", 8.0),
]
_SCHEMA = "region string, id string, val double"
# Per-group distinct counts: east id=3 val=3; west id=2 val=2; north id=1 val=2.


def legacy_config(columns, group_by, minimum, maximum):
    settings = f"columns = {dumps(columns)}\n        group_by = {dumps(group_by)}"
    if minimum is not None:
        settings += f"\n        min = {minimum}"
    if maximum is not None:
        settings += f"\n        max = {maximum}"
    return ConfigFactory.parse_string(f"""
        sync {{ checks = [ {{
            constraint = "DistinctnessByGroup"
            {settings}
            level = "Error"
        }} ] }}
        """).get("sync", {})


def legacy_column_decisions(config, frame, columns):
    """Per-column success from the legacy engine's summaries."""
    decisions = {column: True for column in columns}
    for result in CustomEngine(config).apply(frame):
        column = result["details"]["instance"].rsplit(" for ", 1)[-1]
        if column in decisions:
            decisions[column] = decisions[column] and result["success"]
    return decisions


def portable_column_decisions(columns, group_by, minimum, maximum, frame):
    """Per-column success from groups/v1 rules on the native Spark adapter."""
    rules = []
    for column in columns:
        column_ref = ColumnRef(column)
        grouping = tuple(ColumnRef(name) for name in group_by)
        if minimum is not None:
            rules.append(
                RuleSpec(
                    f"{column}.min",
                    DATASET,
                    RuleKind.GROUPED_DISTINCT,
                    Predicate(Comparison.GE, Decimal(minimum)),
                    column_ref,
                    group_by=grouping,
                    target=MetricKind.GROUP_MIN_DISTINCT,
                )
            )
        if maximum is not None:
            rules.append(
                RuleSpec(
                    f"{column}.max",
                    DATASET,
                    RuleKind.GROUPED_DISTINCT,
                    Predicate(Comparison.LE, Decimal(maximum)),
                    column_ref,
                    group_by=grouping,
                    target=MetricKind.GROUP_MAX_DISTINCT,
                )
            )
    outcomes = execute_plan(ExecutionPlan(tuple(rules)), {"bars": frame})
    decisions = {column: True for column in columns}
    for outcome in outcomes:
        details = outcome.to_legacy()["details"]
        decisions[details["column"]] = (
            decisions[details["column"]] and outcome.to_legacy()["success"]
        )
    return decisions


@pytest.mark.parametrize(
    ("minimum", "maximum"),
    [(2, 3), (2, None), (None, 3), (1, 5), (3, 3)],
)
def test_bounds_decide_identically(spark, minimum, maximum):
    frame = spark.createDataFrame(_ROWS, _SCHEMA)
    columns = ["id", "val"]
    legacy = legacy_column_decisions(
        legacy_config(columns, ["region"], minimum, maximum), frame, columns
    )
    portable = portable_column_decisions(columns, ["region"], minimum, maximum, frame)
    assert legacy == portable


def test_empty_input_decides_identically(spark):
    empty = spark.createDataFrame([], "region string, id string")
    legacy = legacy_column_decisions(
        legacy_config(["id"], ["region"], 2, None), empty, ["id"]
    )
    portable = portable_column_decisions(["id"], ["region"], 2, None, empty)
    assert legacy == portable == {"id": True}, "both engines pass vacuously"


def test_many_groups_decide_identically(spark):
    rows = []
    for group in range(120):
        rows.append((f"g{group}", "a", 1.0))
        rows.append((f"g{group}", "b", 2.0))
    frame = spark.createDataFrame(rows, _SCHEMA)
    columns = ["id", "val"]
    legacy = legacy_column_decisions(
        legacy_config(columns, ["region"], 1, 5), frame, columns
    )
    portable = portable_column_decisions(columns, ["region"], 1, 5, frame)
    assert legacy == portable == {"id": True, "val": True}


def test_translated_plan_matches_the_legacy_engine_end_to_end(spark):
    frame = spark.createDataFrame(_ROWS, _SCHEMA)
    checks = [
        {
            "alias": "bounds",
            "constraint": "DistinctnessByGroup",
            "columns": ["id", "val"],
            "group_by": ["region"],
            "min": 2,
            "max": 3,
        }
    ]
    plan = translate_plan(checks, DATASET)
    assert plan.semantic_version == "groups/v1"
    outcomes = execute_plan(plan, {"bars": frame})
    portable = {}
    for outcome in outcomes:
        legacy = outcome.to_legacy()
        column = legacy["details"]["column"]
        portable[column] = portable.get(column, True) and legacy["success"]
    legacy = legacy_column_decisions(
        legacy_config(["id", "val"], ["region"], 2, 3), frame, ["id", "val"]
    )
    assert portable == legacy == {"id": False, "val": True}
