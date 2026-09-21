# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Spark contracts for counts/v1: nulls, NaN, decimals, empty inputs, partitions.

Expected counts are hand-computed fixtures, deliberately independent of any
PyDeequ result, because counts/v1 excludes floating-point NaN while legacy
Deequ completeness treats NaN as present.
"""

from decimal import Decimal

import pytest

from pyspark.sql.types import (
    DecimalType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

import dq.spark_adapter
from dq.exceptions import ConfigurationError
from dq.outcomes import CheckOutcome
from dq.plan import (
    CapabilitySet,
    ColumnRef,
    Comparison,
    DatasetRef,
    ExecutionPlan,
    MetricKind,
    Predicate,
    RuleKind,
    RuleSpec,
    SEMANTICS_VERSION,
)
from dq.spark_adapter import ADAPTER_NAME, ADAPTER_VERSION, CAPABILITIES, execute_plan
from dq.tests.helpers import legacy_by_rule

pytestmark = pytest.mark.spark

DATASET = DatasetRef("bars", "b" * 64)
OTHER = DatasetRef("qux", "c" * 64)
ROW_COUNT = 5
PRESENT_COUNTS = {"id": 4, "note": 5, "score": 3, "tag": 3, "amount": 4}

_ROWS = [
    ("a", "x", 1.5, None, Decimal("1.25")),
    ("", "NaN", float("nan"), "NaN", None),
    ("b", "", float("inf"), None, Decimal("3.50")),
    (None, "y", float("-inf"), "x", Decimal("0.00")),
    ("c", "y", None, "NaN", Decimal("2.00")),
]

_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("note", StringType(), True),
        StructField("score", DoubleType(), True),
        StructField("tag", StringType(), True),
        StructField("amount", DecimalType(10, 2), True),
    ]
)


def size_rule(identifier, operator, threshold, dataset=DATASET):
    return RuleSpec(
        identifier, dataset, RuleKind.SIZE, Predicate(operator, Decimal(threshold))
    )


def complete_rule(identifier, column, operator=Comparison.GE, threshold="1"):
    return RuleSpec(
        identifier,
        DATASET,
        RuleKind.COMPLETENESS,
        Predicate(operator, Decimal(threshold)),
        ColumnRef(column),
    )


def fixture_plan():
    """Seven rules whose expected outcomes are computed from _ROWS by hand."""
    return ExecutionPlan(
        (
            size_rule("min_rows", Comparison.GE, "1"),
            size_rule("not_five_rows", Comparison.LT, "5"),
            complete_rule("id_ge_three_quarters", "id", Comparison.GE, "0.75"),
            complete_rule("score_nan_excluded", "score", Comparison.GE, "0.8"),
            complete_rule("note_string_nan_present", "note", Comparison.EQ, "1"),
            complete_rule("tag_under_seventy", "tag", Comparison.LT, "0.7"),
            complete_rule("amount_decimal_complete", "amount", Comparison.GE, "0.8"),
        )
    )


def expected_fixture_outcomes():
    """rule_id -> (success, numerator, denominator) for fixture_plan rules."""
    return {
        "min_rows": (True, ROW_COUNT, 1),
        "not_five_rows": (False, ROW_COUNT, 1),
        "id_ge_three_quarters": (True, PRESENT_COUNTS["id"], ROW_COUNT),
        "score_nan_excluded": (False, PRESENT_COUNTS["score"], ROW_COUNT),
        "note_string_nan_present": (True, PRESENT_COUNTS["note"], ROW_COUNT),
        "tag_under_seventy": (True, PRESENT_COUNTS["tag"], ROW_COUNT),
        "amount_decimal_complete": (True, PRESENT_COUNTS["amount"], ROW_COUNT),
    }


def assert_fixture_counts(outcomes):
    by_rule = legacy_by_rule(outcomes)
    assert set(by_rule) == set(expected_fixture_outcomes())
    for rule_id, (
        success,
        numerator,
        denominator,
    ) in expected_fixture_outcomes().items():
        result = by_rule[rule_id]
        assert result["success"] is success, rule_id
        assert result["details"]["observed"] == {
            "numerator": numerator,
            "denominator": denominator,
        }, rule_id
    score = by_rule["score_nan_excluded"]["details"]["observed"]["numerator"]
    assert score == 3, "counts/v1 excludes NaN but keeps infinities"
    note = by_rule["note_string_nan_present"]["details"]["observed"]["numerator"]
    assert note == ROW_COUNT, 'empty strings and string "NaN" stay present'


@pytest.fixture
def mixed_dataframe(spark):
    return spark.createDataFrame(_ROWS, _SCHEMA)


@pytest.fixture
def empty_dataframe(spark):
    return spark.createDataFrame([], _SCHEMA)


def _job_ids(spark):
    tracker = spark.sparkContext.statusTracker
    tracker = tracker() if callable(tracker) else tracker
    return set(tracker.getJobIdsForGroup(None) or [])


def test_executes_size_and_completeness_with_independent_fixture_counts(
    mixed_dataframe,
):
    outcomes = execute_plan(fixture_plan(), {"bars": mixed_dataframe})
    assert isinstance(outcomes, tuple)
    assert len(outcomes) == 7
    assert all(isinstance(outcome, CheckOutcome) for outcome in outcomes)
    assert_fixture_counts(outcomes)
    by_rule = legacy_by_rule(outcomes)
    details = by_rule["min_rows"]["details"]
    assert details["adapter"] == {"name": ADAPTER_NAME, "version": ADAPTER_VERSION}
    assert details["semantic_version"] == SEMANTICS_VERSION
    assert details["reason"] == "evaluated"


@pytest.mark.parametrize("partitions", [1, 3])
def test_repartitioning_preserves_exact_counts(spark, partitions):
    dataframe = spark.createDataFrame(_ROWS, _SCHEMA).repartition(partitions)
    outcomes = execute_plan(fixture_plan(), {"bars": dataframe})
    assert_fixture_counts(outcomes)


def test_empty_dataset_fails_completeness_and_applies_size_thresholds(
    empty_dataframe,
):
    plan = ExecutionPlan(
        (
            size_rule("empty_allows_zero", Comparison.GE, "0"),
            size_rule("empty_needs_one", Comparison.GE, "1"),
            complete_rule("empty_complete", "id", Comparison.GE, "0"),
        )
    )
    by_rule = legacy_by_rule(execute_plan(plan, {"bars": empty_dataframe}))
    assert by_rule["empty_allows_zero"]["success"] is True
    assert by_rule["empty_needs_one"]["success"] is False
    empty = by_rule["empty_complete"]
    assert empty["success"] is False
    assert empty["details"]["reason"] == "empty_dataset"
    assert empty["details"]["observed"] == {"numerator": 0, "denominator": 0}


def test_job_count_is_independent_of_metric_multiplicity(spark, mixed_dataframe):
    spread = mixed_dataframe.repartition(3)
    spread.count()
    single = ExecutionPlan((size_rule("only_size", Comparison.GE, "0"),))
    before = _job_ids(spark)
    execute_plan(single, {"bars": spread})
    after_single = _job_ids(spark)
    outcomes = execute_plan(fixture_plan(), {"bars": spread})
    after_full = _job_ids(spark)
    assert_fixture_counts(outcomes)
    assert len(after_single - before) == len(after_full - after_single) >= 1
    assert len(fixture_plan().metrics) == 6, "row count plus five distinct columns"


def test_bindings_must_cover_exactly_the_plan_datasets(mixed_dataframe):
    plan = ExecutionPlan((size_rule("sized", Comparison.GE, "0"),))
    with pytest.raises(ConfigurationError, match="missing"):
        execute_plan(plan, {})
    with pytest.raises(ConfigurationError, match="unknown"):
        execute_plan(plan, {"bars": mixed_dataframe, "other": mixed_dataframe})
    with pytest.raises(ConfigurationError, match="mapping"):
        execute_plan(plan, [("bars", mixed_dataframe)])


def test_rejects_non_dataframe_bindings_before_execution(spark):
    plan = ExecutionPlan((complete_rule("bound", "id"),))
    before = _job_ids(spark)
    with pytest.raises(ConfigurationError, match="Spark DataFrame"):
        execute_plan(plan, {"bars": object()})
    assert _job_ids(spark) == before


@pytest.mark.parametrize("column", ["Id", "ID"])
def test_rejects_columns_without_exact_case_sensitive_match(spark, column):
    plan = ExecutionPlan((complete_rule("cased", column, Comparison.GE, "0"),))
    before = _job_ids(spark)
    with pytest.raises(ConfigurationError, match="exact case-sensitive"):
        execute_plan(plan, {"bars": spark.createDataFrame(_ROWS, _SCHEMA)})
    assert _job_ids(spark) == before


def test_missing_column_error_lists_available_columns(spark):
    plan = ExecutionPlan((complete_rule("lost", "email"),))
    with pytest.raises(ConfigurationError) as raised:
        execute_plan(plan, {"bars": spark.createDataFrame(_ROWS, _SCHEMA)})
    assert "score" in str(raised.value)


@pytest.mark.parametrize("column", ["value", "VALUE"])
def test_rejects_case_insensitive_ambiguous_columns(spark, column):
    ambiguous = spark.createDataFrame([(1, 2)], ["value", "VALUE"])
    plan = ExecutionPlan((complete_rule("ambiguous", column, Comparison.GE, "0"),))
    with pytest.raises(ConfigurationError, match="ambiguous"):
        execute_plan(plan, {"bars": ambiguous})


def test_execute_plan_rejects_unsupported_capabilities(spark, mixed_dataframe):
    restricted = CapabilitySet(
        ADAPTER_NAME, ADAPTER_VERSION, frozenset({MetricKind.ROW_COUNT})
    )
    plan = ExecutionPlan((complete_rule("complete", "id"),))
    before = _job_ids(spark)
    original = dq.spark_adapter.CAPABILITIES
    dq.spark_adapter.CAPABILITIES = restricted
    try:
        with pytest.raises(ConfigurationError, match="capabilities"):
            execute_plan(plan, {"bars": mixed_dataframe})
    finally:
        dq.spark_adapter.CAPABILITIES = original
    assert _job_ids(spark) == before


def test_capabilities_declare_the_full_counts_v1_metric_subset():
    assert CAPABILITIES.adapter == ADAPTER_NAME == "spark"
    assert CAPABILITIES.version == ADAPTER_VERSION
    assert CAPABILITIES.semantic_version == SEMANTICS_VERSION
    assert CAPABILITIES.metrics == frozenset(
        {MetricKind.ROW_COUNT, MetricKind.PRESENT_COUNT}
    )


def test_outcomes_are_bounded_immutable_snapshots(mixed_dataframe):
    outcomes = execute_plan(
        ExecutionPlan((complete_rule("single", "id"),)), {"bars": mixed_dataframe}
    )
    outcome = outcomes[0]
    assert isinstance(outcome.success, bool)
    assert outcome.serialized_size < 1_048_576
    legacy = outcome.to_legacy()
    assert CheckOutcome.from_legacy(legacy).to_legacy() == legacy


def test_multiple_datasets_execute_against_their_own_bindings(spark):
    qux = spark.createDataFrame([(7, "kept"), (8, None)], ["n", "label"])
    plan = ExecutionPlan(
        (
            size_rule("bars_sized", Comparison.GE, "1"),
            complete_rule("bars_ids", "id"),
            size_rule("qux_sized", Comparison.GE, "1", OTHER),
            RuleSpec(
                "qux_labels",
                OTHER,
                RuleKind.COMPLETENESS,
                Predicate(Comparison.GE, Decimal("0.5")),
                ColumnRef("label"),
            ),
        )
    )
    bindings = {"bars": spark.createDataFrame(_ROWS, _SCHEMA), "qux": qux}
    by_rule = legacy_by_rule(execute_plan(plan, bindings))
    assert by_rule["bars_sized"]["details"]["observed"] == {
        "numerator": ROW_COUNT,
        "denominator": 1,
    }
    assert by_rule["bars_ids"]["details"]["observed"] == {
        "numerator": PRESENT_COUNTS["id"],
        "denominator": ROW_COUNT,
    }
    assert by_rule["qux_sized"]["details"]["observed"] == {
        "numerator": 2,
        "denominator": 1,
    }
    assert by_rule["qux_labels"]["details"]["observed"] == {
        "numerator": 1,
        "denominator": 2,
    }
    assert by_rule["qux_labels"]["success"] is True
