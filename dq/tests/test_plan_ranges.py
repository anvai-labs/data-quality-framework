# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Pure contracts for ranges/v1: value-range metrics and rules (ADR-005).

Expected decisions mirror the legacy isNonNegative / isInRange semantics:
one bounded comparison per rule over the column's exact stored bounds, with
a vacuous pass when the column holds no comparable values.
"""

from dataclasses import replace
from decimal import Decimal
from fractions import Fraction

import pytest

from dq.exceptions import ConfigurationError, ValidationError
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
    SEMANTIC_RANGES_VERSION,
)

DATASET = DatasetRef("bars", "b" * 64)
COLUMN = ColumnRef("amount")
CAPABILITIES = CapabilitySet(
    "ranges_adapter", "1", frozenset(MetricKind), SEMANTIC_RANGES_VERSION
)


def range_rule(
    identifier="amount_low",
    column="amount",
    operator=Comparison.GE,
    threshold="0",
    target=MetricKind.COLUMN_MIN,
):
    return RuleSpec(
        identifier,
        DATASET,
        RuleKind.VALUE_RANGE,
        Predicate(operator, Decimal(threshold)),
        ColumnRef(column),
        target=target,
    )


def range_values(rule, value_count, bound):
    count_key, target_key = rule.required_metrics
    return {count_key: value_count, target_key: bound}


def test_range_metrics_require_ranges_version_and_a_column():
    with pytest.raises(ConfigurationError, match="ranges/v1"):
        from dq.plan import MetricKey

        MetricKey(DATASET, MetricKind.COLUMN_MIN, COLUMN)
    with pytest.raises(ConfigurationError, match="cannot specify groups"):
        from dq.plan import MetricKey

        MetricKey(
            DATASET,
            MetricKind.COLUMN_MIN,
            COLUMN,
            SEMANTIC_RANGES_VERSION,
            (ColumnRef("exchange"),),
        )
    with pytest.raises(ConfigurationError, match="require a column"):
        from dq.plan import MetricKey

        MetricKey(DATASET, MetricKind.COLUMN_MAX, None, SEMANTIC_RANGES_VERSION)


def test_value_range_construction_contracts():
    with pytest.raises(ConfigurationError, match="value range requires"):
        RuleSpec(
            "no_column",
            DATASET,
            RuleKind.VALUE_RANGE,
            Predicate(Comparison.GE, Decimal(0)),
            None,
            target=MetricKind.COLUMN_MIN,
        )
    with pytest.raises(ConfigurationError, match="value range requires"):
        range_rule(target=MetricKind.ROW_COUNT)
    with pytest.raises(ConfigurationError, match="value range requires"):
        RuleSpec(
            "with_groups",
            DATASET,
            RuleKind.VALUE_RANGE,
            Predicate(Comparison.GE, Decimal(0)),
            ColumnRef("amount"),
            group_by=(ColumnRef("exchange"),),
            target=MetricKind.COLUMN_MIN,
        )


def test_range_plan_identity_is_canonical_and_version_bound():
    minimum = range_rule("amount.min", operator=Comparison.GE, threshold="0")
    maximum = range_rule(
        "amount.max",
        operator=Comparison.LE,
        threshold="100",
        target=MetricKind.COLUMN_MAX,
    )
    plan = ExecutionPlan((minimum, maximum))
    assert plan.semantic_version == SEMANTIC_RANGES_VERSION
    assert plan == ExecutionPlan((maximum, minimum))
    assert plan.fingerprint == ExecutionPlan((maximum, minimum)).fingerprint
    assert len(plan.metrics) == 3  # column count + min + max, deduplicated
    with pytest.raises(ConfigurationError, match="mix"):
        ExecutionPlan(
            (
                minimum,
                RuleSpec(
                    "sized",
                    DATASET,
                    RuleKind.SIZE,
                    Predicate(Comparison.GE, Decimal(1)),
                ),
            )
        )


def test_non_negative_semantics_compare_the_smallest_value():
    rule = range_rule("amount_min", operator=Comparison.GE, threshold="0")
    plan = ExecutionPlan((rule,))
    (outcome,) = plan.evaluate(range_values(rule, 4, Fraction(0)), CAPABILITIES)
    assert outcome.success is True, "0 is non-negative"
    (outcome,) = plan.evaluate(range_values(rule, 4, Fraction(-1, 4)), CAPABILITIES)
    assert outcome.success is False, "-0.25 violates the non-negative bound"


def test_upper_bound_compares_the_largest_value():
    rule = range_rule(
        "amount_max",
        operator=Comparison.LE,
        threshold="100",
        target=MetricKind.COLUMN_MAX,
    )
    plan = ExecutionPlan((rule,))
    (outcome,) = plan.evaluate(range_values(rule, 4, Fraction(100)), CAPABILITIES)
    assert outcome.success is True
    (outcome,) = plan.evaluate(range_values(rule, 4, Fraction(150)), CAPABILITIES)
    assert outcome.success is False


def test_bounds_compare_as_exact_fractions():
    rule = range_rule(
        "third", operator=Comparison.GE, threshold="0.3333333333333333333333"
    )
    plan = ExecutionPlan((rule,))
    (outcome,) = plan.evaluate(range_values(rule, 3, Fraction(1, 3)), CAPABILITIES)
    assert outcome.success is True, "1/3 exceeds the truncated decimal bound"


def test_empty_column_passes_vacuously():
    rule = range_rule("amount_min", operator=Comparison.GE, threshold="0")
    plan = ExecutionPlan((rule,))
    count_key, target_key = rule.required_metrics
    (outcome,) = plan.evaluate({count_key: 0, target_key: Fraction(0)}, CAPABILITIES)
    assert outcome.success is True
    assert outcome.to_legacy()["details"]["reason"] == "no_values"


def test_bounds_must_be_exact_fractions():
    rule = range_rule("amount_min", operator=Comparison.GE, threshold="0")
    plan = ExecutionPlan((rule,))
    count_key, target_key = rule.required_metrics
    with pytest.raises(ValidationError, match="exact Fractions"):
        plan.evaluate({count_key: 3, target_key: 0.5}, CAPABILITIES)


def test_fingerprints_distinguish_bound_targets():
    low = range_rule("amount.low", operator=Comparison.GE, threshold="0")
    high = range_rule(
        "amount.high",
        operator=Comparison.LE,
        threshold="100",
        target=MetricKind.COLUMN_MAX,
    )
    assert ExecutionPlan((low,)).fingerprint != ExecutionPlan((high,)).fingerprint
    document = low.to_dict()
    assert document["target"] == "column_min"
    assert ExecutionPlan((high,)).fingerprint != ExecutionPlan((low,)).fingerprint


def test_capability_negotiation_is_per_semantic_version():
    rule = range_rule("amount.min", operator=Comparison.GE, threshold="0")
    plan = ExecutionPlan((rule,))
    counts_capabilities = CapabilitySet(
        "spark",
        "1",
        frozenset({MetricKind.ROW_COUNT, MetricKind.PRESENT_COUNT}),
    )
    with pytest.raises(ConfigurationError, match="capabilities"):
        plan.validate_for(counts_capabilities)
    with pytest.raises(ValidationError, match="exactly the required metrics"):
        plan.evaluate({rule.required_metrics[0]: 0}, CAPABILITIES)
