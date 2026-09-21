# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Pure contracts for groups/v1: grouped distinct-count bounds (ADR-004).

The expected decisions mirror the legacy DistinctnessByGroup semantics: one
bounded comparison per rule, vacuous success on empty input, and no per-group
rows anywhere in the contract.
"""

from dataclasses import replace
from decimal import Decimal

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
    SEMANTICS_VERSION,
    SEMANTIC_GROUPS_VERSION,
)

DATASET = DatasetRef("bars", "b" * 64)
COLUMN = ColumnRef("ticker")
GROUP_BY = (ColumnRef("exchange"), ColumnRef("book"))
CAPABILITIES = CapabilitySet(
    "groups_adapter", "1", frozenset(MetricKind), SEMANTIC_GROUPS_VERSION
)


def grouped_rule(
    identifier="id_min",
    column=COLUMN,
    group_by=GROUP_BY,
    operator=Comparison.GE,
    threshold="3",
    target=MetricKind.GROUP_MIN_DISTINCT,
):
    return RuleSpec(
        identifier,
        DATASET,
        RuleKind.GROUPED_DISTINCT,
        Predicate(operator, Decimal(threshold)),
        column,
        group_by=group_by,
        target=target,
    )


def group_values(rule, group_count, minimum, maximum):
    """Values for one rule's two required metrics: count plus its target bound."""
    count_key, target_key = rule.required_metrics
    target_value = (
        maximum if target_key.kind is MetricKind.GROUP_MAX_DISTINCT else minimum
    )
    return {count_key: group_count, target_key: target_value}


def test_group_metrics_require_groups_version_and_grouping():
    with pytest.raises(ConfigurationError, match="groups/v1"):
        from dq.plan import MetricKey

        MetricKey(DATASET, MetricKind.GROUP_COUNT, group_by=GROUP_BY)
    with pytest.raises(ConfigurationError, match="grouped columns"):
        from dq.plan import MetricKey

        MetricKey(
            DATASET,
            MetricKind.GROUP_MIN_DISTINCT,
            COLUMN,
            SEMANTIC_GROUPS_VERSION,
            (),
        )
    with pytest.raises(ConfigurationError, match="duplicate"):
        from dq.plan import MetricKey

        MetricKey(
            DATASET,
            MetricKind.GROUP_MIN_DISTINCT,
            COLUMN,
            SEMANTIC_GROUPS_VERSION,
            (ColumnRef("a"), ColumnRef("a")),
        )
    with pytest.raises(ConfigurationError, match="requires a column"):
        from dq.plan import MetricKey

        MetricKey(
            DATASET,
            MetricKind.GROUP_MAX_DISTINCT,
            None,
            SEMANTIC_GROUPS_VERSION,
            GROUP_BY,
        )
    with pytest.raises(ConfigurationError, match="group count"):
        from dq.plan import MetricKey

        MetricKey(
            DATASET,
            MetricKind.GROUP_COUNT,
            COLUMN,
            SEMANTIC_GROUPS_VERSION,
            GROUP_BY,
        )


def test_counts_v1_metrics_reject_groups():
    from dq.plan import MetricKey

    with pytest.raises(ConfigurationError, match="counts/v1 metrics cannot specify"):
        MetricKey(
            DATASET,
            MetricKind.PRESENT_COUNT,
            COLUMN,
            SEMANTICS_VERSION,
            GROUP_BY,
        )
    with pytest.raises(ConfigurationError, match="semantic version"):
        MetricKey(
            DATASET,
            MetricKind.PRESENT_COUNT,
            COLUMN,
            None,
        )
    with pytest.raises(ConfigurationError, match="size requires"):
        RuleSpec(
            "sized",
            DATASET,
            RuleKind.SIZE,
            Predicate(Comparison.GE, Decimal(1)),
            group_by=GROUP_BY,
        )
    with pytest.raises(ConfigurationError, match="cannot target grouped"):
        RuleSpec(
            "complete",
            DATASET,
            RuleKind.COMPLETENESS,
            Predicate(Comparison.GE, Decimal(1)),
            COLUMN,
            target=MetricKind.GROUP_MIN_DISTINCT,
        )


def test_grouped_rule_construction_contracts():
    with pytest.raises(ConfigurationError, match="grouped distinct"):
        grouped_rule(column=None)
    with pytest.raises(ConfigurationError, match="grouped distinct"):
        grouped_rule(group_by=())
    with pytest.raises(ConfigurationError, match="grouped distinct"):
        grouped_rule(target=MetricKind.ROW_COUNT)
    with pytest.raises(ConfigurationError, match="grouped distinct"):
        grouped_rule(threshold="2.5")


def test_grouped_plan_identity_is_canonical_and_version_bound():
    minimum = grouped_rule("g.min")
    maximum = grouped_rule(
        "g.max",
        operator=Comparison.LE,
        threshold="9",
        target=MetricKind.GROUP_MAX_DISTINCT,
    )
    plan = ExecutionPlan((minimum, maximum))
    assert plan.semantic_version == SEMANTIC_GROUPS_VERSION
    assert plan == ExecutionPlan((maximum, minimum))
    assert plan.fingerprint == ExecutionPlan((maximum, minimum)).fingerprint
    assert len(plan.metrics) == 3  # group count + min + max, deduplicated
    with pytest.raises(ConfigurationError, match="mix"):
        ExecutionPlan(
            (
                grouped_rule("g.min"),
                RuleSpec(
                    "sized",
                    DATASET,
                    RuleKind.SIZE,
                    Predicate(Comparison.GE, Decimal(1)),
                ),
            )
        )


def test_grouped_evaluation_matches_legacy_decisions():
    rule = grouped_rule("g.min", operator=Comparison.GE, threshold="3")
    plan = ExecutionPlan((rule,))
    values = group_values(rule, 4, 3, 9)
    (outcome,) = plan.evaluate(values, CAPABILITIES)
    assert outcome.success is True, "every group holds at least 3 distinct values"
    legacy = outcome.to_legacy()
    assert legacy["details"]["observed"] == {"numerator": 3, "denominator": 1}
    assert legacy["details"]["reason"] == "evaluated"

    failing = group_values(rule, 4, 2, 9)
    (outcome,) = plan.evaluate(failing, CAPABILITIES)
    assert outcome.success is False, "one group holds only 2 distinct values"


def test_maximum_bound_compares_the_largest_group():
    rule = grouped_rule(
        "g.max",
        operator=Comparison.LE,
        threshold="9",
        target=MetricKind.GROUP_MAX_DISTINCT,
    )
    plan = ExecutionPlan((rule,))
    (outcome,) = plan.evaluate(group_values(rule, 4, 2, 9), CAPABILITIES)
    assert outcome.success is True
    (outcome,) = plan.evaluate(group_values(rule, 4, 2, 10), CAPABILITIES)
    assert outcome.success is False


def test_empty_input_passes_vacuously_like_the_legacy_constraint():
    rule = grouped_rule("g.min", threshold="3")
    plan = ExecutionPlan((rule,))
    count_key, target_key = rule.required_metrics
    values = {count_key: 0, target_key: 0}
    (outcome,) = plan.evaluate(values, CAPABILITIES)
    assert outcome.success is True, "legacy 'No suitable data' passes"
    assert outcome.to_legacy()["details"]["reason"] == "no_groups"


def test_grouped_threshold_must_be_a_cardinality():
    with pytest.raises(ConfigurationError, match="grouped distinct"):
        grouped_rule(threshold="-1")
    okay = grouped_rule(threshold="0")
    plan = ExecutionPlan((okay,))
    count_key, target_key = okay.required_metrics
    (outcome,) = plan.evaluate({count_key: 2, target_key: 0}, CAPABILITIES)
    assert outcome.success is True


def test_grouped_rules_are_snapshot_and_grouping_sensitive():
    minimum = grouped_rule("g.min")
    other_grouping = replace(minimum, group_by=(ColumnRef("exchange"),))
    assert minimum != other_grouping
    plan = ExecutionPlan((minimum,))
    tightened = replace(minimum, dataset=DatasetRef("bars", "c" * 64))
    assert (
        ExecutionPlan((minimum,)).fingerprint != ExecutionPlan((tightened,)).fingerprint
    )
    assert other_grouping.required_metrics[1].group_by == (ColumnRef("exchange"),)


def test_capability_negotiation_is_per_semantic_version():
    rule = grouped_rule("g.min")
    plan = ExecutionPlan((rule,))
    counts_capabilities = CapabilitySet(
        "spark", "1", frozenset({MetricKind.ROW_COUNT, MetricKind.PRESENT_COUNT})
    )
    with pytest.raises(ConfigurationError, match="capabilities"):
        plan.validate_for(counts_capabilities)
    with pytest.raises(ValidationError, match="exactly the required metrics"):
        plan.evaluate({rule.required_metrics[0]: 0}, CAPABILITIES)
    (outcome,) = plan.evaluate(group_values(rule, 2, 3, 9), CAPABILITIES)
    assert outcome.success is True
