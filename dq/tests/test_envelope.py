# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Pure contracts for the RulesetEnvelope (ADR-004/005 composition)."""

from decimal import Decimal
from fractions import Fraction

import pytest

from dq.envelope import RulesetEnvelope
from dq.exceptions import ConfigurationError
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
)

DATASET = DatasetRef("bars", "b" * 64)


def counts_plan():
    return ExecutionPlan(
        (
            RuleSpec(
                "complete",
                DATASET,
                RuleKind.COMPLETENESS,
                Predicate(Comparison.GE, Decimal("0.5")),
                ColumnRef("id"),
            ),
        )
    )


def groups_plan():
    return ExecutionPlan(
        (
            RuleSpec(
                "grp_min",
                DATASET,
                RuleKind.GROUPED_DISTINCT,
                Predicate(Comparison.GE, Decimal(1)),
                ColumnRef("id"),
                group_by=(ColumnRef("region"),),
                target=MetricKind.GROUP_MIN_DISTINCT,
            ),
        )
    )


def ranges_plan():
    return ExecutionPlan(
        (
            RuleSpec(
                "rng_min",
                DATASET,
                RuleKind.VALUE_RANGE,
                Predicate(Comparison.GE, Decimal(0)),
                ColumnRef("amount"),
                target=MetricKind.COLUMN_MIN,
            ),
        )
    )


def test_envelope_composes_mixed_semantics():
    envelope = RulesetEnvelope((counts_plan(), groups_plan(), ranges_plan()))
    assert len(envelope.plans) == 3
    assert len(envelope.fingerprint) == 64


def test_envelope_identity_is_order_insensitive():
    e1 = RulesetEnvelope((counts_plan(), groups_plan()))
    e2 = RulesetEnvelope((groups_plan(), counts_plan()))
    assert e1.fingerprint == e2.fingerprint


def test_envelope_rejects_empty():
    with pytest.raises(ConfigurationError):
        RulesetEnvelope(())


def test_envelope_rejects_mixed_snapshots():
    p1 = ExecutionPlan(
        (
            RuleSpec(
                "a",
                DatasetRef("x", "a" * 64),
                RuleKind.SIZE,
                Predicate(Comparison.GE, Decimal(1)),
            ),
        )
    )
    p2 = ExecutionPlan(
        (
            RuleSpec(
                "b",
                DatasetRef("y", "b" * 64),
                RuleKind.SIZE,
                Predicate(Comparison.GE, Decimal(1)),
            ),
        )
    )
    with pytest.raises(ConfigurationError, match="same.*snapshot"):
        RulesetEnvelope((p1, p2))


def test_envelope_rejects_non_execution_plan():
    with pytest.raises(ConfigurationError):
        RulesetEnvelope(("not a plan",))


def test_envelope_evaluates_mixed_rulesets():
    counts = counts_plan()
    groups = groups_plan()
    ranges = ranges_plan()
    envelope = RulesetEnvelope((counts, groups, ranges))
    caps_by_version = {
        "counts/v1": CapabilitySet(
            "a",
            "1",
            frozenset({MetricKind.ROW_COUNT, MetricKind.PRESENT_COUNT}),
        ),
        "groups/v1": CapabilitySet(
            "b",
            "1",
            frozenset(
                {
                    MetricKind.GROUP_COUNT,
                    MetricKind.GROUP_MIN_DISTINCT,
                    MetricKind.GROUP_MAX_DISTINCT,
                }
            ),
            SEMANTIC_RANGES_VERSION := "groups/v1",
        ),
        "ranges/v1": CapabilitySet(
            "c",
            "1",
            frozenset({MetricKind.COLUMN_COUNT, MetricKind.COLUMN_MIN, MetricKind.COLUMN_MAX}),
            "ranges/v1",
        ),
    }
    # Build values for each plan's metrics
    all_values = {}
    for plan in envelope.plans:
        for metric in plan.metrics:
            if metric.semantic_version == "groups/v1":
                if metric.kind is MetricKind.GROUP_COUNT:
                    all_values[metric] = 3
                elif metric.kind is MetricKind.GROUP_MIN_DISTINCT:
                    all_values[metric] = 2
                else:
                    all_values[metric] = 5
            elif metric.kind is MetricKind.ROW_COUNT:
                all_values[metric] = 10
            elif metric.kind is MetricKind.PRESENT_COUNT:
                all_values[metric] = 8
            elif metric.kind is MetricKind.COLUMN_COUNT:
                all_values[metric] = 5
            elif metric.kind is MetricKind.COLUMN_MIN:
                all_values[metric] = Fraction(0)
            elif metric.kind is MetricKind.COLUMN_MAX:
                all_values[metric] = Fraction(100)
    outcomes = envelope.evaluate(all_values, caps_by_version)
    assert outcomes
