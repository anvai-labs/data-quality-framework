# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Pure contracts for translating the bounded legacy HOCON check subset."""

from decimal import Decimal
from pathlib import Path
import os
import subprocess
import sys

import pytest

from dq.exceptions import ConfigurationError
from dq.plan import (
    CapabilitySet,
    ColumnRef,
    Comparison,
    DatasetRef,
    MetricKind,
    RuleKind,
    Severity,
)

from dq.portable_config import SUPPORTED_CONSTRAINTS, translate_checks, translate_plan

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
DATASET = DatasetRef("bars", "b" * 64)


def check(**overrides):
    base = {
        "alias": "id_complete",
        "constraint": "isComplete",
        "column": "id",
        "level": "Error",
    }
    base.update(overrides)
    return base


def test_supported_subset_is_exactly_the_representable_rules():
    assert SUPPORTED_CONSTRAINTS == frozenset(
        {
            "isComplete",
            "hasCompleteness",
            "hasSize",
            "DistinctnessByGroup",
            "isNonNegative",
            "isInRange",
        }
    )


def test_translates_is_complete_to_exact_equality():
    (rule,) = translate_checks([check()], DATASET)
    assert rule.rule_id == "id_complete"
    assert rule.kind is RuleKind.COMPLETENESS
    assert rule.column.name == "id"
    assert rule.predicate.operator is Comparison.EQ
    assert rule.predicate.threshold == Decimal(1)
    assert rule.severity is Severity.ERROR


def test_translates_warning_level_and_default_severity():
    (warned,) = translate_checks([check(level="Warning")], DATASET)
    assert warned.severity is Severity.WARNING
    (defaulted,) = translate_checks(
        [check(level=None, alias="other")], DATASET, default_severity=Severity.WARNING
    )
    assert defaulted.severity is Severity.WARNING
    (implicit,) = translate_checks([check(level=None)], DATASET)
    assert implicit.severity is Severity.ERROR


def test_translates_has_completeness_with_single_comparison():
    (rule,) = translate_checks(
        [
            check(
                alias="email_mostly_complete",
                constraint="hasCompleteness",
                assertion="lambda x: x >= 0.9",
            )
        ],
        DATASET,
    )
    assert rule.kind is RuleKind.COMPLETENESS
    assert rule.predicate.operator is Comparison.GE
    assert rule.predicate.threshold == Decimal("0.9")


def test_translates_has_size_with_integer_threshold():
    (rule,) = translate_checks(
        [
            {
                "alias": "at_least_five",
                "constraint": "hasSize",
                "assertion": "lambda x: x >= 5",
            }
        ],
        DATASET,
    )
    assert rule.kind is RuleKind.SIZE
    assert rule.column is None
    assert rule.predicate.operator is Comparison.GE
    assert rule.predicate.threshold == Decimal(5)


def test_rule_id_falls_back_to_constraint_name():
    (rule,) = translate_checks(
        [
            {
                "constraint": "isComplete",
                "column": "id",
                "constraint_name": "id_is_complete",
            }
        ],
        DATASET,
    )
    assert rule.rule_id == "id_is_complete"
    assert rule.predicate.operator is Comparison.EQ


def test_translate_plan_matches_direct_construction():
    checks = [
        check(),
        check(
            alias="named", constraint="hasCompleteness", assertion="lambda x: x > 0.5"
        ),
    ]
    plan = translate_plan(checks, DATASET)
    assert plan == translate_plan(checks, DATASET)
    assert plan.fingerprint == translate_plan(checks, DATASET).fingerprint


def test_translated_plan_evaluates_like_a_direct_plan():
    plan = translate_plan([check(alias="complete", column="id")], DATASET)
    capabilities = CapabilitySet("any", "1", frozenset(MetricKind))
    complete_values = {metric: 5 for metric in plan.metrics}
    (outcome,) = plan.evaluate(complete_values, capabilities)
    assert outcome.success is True
    partial_values = {
        metric: 4 if metric.kind is MetricKind.PRESENT_COUNT else 5
        for metric in plan.metrics
    }
    (outcome,) = plan.evaluate(partial_values, capabilities)
    assert outcome.success is False


def test_rejects_unsupported_constraints_and_names_the_legacy_path():
    with pytest.raises(ConfigurationError, match="isUnique"):
        translate_checks([check(constraint="isUnique", alias="unique")], DATASET)
    with pytest.raises(ConfigurationError, match="legacy engine path"):
        translate_checks([check(constraint=None)], DATASET)
    with pytest.raises(ConfigurationError, match="legacy engine path"):
        translate_checks([check(constraint=7)], DATASET)


def test_rejects_is_complete_with_assertion_or_kwargs():
    with pytest.raises(ConfigurationError, match="assertion"):
        translate_checks([check(assertion="lambda x: x >= 0.5")], DATASET)
    with pytest.raises(ConfigurationError, match="kwargs"):
        translate_checks([check(kwargs={"hint": "why"})], DATASET)


@pytest.mark.parametrize(
    "assertion",
    [
        "lambda x: x >= 0.5 and x <= 1",
        "lambda x: 0.5 <= x",
        "lambda x: x + 1 >= 2",
        "lambda x: x >= float('nan')",
        "lambda x: x >= 'high'",
        "lambda x, y: x >= 0.5",
        "lambda x: x >= 1e999",
        "lambda x: x != 0.5",
        "lambda x: x >= 0.5 if True else False",
        "x >= 0.5",
        "lambda x: x >= ",
        None,
    ],
)
def test_rejects_assertions_outside_the_single_comparison_shape(assertion):
    with pytest.raises(ConfigurationError):
        translate_checks(
            [check(constraint="hasCompleteness", assertion=assertion)], DATASET
        )


def test_rejects_has_size_with_column_or_missing_assertion():
    with pytest.raises(ConfigurationError):
        translate_checks(
            [
                {
                    "alias": "sized",
                    "constraint": "hasSize",
                    "column": "id",
                    "assertion": "lambda x: x >= 5",
                }
            ],
            DATASET,
        )
    with pytest.raises(ConfigurationError):
        translate_checks([{"alias": "sized", "constraint": "hasSize"}], DATASET)


def test_rejects_completeness_without_single_column():
    with pytest.raises(ConfigurationError):
        translate_checks([{"constraint": "isComplete", "alias": "no_column"}], DATASET)
    with pytest.raises(ConfigurationError):
        translate_checks([check(columns=["id", "name"], alias="many_columns")], DATASET)


def test_rejects_invalid_levels_and_check_shapes():
    with pytest.raises(ConfigurationError, match="Error"):
        translate_checks([check(level="Fatal")], DATASET)
    with pytest.raises(ConfigurationError):
        translate_checks("not-a-list", DATASET)
    with pytest.raises(ConfigurationError):
        translate_checks({"alias": "single"}, DATASET)
    with pytest.raises(ConfigurationError):
        translate_checks(["not-a-mapping"], DATASET)
    with pytest.raises(ConfigurationError):
        translate_checks([check(alias="bad id!")], DATASET)


def test_rejects_thresholds_outside_the_rule_domain():
    with pytest.raises(ConfigurationError, match="translated"):
        translate_checks(
            [
                check(
                    alias="too_complete",
                    constraint="hasCompleteness",
                    assertion="lambda x: x >= 1.5",
                )
            ],
            DATASET,
        )
    with pytest.raises(ConfigurationError, match="translated"):
        translate_checks(
            [
                {
                    "alias": "fractional_size",
                    "constraint": "hasSize",
                    "assertion": "lambda x: x >= 2.5",
                }
            ],
            DATASET,
        )


def test_rejects_duplicate_rule_ids_through_translate_plan():
    with pytest.raises(ConfigurationError, match="unique"):
        translate_plan([check(), check()], DATASET)


def test_translation_requires_typed_inputs_and_string_aliases():
    with pytest.raises(ConfigurationError, match="DatasetRef"):
        translate_checks([check()], "bars")
    with pytest.raises(ConfigurationError, match="severity"):
        translate_checks([check()], DATASET, default_severity="Error")
    with pytest.raises(ConfigurationError, match="alias"):
        translate_checks([check(alias=7)], DATASET)


def test_translates_parsed_pyhocon_config_trees():
    """pyhocon ConfigTree raises on one-argument .get; translation must not."""
    from pyhocon import ConfigFactory

    config = ConfigFactory.parse_string("""
        checks = [
            { alias = "enough_rows", constraint = "hasSize", assertion = "lambda x: x >= 5", level = "Error" }
            { alias = "id_complete", constraint = "isComplete", column = "id", level = "Error" }
            { alias = "score_present", constraint = "hasCompleteness", column = "score", assertion = "lambda x: x >= 0.5", level = "Warning" }
        ]
        """)
    rules = translate_checks(config["checks"], DATASET)
    assert [(rule.rule_id, rule.kind) for rule in rules] == [
        ("enough_rows", RuleKind.SIZE),
        ("id_complete", RuleKind.COMPLETENESS),
        ("score_present", RuleKind.COMPLETENESS),
    ]
    assert rules[0].predicate.threshold == Decimal(5)
    assert rules[1].predicate.operator is Comparison.EQ
    assert rules[2].predicate.threshold == Decimal("0.5")
    assert rules[2].severity is Severity.WARNING


def test_translates_distinctness_by_group_into_bounded_rules():
    checks = [
        {
            "alias": "ticker_bounds",
            "constraint": "DistinctnessByGroup",
            "columns": ["ticker", "book"],
            "group_by": ["exchange"],
            "min": 2,
            "max": 9,
            "level": "Error",
        }
    ]
    rules = translate_checks(checks, DATASET)
    assert [(rule.rule_id, rule.column.name) for rule in rules] == [
        ("ticker_bounds.ticker.min", "ticker"),
        ("ticker_bounds.ticker.max", "ticker"),
        ("ticker_bounds.book.min", "book"),
        ("ticker_bounds.book.max", "book"),
    ]
    minimum_rule = rules[0]
    assert minimum_rule.kind is RuleKind.GROUPED_DISTINCT
    assert minimum_rule.predicate.operator is Comparison.GE
    assert minimum_rule.predicate.threshold == Decimal(2)
    assert minimum_rule.group_by == (ColumnRef("exchange"),)
    assert minimum_rule.target is MetricKind.GROUP_MIN_DISTINCT
    assert minimum_rule.severity is Severity.ERROR
    maximum_rule = rules[1]
    assert maximum_rule.predicate.operator is Comparison.LE
    assert maximum_rule.target is MetricKind.GROUP_MAX_DISTINCT


def test_distinctness_one_sided_thresholds_translate_to_one_rule():
    (minimum_rule,) = translate_checks(
        [
            {
                "alias": "lo",
                "constraint": "DistinctnessByGroup",
                "columns": ["id"],
                "group_by": ["g"],
                "min": 2,
            }
        ],
        DATASET,
    )
    assert minimum_rule.rule_id == "lo.id.min"
    (maximum_rule,) = translate_checks(
        [
            {
                "alias": "hi",
                "constraint": "DistinctnessByGroup",
                "columns": ["id"],
                "group_by": ["g"],
                "max": 7,
            }
        ],
        DATASET,
    )
    assert maximum_rule.rule_id == "hi.id.max"
    assert maximum_rule.predicate.operator is Comparison.LE


def test_distinctness_rejects_disabled_thresholds_and_bad_shapes():
    base = {
        "alias": "d",
        "constraint": "DistinctnessByGroup",
        "columns": ["id"],
        "group_by": ["g"],
    }
    with pytest.raises(ConfigurationError, match="truthy min or max"):
        translate_checks([base], DATASET)
    with pytest.raises(ConfigurationError, match="truthy min or max"):
        translate_checks([dict(base, min=0, max=0)], DATASET)
    with pytest.raises(ConfigurationError, match="column names"):
        translate_checks(
            [
                {
                    "alias": "d",
                    "constraint": "DistinctnessByGroup",
                    "group_by": ["g"],
                    "min": 1,
                }
            ],
            DATASET,
        )
    with pytest.raises(ConfigurationError, match="group_by"):
        translate_checks(
            [
                {
                    "alias": "d",
                    "constraint": "DistinctnessByGroup",
                    "columns": ["id"],
                    "min": 1,
                }
            ],
            DATASET,
        )
    with pytest.raises(ConfigurationError, match="group_by"):
        translate_checks([dict(base, min=1, group_by=[])], DATASET)
    with pytest.raises(ConfigurationError, match="groups/v1"):
        translate_checks([dict(base, min=1, assertion="lambda x: x >= 1")], DATASET)


def test_translate_plan_rejects_mixed_semantic_versions():
    checks = [
        check(),
        {
            "alias": "d",
            "constraint": "DistinctnessByGroup",
            "columns": ["id"],
            "group_by": ["g"],
            "min": 1,
        },
    ]
    with pytest.raises(ConfigurationError, match="mix"):
        translate_plan(checks, DATASET)


def test_translates_is_non_negative_into_a_lower_bound_rule():
    (rule,) = translate_checks(
        [
            {
                "alias": "amount_check",
                "constraint": "isNonNegative",
                "column": "amount",
                "level": "Error",
            }
        ],
        DATASET,
    )
    assert rule.rule_id == "amount_check.low"
    assert rule.kind is RuleKind.VALUE_RANGE
    assert rule.target is MetricKind.COLUMN_MIN
    assert rule.predicate.operator is Comparison.GE
    assert rule.predicate.threshold == Decimal(0)


def test_translates_is_in_range_into_low_and_high_rules():
    low, high = translate_checks(
        [
            {
                "alias": "amount_range",
                "constraint": "isInRange",
                "column": "amount",
                "min": -5,
                "max": 2.5,
            }
        ],
        DATASET,
    )
    assert low.rule_id == "amount_range.low"
    assert low.predicate.operator is Comparison.GE
    assert low.predicate.threshold == Decimal(-5)
    assert low.target is MetricKind.COLUMN_MIN
    assert high.rule_id == "amount_range.high"
    assert high.predicate.operator is Comparison.LE
    assert high.predicate.threshold == Decimal("2.5")
    assert high.target is MetricKind.COLUMN_MAX


def test_rejects_range_checks_without_bounds_or_column():
    base = {"alias": "r", "constraint": "isInRange", "column": "amount"}
    with pytest.raises(ConfigurationError, match="min or max"):
        translate_checks([base], DATASET)
    with pytest.raises(ConfigurationError, match="string column"):
        translate_checks([dict(base, min=1, column=7)], DATASET)
    with pytest.raises(ConfigurationError, match="assertion"):
        translate_checks([dict(base, min=1, assertion="lambda x: x")], DATASET)


def test_translator_imports_without_optional_dependencies():
    script = "import dq.portable_config as module; "
    script += "print(sorted(module.SUPPORTED_CONSTRAINTS))"
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(REPOSITORY_ROOT)
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=REPOSITORY_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
