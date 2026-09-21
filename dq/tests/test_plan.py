# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Pure contracts for the first portable count-based rule subset."""

from dataclasses import FrozenInstanceError, replace
from decimal import Decimal, localcontext
import json
import subprocess
import sys

import pytest

from dq.exceptions import ConfigurationError, ValidationError
from dq.plan import (
    CapabilitySet,
    ColumnRef,
    Comparison,
    DatasetRef,
    ExecutionPlan,
    MetricKey,
    MetricKind,
    Predicate,
    RuleKind,
    RuleSpec,
    SEMANTICS_VERSION,
    Severity,
)

DATASET = DatasetRef("bars", "a" * 64)
COLUMN = ColumnRef("close")
CAPABILITIES = CapabilitySet("test_adapter", "1.0", frozenset(MetricKind))


def rule(identifier="complete", threshold="1", kind=RuleKind.COMPLETENESS):
    return RuleSpec(
        identifier,
        DATASET,
        kind,
        Predicate(Comparison.GE, Decimal(threshold)),
        COLUMN if kind is RuleKind.COMPLETENESS else None,
    )


def metric_values(plan, rows, present):
    return {
        metric: rows if metric.kind is MetricKind.ROW_COUNT else present
        for metric in plan.metrics
    }


def test_plan_deduplicates_metrics_and_has_canonical_identity():
    rules = (rule("strict"), rule("lenient", "0.9"), rule("size", "1", RuleKind.SIZE))
    plan = ExecutionPlan(rules)
    assert tuple(item.rule_id for item in plan.rules) == ("lenient", "size", "strict")
    assert len(plan.metrics) == 2
    assert plan == ExecutionPlan(tuple(reversed(rules)))
    assert plan.fingerprint == ExecutionPlan(tuple(reversed(rules))).fingerprint
    assert hash(plan) == hash(ExecutionPlan(tuple(reversed(rules))))
    assert len(plan.fingerprint) == 64
    assert json.loads(plan.canonical_json())["semantic_version"] == SEMANTICS_VERSION
    with pytest.raises(FrozenInstanceError):
        plan.rules = ()


def test_decimal_identity_is_context_independent():
    first = ExecutionPlan((rule(threshold="0.900"),))
    with localcontext() as context:
        context.prec = 2
        second = ExecutionPlan((rule(threshold="0.9"),))
        assert first.fingerprint == second.fingerprint
        assert Predicate(Comparison.EQ, Decimal("-0.00")).threshold_text == "0"


@pytest.mark.parametrize("field", ["digest", "column", "threshold", "severity"])
def test_semantic_changes_invalidate_plan_identity(field):
    original = rule()
    changes = {
        "digest": {"dataset": DatasetRef("bars", "b" * 64)},
        "column": {"column": ColumnRef("open")},
        "threshold": {"predicate": Predicate(Comparison.GE, Decimal("0.9"))},
        "severity": {"severity": Severity.WARNING},
    }
    assert (
        ExecutionPlan((original,)).fingerprint
        != ExecutionPlan((replace(original, **changes[field]),)).fingerprint
    )


@pytest.mark.parametrize(
    "name",
    ["", "a.b", "x; DROP TABLE x", "1bad", "x" * 129, None],
    ids=["empty", "nested", "sql", "digit", "long", "none"],
)
def test_column_identifiers_are_bounded_and_not_sql(name):
    with pytest.raises(ConfigurationError):
        ColumnRef(name)


@pytest.mark.parametrize("digest", [None, "wrong", "g" * 64])
def test_dataset_requires_snapshot_digest(digest):
    with pytest.raises(ConfigurationError):
        DatasetRef("bars", digest)


def test_digest_normalization_and_name_validation():
    assert DatasetRef("bars", "A" * 64) == DATASET
    with pytest.raises(ConfigurationError):
        DatasetRef("bad alias", "a" * 64)


@pytest.mark.parametrize(
    "threshold",
    [
        True,
        0.9,
        Decimal("NaN"),
        Decimal("Infinity"),
        Decimal("1e100"),
        Decimal("1e-100"),
    ],
    ids=["bool", "float", "nan", "inf", "large-exp", "small-exp"],
)
def test_predicates_require_bounded_exact_decimals(threshold):
    with pytest.raises(ConfigurationError):
        Predicate(Comparison.GE, threshold)


def test_invalid_tag_and_rule_shapes_fail_early():
    with pytest.raises(ConfigurationError):
        Predicate(">=", Decimal(1))
    with pytest.raises(ValidationError, match="Fraction"):
        Predicate(Comparison.GE, Decimal(1)).matches(0.9)
    for changes in (
        {"rule_id": ""},
        {"dataset": "bars"},
        {"kind": "completeness"},
        {"predicate": "lambda x: True"},
        {"severity": "Error"},
        {"column": None},
        {"column": "close"},
        {"kind": RuleKind.SIZE},
        {"predicate": Predicate(Comparison.GE, Decimal("1.01"))},
    ):
        with pytest.raises(ConfigurationError):
            replace(rule(), **changes)
    for threshold in ("-1", "1.5"):
        with pytest.raises(ConfigurationError):
            rule(threshold=threshold, kind=RuleKind.SIZE)


def test_metric_shapes_are_checked():
    for kind, column in (
        ("row_count", None),
        (MetricKind.ROW_COUNT, COLUMN),
        (MetricKind.PRESENT_COUNT, None),
        (MetricKind.PRESENT_COUNT, "x"),
    ):
        with pytest.raises(ConfigurationError):
            MetricKey(DATASET, kind, column)
    with pytest.raises(ConfigurationError):
        MetricKey("bars", MetricKind.ROW_COUNT)
    with pytest.raises(ConfigurationError):
        MetricKey(DATASET, MetricKind.ROW_COUNT, COLUMN)
    with pytest.raises(ConfigurationError):
        MetricKey(DATASET, MetricKind.ROW_COUNT, semantic_version="other")
    assert (
        MetricKey(DATASET, MetricKind.ROW_COUNT).semantic_version == SEMANTICS_VERSION
    )


def test_plan_rejects_mutable_empty_duplicate_and_conflicting_inputs():
    for rules in ([], (), (rule(), rule()), ("rule",)):
        with pytest.raises(ConfigurationError):
            ExecutionPlan(rules)
    with pytest.raises(ConfigurationError, match="snapshot"):
        ExecutionPlan(
            (rule("a"), replace(rule("b"), dataset=DatasetRef("bars", "b" * 64)))
        )
    with pytest.raises(ConfigurationError, match="10000"):
        ExecutionPlan((rule(),) * 10_001)


def test_capabilities_fail_before_any_adapter_execution():
    plan = ExecutionPlan((rule(),))
    plan.validate_for(CAPABILITIES)
    for capabilities in (
        replace(CAPABILITIES, metrics=frozenset({MetricKind.ROW_COUNT})),
        replace(CAPABILITIES, semantic_version="counts/other"),
    ):
        with pytest.raises(ConfigurationError, match="capabilit"):
            plan.validate_for(capabilities)
        with pytest.raises(ConfigurationError, match="capabilit"):
            plan.evaluate(object(), capabilities)
    for changes in (
        {"metrics": set(MetricKind)},
        {"metrics": frozenset({"row_count"})},
        {"adapter": ""},
        {"version": ""},
        {"semantic_version": ""},
    ):
        with pytest.raises(ConfigurationError):
            replace(CAPABILITIES, **changes)
    with pytest.raises(ConfigurationError):
        plan.validate_for("spark")


@pytest.mark.parametrize(
    "comparison, expected",
    [
        (Comparison.GE, True),
        (Comparison.GT, False),
        (Comparison.LE, True),
        (Comparison.LT, False),
        (Comparison.EQ, True),
    ],
)
def test_predicate_comparisons(comparison, expected):
    plan = ExecutionPlan(
        (
            replace(
                rule(kind=RuleKind.SIZE), predicate=Predicate(comparison, Decimal(1))
            ),
        )
    )
    assert plan.evaluate(metric_values(plan, 1, 1), CAPABILITIES)[0].success is expected


def test_completeness_uses_exact_ratio_not_decimal_context_rounding():
    plan = ExecutionPlan((rule(threshold="0.33333333333333333333333333333333333334"),))
    with localcontext() as context:
        context.prec = 2
        outcome = plan.evaluate(metric_values(plan, 3, 1), CAPABILITIES)[0]
    assert outcome.success is False
    result = outcome.to_legacy()
    assert result["details"]["observed"] == {"numerator": 1, "denominator": 3}
    assert result["details"]["plan_sha256"] == plan.fingerprint
    assert result["rule_id"] == "complete"


def test_empty_completeness_fails_even_with_zero_threshold():
    plan = ExecutionPlan((rule(threshold="0"), rule("size", "0", RuleKind.SIZE)))
    outcomes = plan.evaluate(metric_values(plan, 0, 0), CAPABILITIES)
    assert [outcome.success for outcome in outcomes] == [False, True]
    assert outcomes[0].to_legacy()["details"]["reason"] == "empty_dataset"


@pytest.mark.parametrize(
    "value",
    [-1, True, 0.5, float("nan"), 2**64],
    ids=["negative", "bool", "float", "nan", "overflow"],
)
def test_count_values_are_exact_unsigned_64_bit_integers(value):
    plan = ExecutionPlan((rule(),))
    with pytest.raises(ValidationError):
        plan.evaluate(metric_values(plan, value, 0), CAPABILITIES)


def test_missing_extra_and_inconsistent_metrics_are_execution_errors():
    plan = ExecutionPlan((rule(),))
    for values in (
        {},
        [],
        {**metric_values(plan, 1, 1), "extra": 1},
        metric_values(plan, 1, 2),
    ):
        with pytest.raises(ValidationError):
            plan.evaluate(values, CAPABILITIES)

    wrong_snapshot = {
        replace(key, dataset=DatasetRef("bars", "b" * 64)): value
        for key, value in metric_values(plan, 1, 1).items()
    }
    with pytest.raises(ValidationError, match="required metrics"):
        plan.evaluate(wrong_snapshot, CAPABILITIES)


def test_all_counts_are_type_checked_before_cross_metric_comparison():
    plan = ExecutionPlan((rule(),))
    values = dict(reversed(tuple(metric_values(plan, None, 1).items())))
    with pytest.raises(ValidationError, match="unsigned"):
        plan.evaluate(values, CAPABILITIES)


def test_exact_counts_are_partition_invariant_and_support_uint64_boundary():
    plan = ExecutionPlan((rule(threshold="0.6"),))
    direct = plan.evaluate(metric_values(plan, 10, 6), CAPABILITIES)
    partitions = [(2, 1), (3, 1), (5, 4)]
    merged = plan.evaluate(
        metric_values(
            plan, sum(p[0] for p in partitions), sum(p[1] for p in partitions)
        ),
        CAPABILITIES,
    )
    assert direct == merged
    max_count = 2**64 - 1
    assert plan.evaluate(metric_values(plan, max_count, max_count), CAPABILITIES)[
        0
    ].success


def test_snapshot_and_column_keys_do_not_alias_between_datasets():
    second = replace(rule("other"), dataset=DatasetRef("other_bars", "b" * 64))
    plan = ExecutionPlan((rule(), second))
    assert len(plan.metrics) == 4
    assert len({metric.dataset for metric in plan.metrics}) == 2


def test_rule_exports_are_independent_and_logical_digest_is_adapter_independent():
    plan = ExecutionPlan((rule(),))
    exported = plan.rules[0].to_dict()
    exported["dataset"]["name"] = "changed"
    assert plan.rules[0].dataset.name == "bars"
    outcomes = [
        plan.evaluate(metric_values(plan, 2, 2), capability)[0].to_legacy()
        for capability in (CAPABILITIES, replace(CAPABILITIES, adapter="other"))
    ]
    assert (
        outcomes[0]["details"]["plan_sha256"] == outcomes[1]["details"]["plan_sha256"]
    )
    assert outcomes[0]["details"]["adapter"] != outcomes[1]["details"]["adapter"]


def test_warning_failure_is_not_silently_passing_evidence():
    plan = ExecutionPlan((replace(rule(), severity=Severity.WARNING),))
    assert not plan.evaluate(metric_values(plan, 1, 0), CAPABILITIES)[0].success


def test_kernel_imports_without_optional_dependencies():
    code = """
import sys
class BlockEngines:
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in {'pyspark','pydeequ','great_expectations','datafusion','pyarrow','pyhocon'}:
            raise AssertionError(fullname)
sys.meta_path.insert(0, BlockEngines())
from dq.plan import ExecutionPlan, RuleSpec, MetricKey, CapabilitySet
"""
    subprocess.run([sys.executable, "-c", code], check=True, timeout=30)


def test_portable_example_is_an_offline_executable_contract():
    result = subprocess.run(
        [sys.executable, "-m", "examples.portable_counts"],
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    output = json.loads(result.stdout)
    assert output["demo_only"] is True
    assert len(output["checks"]) == 2
    assert all(check["success"] for check in output["checks"])
    assert all(
        check["details"]["plan_sha256"] == output["plan_sha256"]
        for check in output["checks"]
    )
