# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Differential certification: native Spark counts adapter versus PyDeequ.

This is the ADR-002 promotion evidence for the counts/v1 subset. Both engines
run the same logical rules over shared fixtures and their decisions must
agree wherever the semantics coincide (sizes, null handling, decimals, empty
inputs, exact thresholds).

One divergence is intentional and pinned: counts/v1 excludes floating-point
NaN from present counts, while Deequ's completeness analyzer treats NaN as
present. Rule packs that rely on either behavior must know which engine
produced their evidence; the outcome's adapter field names it.
"""

from decimal import Decimal
import random

import pytest
from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationResult, VerificationSuite

from dq.plan import (
    ColumnRef,
    Comparison,
    DatasetRef,
    ExecutionPlan,
    Predicate,
    RuleKind,
    RuleSpec,
)
from dq.spark_adapter import execute_plan

pytestmark = pytest.mark.spark

_MIXED_ROWS = [
    ("a", 1.5, Decimal("1.25")),
    ("", float("nan"), None),
    ("b", float("inf"), Decimal("3.50")),
    (None, float("-inf"), Decimal("0.00")),
    ("c", None, Decimal("2.00")),
]
_MIXED_SCHEMA = "id string, score double, amount decimal(10,2)"


def deequ_decisions(spark, dataframe, checks):
    """Run PyDeequ once and map check description to constraint_status."""
    builder = VerificationSuite(spark).onData(dataframe)
    for description, chain in checks:
        builder = builder.addCheck(chain(Check(spark, CheckLevel.Error, description)))
    result = builder.run()
    decisions = {}
    for row in VerificationResult.checkResultsAsDataFrame(spark, result).collect():
        decisions[row["check"]] = row["constraint_status"]
    assert len(decisions) == len(checks), "check descriptions must be unique"
    return decisions


def native_decisions(dataframe, rules, name="diff"):
    """Run the portable plan on the native Spark adapter."""
    dataset = DatasetRef(name, "d" * 64)
    plan = ExecutionPlan(
        tuple(
            RuleSpec(
                rule_id,
                dataset,
                kind,
                Predicate(operator, Decimal(text)),
                ColumnRef(column) if column else None,
            )
            for rule_id, kind, operator, text, column in rules
        )
    )
    return {
        outcome.to_legacy()["details"]["rule_id"]: outcome.to_legacy()["success"]
        for outcome in execute_plan(plan, {name: dataframe})
    }


def assert_decisions_agree(deequ, native, description, rule_id):
    deequ_pass = deequ[description] == "Success"
    assert deequ_pass == native[rule_id], (
        f"{description}/{rule_id}: deequ={deequ[description]}, "
        f"native={native[rule_id]}"
    )


def test_mixed_dataset_decisions_agree(spark):
    dataframe = spark.createDataFrame(_MIXED_ROWS, _MIXED_SCHEMA)
    deequ = deequ_decisions(
        spark,
        dataframe,
        [
            ("sz_ge_one", lambda check: check.hasSize(lambda x: x >= 1)),
            ("sz_under_five", lambda check: check.hasSize(lambda x: x < 5)),
            (
                "cmp_id_half",
                lambda check: check.hasCompleteness("id", lambda x: x >= 0.5),
            ),
            ("cmp_id_eq_one", lambda check: check.isComplete("id")),
            (
                "cmp_amount_eighty",
                lambda check: check.hasCompleteness("amount", lambda x: x >= 0.8),
            ),
            (
                "cmp_amount_eq_one",
                lambda check: check.isComplete("amount"),
            ),
        ],
    )
    native = native_decisions(
        dataframe,
        [
            ("sz_ge_one", RuleKind.SIZE, Comparison.GE, "1", None),
            ("sz_under_five", RuleKind.SIZE, Comparison.LT, "5", None),
            ("cmp_id_half", RuleKind.COMPLETENESS, Comparison.GE, "0.5", "id"),
            ("cmp_id_eq_one", RuleKind.COMPLETENESS, Comparison.EQ, "1", "id"),
            (
                "cmp_amount_eighty",
                RuleKind.COMPLETENESS,
                Comparison.GE,
                "0.8",
                "amount",
            ),
            ("cmp_amount_eq_one", RuleKind.COMPLETENESS, Comparison.EQ, "1", "amount"),
        ],
    )
    for description in deequ:
        assert_decisions_agree(deequ, native, description, description)


def test_exact_threshold_boundary_agrees(spark):
    # id is present for 2 of 3 rows; the threshold is the closest double to 2/3.
    dataframe = spark.createDataFrame([("a", 1), ("b", 2), (None, 3)], ["id", "filler"])
    deequ = deequ_decisions(
        spark,
        dataframe,
        [
            (
                "cmp_two_thirds",
                lambda check: check.hasCompleteness(
                    "id", lambda x: x >= 0.6666666666666666
                ),
            )
        ],
    )
    native = native_decisions(
        dataframe,
        [
            (
                "cmp_two_thirds",
                RuleKind.COMPLETENESS,
                Comparison.GE,
                "0.6666666666666666",
                "id",
            )
        ],
    )
    assert_decisions_agree(deequ, native, "cmp_two_thirds", "cmp_two_thirds")


def test_empty_dataset_fails_in_both_engines(spark):
    dataframe = spark.createDataFrame([], "id string, score double")
    deequ = deequ_decisions(
        spark,
        dataframe,
        [
            ("sz_ge_one", lambda check: check.hasSize(lambda x: x >= 1)),
            ("cmp_id_eq_one", lambda check: check.isComplete("id")),
        ],
    )
    native = native_decisions(
        dataframe,
        [
            ("sz_ge_one", RuleKind.SIZE, Comparison.GE, "1", None),
            ("cmp_id_eq_one", RuleKind.COMPLETENESS, Comparison.EQ, "1", "id"),
        ],
    )
    assert deequ["sz_ge_one"] == "Failure"
    assert deequ["cmp_id_eq_one"] == "Failure", "deequ reports an empty state failure"
    assert native["sz_ge_one"] is False
    assert native["cmp_id_eq_one"] is False


def test_nan_divergence_is_pinned_and_documented(spark):
    # score: 1.5, NaN, inf, -inf, null. Deequ counts NaN as present (4/5 = 0.8,
    # passes the 0.7 threshold); counts/v1 excludes NaN (3/5 = 0.6, fails).
    dataframe = spark.createDataFrame(_MIXED_ROWS, _MIXED_SCHEMA)
    deequ = deequ_decisions(
        spark,
        dataframe,
        [
            (
                "cmp_score_seventy",
                lambda check: check.hasCompleteness("score", lambda x: x >= 0.7),
            )
        ],
    )
    native = native_decisions(
        dataframe,
        [("cmp_score_seventy", RuleKind.COMPLETENESS, Comparison.GE, "0.7", "score")],
    )
    assert deequ["cmp_score_seventy"] == "Success", "deequ counts NaN as complete"
    assert native["cmp_score_seventy"] is False, "counts/v1 excludes NaN"


def test_generated_dataset_decisions_agree(spark):
    rows = []
    generator = random.Random(20260920)
    for index in range(200):
        key = None if generator.random() < 0.15 else f"v{index}"
        number = None if generator.random() < 0.30 else float(index)
        rows.append((key, number))
    dataframe = spark.createDataFrame(rows, ["k", "n"])
    deequ = deequ_decisions(
        spark,
        dataframe,
        [
            (
                "cmp_k_half",
                lambda check: check.hasCompleteness("k", lambda x: x >= 0.5),
            ),
            ("cmp_k_eq_one", lambda check: check.isComplete("k")),
            (
                "cmp_n_half",
                lambda check: check.hasCompleteness("n", lambda x: x >= 0.5),
            ),
            (
                "cmp_n_ninety",
                lambda check: check.hasCompleteness("n", lambda x: x >= 0.9),
            ),
        ],
    )
    native = native_decisions(
        dataframe,
        [
            ("cmp_k_half", RuleKind.COMPLETENESS, Comparison.GE, "0.5", "k"),
            ("cmp_k_eq_one", RuleKind.COMPLETENESS, Comparison.EQ, "1", "k"),
            ("cmp_n_half", RuleKind.COMPLETENESS, Comparison.GE, "0.5", "n"),
            ("cmp_n_ninety", RuleKind.COMPLETENESS, Comparison.GE, "0.9", "n"),
        ],
    )
    for description in deequ:
        assert_decisions_agree(deequ, native, description, description)
