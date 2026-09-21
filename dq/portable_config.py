# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Translate a bounded legacy HOCON check subset into portable count plans.

Only rules exactly representable in counts/v1 cross this boundary:
``isComplete``, ``hasCompleteness`` with one literal comparison, and ``hasSize``
with one integer comparison. Everything else fails closed and stays on the
legacy engine path. Counts/v1 semantics (null and NaN exclusion, empty-dataset
failure) are owned by ``dq.plan`` and can differ from legacy PyDeequ results;
callers certify the snapshot digest because legacy configuration has none.
"""

from __future__ import annotations

import ast
from collections.abc import Iterable, Mapping
from decimal import Decimal

from dq.exceptions import ConfigurationError
from dq.plan import (
    ColumnRef,
    Comparison,
    DatasetRef,
    ExecutionPlan,
    Predicate,
    RuleKind,
    RuleSpec,
    Severity,
)

SUPPORTED_CONSTRAINTS = frozenset({"isComplete", "hasCompleteness", "hasSize"})

_COMPARISONS = {
    ast.Gt: Comparison.GT,
    ast.GtE: Comparison.GE,
    ast.Lt: Comparison.LT,
    ast.LtE: Comparison.LE,
    ast.Eq: Comparison.EQ,
}
_LEVELS = {"Error": Severity.ERROR, "Warning": Severity.WARNING}
_COMPARISON_NAMES = ("ge", "gt", "le", "lt", "eq")


def translate_plan(checks, dataset: DatasetRef) -> ExecutionPlan:
    """Translate legacy checks and build one canonical portable plan."""
    return ExecutionPlan(translate_checks(checks, dataset))


def translate_checks(
    checks: object, dataset: DatasetRef, default_severity: Severity = Severity.ERROR
) -> tuple[RuleSpec, ...]:
    """Translate a check list into portable rules, rejecting the rest."""
    if type(dataset) is not DatasetRef:
        raise ConfigurationError("translation requires a typed DatasetRef")
    if type(default_severity) is not Severity:
        raise ConfigurationError("translation requires a typed default severity")
    if isinstance(checks, (str, bytes, Mapping)) or not isinstance(checks, Iterable):
        raise ConfigurationError("translation requires an iterable of check mappings")
    return tuple(_translate_check(check, dataset, default_severity) for check in checks)


def _translate_check(
    check: object, dataset: DatasetRef, default_severity: Severity
) -> RuleSpec:
    if not isinstance(check, Mapping):
        raise ConfigurationError("each check must be a mapping of HOCON fields")

    constraint = check.get("constraint", None)
    if type(constraint) is not str or constraint not in SUPPORTED_CONSTRAINTS:
        raise ConfigurationError(
            f"constraint {constraint!r} cannot be translated to counts/v1; only "
            f"{sorted(SUPPORTED_CONSTRAINTS)} are supported; keep this rule on "
            "the legacy engine path"
        )

    rule_id = check.get("alias", None)
    if rule_id is None:
        rule_id = check.get("constraint_name", None)
    if type(rule_id) is not str:
        raise ConfigurationError(
            f"check with constraint {constraint!r} requires a string alias or "
            "constraint_name"
        )

    level = check.get("level", None)
    if level is None:
        severity = default_severity
    elif type(level) is str and level in _LEVELS:
        severity = _LEVELS[level]
    else:
        raise ConfigurationError(
            f"check {rule_id!r} level must be Error or Warning, not {level!r}"
        )

    for rejected in ("kwargs", "hint", "columns"):
        if check.get(rejected, None) is not None:
            raise ConfigurationError(
                f"check {rule_id!r} cannot translate {rejected!r} into counts/v1; "
                "keep this rule on the legacy engine path"
            )

    column = check.get("column", None)
    assertion = check.get("assertion", None)
    if constraint == "isComplete":
        if assertion is not None:
            raise ConfigurationError(
                f"check {rule_id!r} isComplete has no assertion in counts/v1; "
                "use hasCompleteness for a threshold"
            )
        kind, operator, threshold = RuleKind.COMPLETENESS, Comparison.EQ, Decimal(1)
    elif constraint == "hasCompleteness":
        kind = RuleKind.COMPLETENESS
        operator, threshold = _parse_comparison(assertion, rule_id)
    else:
        if column is not None:
            raise ConfigurationError(
                f"check {rule_id!r} hasSize cannot bind a column in counts/v1"
            )
        kind = RuleKind.SIZE
        operator, threshold = _parse_comparison(assertion, rule_id)

    try:
        column_ref = None
        if kind is RuleKind.COMPLETENESS:
            if type(column) is not str:
                raise ConfigurationError(
                    f"check {rule_id!r} requires exactly one string column for "
                    "completeness"
                )
            column_ref = ColumnRef(column)
        return RuleSpec(
            rule_id,
            dataset,
            kind,
            Predicate(operator, threshold),
            column_ref,
            severity,
        )
    except ConfigurationError as error:
        raise ConfigurationError(
            f"check {rule_id!r} cannot be translated: {error}"
        ) from error


def _parse_comparison(assertion, rule_id):
    """Parse ``lambda <arg>: <arg> <op> <number>`` without executing it."""
    if type(assertion) is not str:
        raise ConfigurationError(
            f"check {rule_id!r} requires a single-comparison lambda assertion"
        )
    try:
        parsed = ast.parse(assertion, mode="eval")
    except SyntaxError as error:
        raise ConfigurationError(
            f"check {rule_id!r} assertion is not valid Python: {error.msg}"
        ) from error
    body = parsed.body
    if (
        not isinstance(body, ast.Lambda)
        or len(body.args.args) != 1
        or body.args.posonlyargs
        or body.args.vararg is not None
        or body.args.kwonlyargs
        or body.args.kwarg is not None
        or body.args.defaults
        or body.args.kw_defaults
    ):
        raise ConfigurationError(
            f"check {rule_id!r} assertion must be a lambda with one argument"
        )
    argument = body.args.args[0].arg
    comparison = body.body
    if not (
        isinstance(comparison, ast.Compare)
        and len(comparison.ops) == 1
        and isinstance(comparison.left, ast.Name)
        and comparison.left.id == argument
    ):
        raise ConfigurationError(
            f"check {rule_id!r} assertion must compare its argument once with a "
            "numeric literal, like lambda x: x >= 0.5"
        )
    operator = comparison.ops[0]
    if type(operator) not in _COMPARISONS:
        raise ConfigurationError(
            f"check {rule_id!r} assertion operator must be one of "
            f"{', '.join(_COMPARISON_NAMES)}"
        )
    literal = comparison.comparators[0]
    if not (isinstance(literal, ast.Constant) and type(literal.value) in (int, float)):
        raise ConfigurationError(
            f"check {rule_id!r} assertion threshold must be a numeric literal"
        )
    value = literal.value
    if type(value) is float and (
        value != value or value in (float("inf"), float("-inf"))
    ):
        raise ConfigurationError(
            f"check {rule_id!r} assertion threshold must be finite"
        )
    return _COMPARISONS[type(operator)], Decimal(str(value))
