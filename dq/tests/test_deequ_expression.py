# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

from unittest.mock import MagicMock

import pytest
from pydeequ.checks import ConstrainableDataTypes

from dq.engine.deequ.expression import (
    apply_constraint_suggestion,
    parse_assertion,
)
from dq.exceptions import ConfigurationError


@pytest.mark.parametrize(
    "expression,value,expected",
    [
        ("lambda value: value == 1", 1, True),
        ("lambda value: 0.95 <= value <= 1", 0.97, True),
        ("lambda value: value < 0.1 or value > 0.9", 0.5, False),
        ("lambda value: not value < 0", 3, True),
        ("lambda value: (value * 100) >= 95", 0.96, True),
    ],
)
def test_parse_assertion_supports_bounded_metric_expressions(
    expression, value, expected
):
    assert parse_assertion(expression)(value) is expected


@pytest.mark.parametrize(
    "expression",
    [
        "value == 1",
        "lambda: True",
        "lambda left, right: left == right",
        "lambda value: __import__('os').system('id')",
        "lambda value: value.__class__",
        "lambda value: value[0]",
        "lambda value: [item for item in value]",
        "lambda value: 'true'",
    ],
)
def test_parse_assertion_rejects_executable_or_ambiguous_python(expression):
    with pytest.raises(ConfigurationError):
        assertion = parse_assertion(expression)
        assertion(1)


@pytest.mark.parametrize("expression", [None, "", "   ", "lambda value:"])
def test_parse_assertion_rejects_missing_or_invalid_syntax(expression):
    with pytest.raises(ConfigurationError):
        parse_assertion(expression)


def test_parse_assertion_rejects_non_boolean_runtime_result():
    assertion = parse_assertion("lambda value: value")

    with pytest.raises(ConfigurationError, match="must evaluate to a Boolean"):
        assertion(1)


def test_parse_assertion_rejects_unsupported_comparison_operator():
    with pytest.raises(ConfigurationError, match="Unsupported assertion"):
        parse_assertion("lambda value: value is None")


def test_apply_constraint_suggestion_calls_one_public_method():
    check = MagicMock()
    check.hasCompleteness.return_value = "updated-check"

    result = apply_constraint_suggestion(
        check,
        '.hasCompleteness("price", lambda value: value >= 0.99)',
    )

    assert result == "updated-check"
    column, assertion = check.hasCompleteness.call_args.args
    assert column == "price"
    assert assertion(1.0) is True
    assert assertion(0.5) is False


def test_apply_constraint_suggestion_supports_constrainable_datatype():
    check = MagicMock()
    check.hasDataType.return_value = "updated-check"

    apply_constraint_suggestion(
        check,
        '.hasDataType("quantity", ConstrainableDataTypes.Integral)',
    )

    check.hasDataType.assert_called_once_with(
        "quantity", ConstrainableDataTypes.Integral
    )


def test_apply_constraint_suggestion_supports_literal_collections():
    check = MagicMock()

    apply_constraint_suggestion(
        check,
        ".isContainedIn('status', ['new', 'complete'], hint='known status')",
    )

    check.isContainedIn.assert_called_once_with(
        "status", ["new", "complete"], hint="known status"
    )


@pytest.mark.parametrize("expression", [None, "", "   ", ".isComplete("])
def test_apply_constraint_suggestion_rejects_missing_or_invalid_syntax(expression):
    with pytest.raises(ConfigurationError):
        apply_constraint_suggestion(MagicMock(), expression)


@pytest.mark.parametrize(
    "expression,message",
    [
        (".isComplete(**{'column': 'id'})", "Expanded keyword"),
        (".isComplete([column])", "non-literal"),
        (
            '.hasDataType("id", ConstrainableDataTypes.Unknown)',
            "Unknown constrainable data type",
        ),
        (".isComplete(column)", "Unsupported constraint argument"),
    ],
)
def test_apply_constraint_suggestion_rejects_unsupported_arguments(expression, message):
    with pytest.raises(ConfigurationError, match=message):
        apply_constraint_suggestion(MagicMock(), expression)


@pytest.mark.parametrize(
    "expression",
    [
        ".__getattribute__('secret')",
        ".isComplete(open('/tmp/secret'))",
        ".isComplete('id').isUnique('id')",
        ".missingConstraint('id')",
    ],
)
def test_apply_constraint_suggestion_rejects_non_dsl_shapes(expression):
    with pytest.raises(ConfigurationError):
        apply_constraint_suggestion(object(), expression)
