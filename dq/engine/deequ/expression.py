# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Restricted expression adapters for PyDeequ configuration.

Configuration is data, not Python source.  This module interprets the small
expression shapes emitted by PyDeequ and accepted by this framework without
executing arbitrary code.
"""

from __future__ import annotations

import ast
import operator
from collections.abc import Callable, Mapping
from typing import Any

from pydeequ.checks import ConstrainableDataTypes

from dq.exceptions import ConfigurationError

_BINARY_OPERATORS: Mapping[type[ast.operator], Callable[[Any, Any], Any]] = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.Mod: operator.mod,
}
_COMPARISON_OPERATORS: Mapping[type[ast.cmpop], Callable[[Any, Any], bool]] = {
    ast.Eq: operator.eq,
    ast.NotEq: operator.ne,
    ast.Lt: operator.lt,
    ast.LtE: operator.le,
    ast.Gt: operator.gt,
    ast.GtE: operator.ge,
}
_UNARY_OPERATORS: Mapping[type[ast.unaryop], Callable[[Any], Any]] = {
    ast.Not: operator.not_,
    ast.UAdd: operator.pos,
    ast.USub: operator.neg,
}


def parse_assertion(expression: str) -> Callable[[Any], bool]:
    """Parse a one-argument assertion lambda using a restricted grammar.

    Supported bodies contain numeric/Boolean constants, the lambda argument,
    comparisons, ``and``/``or``/``not``, and basic arithmetic.  Function calls,
    attribute access, subscripting, comprehensions, and imports are rejected.
    """
    if not isinstance(expression, str) or not expression.strip():
        raise ConfigurationError("Assertion must be a non-empty lambda expression")

    try:
        parsed = ast.parse(expression, mode="eval")
    except SyntaxError as error:
        raise ConfigurationError(f"Invalid assertion syntax: {error.msg}") from error

    if not isinstance(parsed.body, ast.Lambda):
        raise ConfigurationError("Assertion must be a lambda expression")

    lambda_node = parsed.body
    arguments = lambda_node.args
    if (
        len(arguments.args) != 1
        or arguments.posonlyargs
        or arguments.vararg is not None
        or arguments.kwonlyargs
        or arguments.kwarg is not None
        or arguments.defaults
        or arguments.kw_defaults
    ):
        raise ConfigurationError("Assertion lambda must accept exactly one argument")

    argument_name = arguments.args[0].arg
    _validate_assertion_node(lambda_node.body, argument_name)

    def assertion(value: Any) -> bool:
        result = _evaluate_assertion_node(lambda_node.body, argument_name, value)
        if not isinstance(result, bool):
            raise ConfigurationError("Assertion lambda must evaluate to a Boolean")
        return result

    return assertion


def apply_constraint_suggestion(check: Any, expression: str) -> Any:
    """Apply one PyDeequ-generated ``.method(...)`` expression to ``check``.

    Only one public method call rooted at the supplied check object is allowed.
    Arguments may be literals, restricted assertion lambdas, or a public member
    of ``ConstrainableDataTypes``.
    """
    if not isinstance(expression, str) or not expression.strip():
        raise ConfigurationError("Constraint suggestion must be non-empty")

    try:
        parsed = ast.parse(f"_check{expression.strip()}", mode="eval")
    except SyntaxError as error:
        raise ConfigurationError(
            f"Invalid constraint suggestion syntax: {error.msg}"
        ) from error

    call = parsed.body
    if (
        not isinstance(call, ast.Call)
        or not isinstance(call.func, ast.Attribute)
        or not isinstance(call.func.value, ast.Name)
        or call.func.value.id != "_check"
    ):
        raise ConfigurationError(
            "Constraint suggestion must be one method call on the check object"
        )

    method_name = call.func.attr
    if method_name.startswith("_"):
        raise ConfigurationError("Constraint suggestion cannot call private methods")
    method = getattr(check, method_name, None)
    if not callable(method):
        raise ConfigurationError(f"Unknown PyDeequ constraint method: {method_name}")

    args = [_suggestion_value(node) for node in call.args]
    kwargs = {}
    for keyword in call.keywords:
        if keyword.arg is None:
            raise ConfigurationError("Expanded keyword arguments are not supported")
        kwargs[keyword.arg] = _suggestion_value(keyword.value)

    return method(*args, **kwargs)


def _validate_assertion_node(node: ast.AST, argument_name: str) -> None:
    if isinstance(node, ast.Constant):
        if isinstance(node.value, (bool, int, float)) or node.value is None:
            return
    elif isinstance(node, ast.Name):
        if node.id == argument_name:
            return
    elif isinstance(node, ast.UnaryOp) and type(node.op) in _UNARY_OPERATORS:
        _validate_assertion_node(node.operand, argument_name)
        return
    elif isinstance(node, ast.BinOp) and type(node.op) in _BINARY_OPERATORS:
        _validate_assertion_node(node.left, argument_name)
        _validate_assertion_node(node.right, argument_name)
        return
    elif isinstance(node, ast.BoolOp) and isinstance(node.op, (ast.And, ast.Or)):
        for value in node.values:
            _validate_assertion_node(value, argument_name)
        return
    elif isinstance(node, ast.Compare):
        _validate_assertion_node(node.left, argument_name)
        for operation, comparator in zip(node.ops, node.comparators):
            if type(operation) not in _COMPARISON_OPERATORS:
                break
            _validate_assertion_node(comparator, argument_name)
        else:
            return

    raise ConfigurationError(
        f"Unsupported assertion expression element: {type(node).__name__}"
    )


def _evaluate_assertion_node(node: ast.AST, argument_name: str, value: Any) -> Any:
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, ast.Name):
        return value
    if isinstance(node, ast.UnaryOp):
        return _UNARY_OPERATORS[type(node.op)](
            _evaluate_assertion_node(node.operand, argument_name, value)
        )
    if isinstance(node, ast.BinOp):
        return _BINARY_OPERATORS[type(node.op)](
            _evaluate_assertion_node(node.left, argument_name, value),
            _evaluate_assertion_node(node.right, argument_name, value),
        )
    if isinstance(node, ast.BoolOp):
        evaluated = (
            _evaluate_assertion_node(item, argument_name, value) for item in node.values
        )
        return all(evaluated) if isinstance(node.op, ast.And) else any(evaluated)
    if isinstance(node, ast.Compare):
        left = _evaluate_assertion_node(node.left, argument_name, value)
        for operation, comparator in zip(node.ops, node.comparators):
            right = _evaluate_assertion_node(comparator, argument_name, value)
            if not _COMPARISON_OPERATORS[type(operation)](left, right):
                return False
            left = right
        return True
    raise AssertionError(f"Validated node was not evaluable: {type(node).__name__}")


def _suggestion_value(node: ast.AST) -> Any:
    if isinstance(node, ast.Lambda):
        return parse_assertion(ast.unparse(node))
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, (ast.List, ast.Tuple, ast.Set, ast.Dict)):
        try:
            return ast.literal_eval(node)
        except (ValueError, TypeError) as error:
            raise ConfigurationError(
                "Constraint suggestion contains a non-literal value"
            ) from error
    if (
        isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "ConstrainableDataTypes"
        and not node.attr.startswith("_")
    ):
        datatype = getattr(ConstrainableDataTypes, node.attr, None)
        if datatype is not None:
            return datatype
        raise ConfigurationError(f"Unknown constrainable data type: {node.attr}")

    raise ConfigurationError(
        f"Unsupported constraint argument element: {type(node).__name__}"
    )
