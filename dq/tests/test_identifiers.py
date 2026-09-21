# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Pure contracts for the shared identifier value objects (TD-ARCH-2/U4)."""

import pytest

from dq.exceptions import ConfigurationError
from dq.identifiers import ColumnName, IdentifierError, TableName, qualified_table_name


@pytest.mark.parametrize(
    "value", ["orders", "sales.orders", "main.sales.orders", "_orders"]
)
def test_accepts_one_to_three_part_identifiers(value):
    table = TableName.parse(value)
    assert str(table) == value


@pytest.mark.parametrize(
    "value",
    [
        "",
        ".",
        "a..b",
        ".orders",
        "orders.",
        "1orders",
        "orders-2024",
        'orders"; drop table x',
        "orders; drop table x",
        "orders `union`",
        "a.b.c.d",
    ],
)
def test_rejects_malformed_or_injected_table_names(value):
    with pytest.raises(ConfigurationError):
        TableName.parse(value)


def test_rejects_non_string_and_enforces_part_and_length_bounds():
    with pytest.raises(ConfigurationError, match="must be a string"):
        TableName.parse(None)
    with pytest.raises(ConfigurationError, match="one to 2"):
        TableName.parse("a.b.c", max_parts=2)
    with pytest.raises(ConfigurationError, match="128 characters"):
        TableName.parse("a" * 129)


def test_labels_identify_the_offending_boundary():
    with pytest.raises(ConfigurationError, match="reference table name"):
        TableName.parse("bad name", label="reference table name")
    with pytest.raises(ConfigurationError, match="ref_columns"):
        ColumnName.parse("bad name", label="ref_columns")


def test_quoted_rendering_uses_backticks_per_part():
    table = TableName.parse("main.sales.orders")
    assert table.quoted == "`main`.`sales`.`orders`"
    column = ColumnName.parse("order_id")
    assert column.quoted == "`order_id`"
    assert str(column) == "order_id"


def test_identifier_errors_are_configuration_errors():
    with pytest.raises(ConfigurationError):
        TableName.parse("bad name")


def test_qualified_table_name_prepends_components():
    assert (
        qualified_table_name("orders", database="sales", catalog="main")
        == "main.sales.orders"
    )
    assert qualified_table_name("sales.orders", catalog="main") == "main.sales.orders"
    assert qualified_table_name("main.sales.orders") == "main.sales.orders"
    assert (
        qualified_table_name("orders", database="sales", max_parts=2) == "sales.orders"
    )
    with pytest.raises(ConfigurationError):
        qualified_table_name("orders", database="sales; drop", catalog="main")
    with pytest.raises(ConfigurationError):
        qualified_table_name("a.b.c", database="sales", max_parts=2)
