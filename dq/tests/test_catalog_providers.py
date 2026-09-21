# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Provider characterization with fake Spark sessions (TD-ARCH-2/U4).

Providers must resolve validated dotted names through ``spark.table`` and
build quoted, injection-free SQL statements. Every malformed identifier must
be rejected before the fake Spark session is touched.
"""

import pytest

from dq.catalog.glue_catalog import GlueCatalogProvider
from dq.catalog.spark_catalog import SparkCatalogProvider
from dq.catalog.unity_catalog import UnityCatalogProvider
from dq.exceptions import ConfigurationError


class FakeResult:
    """Stands in for a Spark DataFrame/Result: iterable via collect()."""

    def collect(self):
        return []


class FakeSpark:
    """Records table and SQL calls; returns sentinel objects."""

    def __init__(self):
        self.table_calls = []
        self.sql_calls = []

    def table(self, name):
        self.table_calls.append(name)
        return f"df({name})"

    def sql(self, statement):
        self.sql_calls.append(statement)
        return FakeResult()


def test_spark_provider_builds_and_loads_qualified_names():
    spark = FakeSpark()
    provider = SparkCatalogProvider(spark)
    assert provider.get_dataframe("orders") == "df(orders)"
    assert provider.get_dataframe("orders", database="sales") == "df(sales.orders)"
    assert spark.table_calls == ["orders", "sales.orders"]


def test_spark_provider_rejects_injected_names_before_spark():
    spark = FakeSpark()
    provider = SparkCatalogProvider(spark)
    with pytest.raises(ConfigurationError, match="orders"):
        provider.get_dataframe("orders; drop table sensitive")
    assert spark.table_calls == [], "no Spark call may run for a bad identifier"


def test_unity_provider_resolves_one_to_three_part_references():
    spark = FakeSpark()
    provider = UnityCatalogProvider(spark)
    provider.get_dataframe("orders", database="sales", catalog="main")
    provider.get_dataframe("sales.orders", catalog="main")
    provider.get_dataframe("main.sales.orders")
    assert spark.table_calls == [
        "main.sales.orders",
        "main.sales.orders",
        "main.sales.orders",
    ]


def test_unity_provider_sql_statements_use_quoted_names():
    spark = FakeSpark()
    provider = UnityCatalogProvider(spark)
    assert provider.table_exists("main.sales.orders") is True
    provider.list_schemas(catalog_name="main")
    provider.list_tables("sales", catalog_name="main")
    provider.set_current_catalog("main")
    assert spark.sql_calls == [
        "DESCRIBE TABLE `main`.`sales`.`orders`",
        "SHOW SCHEMAS IN `main`",
        "SHOW TABLES IN `main`.`sales`",
        "USE CATALOG `main`",
    ]


def test_unity_provider_rejects_injected_names_before_spark():
    spark = FakeSpark()
    provider = UnityCatalogProvider(spark)
    with pytest.raises(ConfigurationError):
        provider.get_dataframe("main.sales.orders; drop table sensitive")
    with pytest.raises(ConfigurationError):
        provider.set_current_catalog("main; use catalog evil")
    with pytest.raises(ConfigurationError):
        provider.list_tables("sales; drop", catalog_name="main")
    assert spark.sql_calls == [], "no Spark SQL may run for a bad identifier"


def test_glue_provider_rejects_names_with_too_many_parts():
    spark = FakeSpark()
    provider = GlueCatalogProvider(spark)
    with pytest.raises(ConfigurationError):
        provider.get_dataframe("a.b.orders")
    assert spark.table_calls == []
