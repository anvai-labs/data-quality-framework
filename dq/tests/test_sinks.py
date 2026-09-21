# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Outcome sink contracts: idempotency, eager validation, storage lane (U6)."""

import pytest
from pyhocon import ConfigFactory
import uuid

from pyspark.sql.types import StringType, StructField, StructType

from dq.engine.custom.custom_engine import CustomEngine
from dq.exceptions import ConfigurationError, RepositoryError
from dq.sinks import InMemorySink, RepositorySink, sink_from_config


class FakeRow:
    def __init__(self, values):
        self._values = values

    def asDict(self, recursive=True):
        return dict(self._values)


class FakeDataFrame:
    """Stands in for a DataFrame where a sink only needs collect()."""

    def __init__(self, rows):
        self._rows = [FakeRow(row) for row in rows]

    def collect(self):
        return self._rows


def test_in_memory_sink_records_bounded_artifacts():
    sink = InMemorySink()
    frame = FakeDataFrame([{"entity": "MultiColumn", "value": 1}])
    result = sink.save(frame, "metrics", 1234)
    assert result.metric_type == "metrics"
    assert result.key == 1234
    assert result.targets == ("memory:metrics",)
    assert sink.rows("metrics", 1234) == [{"entity": "MultiColumn", "value": 1}]


def test_in_memory_sink_rejects_duplicate_keys():
    sink = InMemorySink()
    sink.save(FakeDataFrame([{"value": 1}]), "metrics", 1234)
    with pytest.raises(RepositoryError, match="duplicate sink write"):
        sink.save(FakeDataFrame([{"value": 1}]), "metrics", 1234)
    sink.save(FakeDataFrame([{"value": 2}]), "metrics", 5678)
    sink.save(FakeDataFrame([{"value": 3}]), "verifications", 1234)


def test_repository_sink_validates_configuration_eagerly():
    with pytest.raises(ConfigurationError, match="dataset name is required"):
        RepositorySink({"format": "parquet", "file": {"paths": ["/tmp/x"]}})
    with pytest.raises(ConfigurationError, match="format"):
        RepositorySink(
            {"dataset": "bars", "format": "excel", "file": {"paths": ["/tmp/x"]}}
        )
    with pytest.raises(ConfigurationError, match="at least one"):
        RepositorySink({"dataset": "bars", "format": "parquet"})
    with pytest.raises(ConfigurationError, match="two-part"):
        RepositorySink(
            {
                "dataset": "bars",
                "format": "parquet",
                "catalog": {"tables": ["onlyonepart"]},
            }
        )
    with pytest.raises(ConfigurationError, match="two-part"):
        RepositorySink(
            {
                "dataset": "bars",
                "format": "parquet",
                "catalog": {"tables": ["a.b.c"]},
            }
        )
    with pytest.raises(ConfigurationError, match="repository table"):
        RepositorySink(
            {
                "dataset": "bars",
                "format": "parquet",
                "catalog": {"tables": ["bad table; drop"]},
            }
        )
    with pytest.raises(ConfigurationError, match="table name is not provided"):
        RepositorySink(
            {
                "dataset": "bars",
                "format": "parquet",
                "catalog": {"tables": [None]},
            }
        )


def test_sink_from_config_returns_none_for_empty_configuration():
    assert sink_from_config({}) is None
    assert sink_from_config(None) is None
    assert isinstance(
        sink_from_config({"dataset": "bars", "file": {"paths": ["/tmp/x"]}}),
        RepositorySink,
    )


def test_engine_hands_artifacts_to_the_injected_sink(spark):
    rows = [
        ("east", "a"),
        ("east", "b"),
        ("west", "a"),
    ]
    frame = spark.createDataFrame(rows, ["region", "id"])
    config = ConfigFactory.parse_string("""
        sync { checks = [ {
            constraint = "DistinctnessByGroup"
            columns = ["id"]
            group_by = ["region"]
            min = 2
            level = "Error"
        } ] }
        """).get("sync", {})
    sink = InMemorySink()
    results = CustomEngine(config).apply(frame, sink)
    assert len(results) == 1
    writes = sink.writes()
    assert set(kind for kind, _ in writes) == {"metrics", "verifications"}
    stored_metrics = writes[("metrics", list(writes)[0][1])]
    assert stored_metrics, "engine artifacts reach the sink as rows"


def test_deequ_engine_hands_artifacts_to_the_injected_sink(spark):
    from dq.engine.deequ.deequ_engine import DeequEngine

    config = ConfigFactory.parse_string("""
        {
            name = "deequ_sink_test"
            checks = [ {
                constraint_name = "id_complete"
                constraint = "isComplete"
                column = "id"
                level = "Error"
            } ]
        }
        """)
    frame = spark.createDataFrame([("a",), ("b",)], ["id"])
    sink = InMemorySink()
    results = DeequEngine(config).apply(frame, sink)
    assert results
    writes = sink.writes()
    assert set(kind for kind, _ in writes) == {"metrics", "verifications"}


def test_schemavalidation_engine_hands_artifacts_to_the_injected_sink(spark):
    from dq.engine.schemavalidation.schemavalidation_engine import (
        SchemavalidationEngine,
    )

    schema = StructType(
        [
            StructField("name", StringType(), False),
            StructField("signup_date", StringType(), False),
        ]
    )
    frame = spark.createDataFrame([("John", "20230101"), ("Joe", "20230303")], schema)
    frame.createOrReplaceTempView("temp_sink_table")
    config = ConfigFactory.parse_string("""
        {
            name = "schema_sink_test"
            engine = "schemavalidation"
            single_check_mode = true
            schema = { catalog_type = "spark", table = "temp_sink_table" }
        }
        """)
    sink = InMemorySink()
    results = SchemavalidationEngine(config).apply(frame, sink)
    assert results
    writes = sink.writes()
    assert set(kind for kind, _ in writes) == {"metrics", "verifications"}


def test_repository_sink_appends_files_keyed_by_write(spark, tmp_path):
    config = {
        "dataset": "bars",
        "format": "parquet",
        "file": {"paths": [str(tmp_path)]},
    }
    sink = RepositorySink(config)
    frame = spark.createDataFrame([("east", "a", 1)], ["region", "id", "value"])
    sink.save(frame, "metrics", 1700000000000)
    first = spark.read.parquet(str(tmp_path / "metrics"))
    assert first.count() == 1
    assert {"dqts", "dataset", "year"} <= set(first.columns)
    assert first.filter(first["dqts"] == 1700000000000).count() == 1

    sink.save(frame, "metrics", 1700000001000)
    assert (
        spark.read.parquet(str(tmp_path / "metrics")).count() == 2
    ), "append semantics: distinct keys accumulate"


def test_repository_sink_catalog_table_lane_creates_then_appends(spark, tmp_path):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS dw_u6 LOCATION '{tmp_path}/dw'")
    config = {
        "dataset": "bars",
        "format": "parquet",
        "catalog": {"tables": ["dw_u6.reposink"]},
    }
    sink = RepositorySink(config)
    frame = spark.createDataFrame([("e", 1)], ["instance", "value"])
    sink.save(frame, "metrics", 1700000000000)
    sink.save(frame, "metrics", 1700000001000)
    table = spark.read.table("dw_u6.reposink_metrics")
    assert table.count() == 2, "create on first write, append on the second"
    assert table.filter(table["dqts"] == 1700000001000).count() == 1


pytestmark = pytest.mark.spark


def test_framework_persists_through_the_sink_to_files(spark, tmp_path):
    from dq.dq_framework import DQFramework

    config = f"""
    dqframework {{
      dqrules = [ {{
        name = "sink_lane"
        engine = "custom"
        checks = [ {{ constraint = "DistinctnessByGroup", columns = ["id"], group_by = ["region"], min = 1, level = "Error" }} ]
      }} ]
      repository {{
        dataset = "bars"
        namespace = "team_a"
        format = "parquet"
        file {{ paths = ["{tmp_path}"] }}
      }}
    }}
    """
    frame = spark.createDataFrame([("east", "a"), ("west", "b")], ["region", "id"])
    results = DQFramework(spark, config, default_dataframe=frame).run()
    assert results, "the run produces validated outcomes"
    written = spark.read.parquet(str(tmp_path / "metrics"))
    assert written.count() == 1, "one bounded summary per column"
    assert {"dqts", "dataset", "tenant_namespace", "year"} <= set(written.columns)
    assert written.filter(written["dataset"] == "bars").count() == 1
    verifications = spark.read.parquet(str(tmp_path / "verifications"))
    assert verifications.count() >= 1
