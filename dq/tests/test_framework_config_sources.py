# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Config-source resolution contracts: every URI branch loads or fails closed."""

import pytest
from pyhocon import ConfigFactory

from dq.dq_framework import DQFramework
from dq.exceptions import ConfigurationError, DataFrameNotFoundError

HOCON = "dqframework { dqrules = [] }"


def test_inline_config_loads(spark):
    framework = DQFramework(spark, HOCON)
    assert framework.dataframes == {}


def test_file_source_loads(spark, tmp_path):
    config_file = tmp_path / "dq.conf"
    config_file.write_text(HOCON)
    framework = DQFramework(spark, f"file://{config_file}")
    assert framework.dataframes == {}


def test_http_source_loads_through_the_uri_helper(monkeypatch, spark):
    import dq.utils.config_utils as config_utils

    monkeypatch.setattr(config_utils, "load_from_uri", lambda *args, **kwargs: HOCON)
    framework = DQFramework(spark, "https://example.invalid/dq.conf")
    assert framework.dataframes == {}


def test_s3_source_loads_through_the_uri_helper(monkeypatch, spark):
    import dq.utils.config_utils as config_utils

    seen = {}

    def fake_load_from_s3(bucket, key):
        seen["bucket"] = bucket
        seen["key"] = key
        return HOCON

    monkeypatch.setattr(config_utils, "load_from_s3", fake_load_from_s3)
    framework = DQFramework(spark, "s3://dq-bucket/rules.conf")
    assert seen == {"bucket": "dq-bucket", "key": "/rules.conf"}


def test_abfss_source_loads_through_the_uri_helper(monkeypatch, spark):
    import dq.utils.config_utils as config_utils

    monkeypatch.setattr(config_utils, "load_from_adls", lambda *args, **kwargs: HOCON)
    framework = DQFramework(
        spark, "abfss://container@account.dfs.core.windows.net/dq.conf"
    )


def test_unsupported_scheme_fails_closed(spark):
    with pytest.raises(ConfigurationError, match="Unsupported config scheme"):
        DQFramework(spark, "ftp://example.invalid/dq.conf")


def test_structured_dataframe_references_resolve_through_the_catalog(spark):
    frame = spark.createDataFrame([("a", 1)], ["id", "n"])
    frame.createOrReplaceTempView("structured_view")
    config = """
    dqframework {
      dataframes {
        viewed { table = "structured_view" }
      }
      dqrules = [ {
        name = "resolution"
        engine = "custom"
        checks = [ { constraint = "DistinctnessByGroup", columns = ["id"], group_by = ["id"], min = 1, level = "Error" } ]
      } ]
    }
    """
    framework = DQFramework(spark, config, default_dataframe=frame)
    results = framework.run()
    assert results, "the structured reference resolves to the temp view"


def test_rule_referencing_a_missing_table_fails_closed(spark):
    config = """
    dqframework {
      dqrules = [ {
        name = "missing"
        engine = "custom"
        dataframes = ["does_not_exist"]
        checks = [ { constraint = "DistinctnessByGroup", columns = ["id"], group_by = ["region"], min = 1, level = "Error" } ]
      } ]
    }
    """
    frame = spark.createDataFrame([("a", 1)], ["id", "n"])
    framework = DQFramework(spark, config, default_dataframe=frame)
    with pytest.raises(DataFrameNotFoundError, match="does_not_exist"):
        framework.run()
