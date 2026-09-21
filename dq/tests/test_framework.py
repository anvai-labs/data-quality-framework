# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

from dq.dq_framework import DQFramework
import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from pyhocon import ConfigFactory


@pytest.fixture
def sample_dataframe_with_success(spark):
    data1 = [
        (1, "Alice", 34, "alice@example.com"),
        (2, "Bob", 45, "bob@example.com"),
        (3, "Charlie", 29, "charlie@example.com"),
        (4, "Delta", 18, "delta@example.org"),
    ]
    columns = ["id", "name", "age", "email"]
    return spark.createDataFrame(data1, columns)


@pytest.fixture
def sample_dataframe_with_failure(spark):
    data2 = [
        (1, "Alice", 34, "alice"),
        (2, "Bob", 45, "bob.lastname"),
        (3, None, 29, "charlie@example.com"),
        (4, "Delta", 18, "delta/78@notvalid"),
    ]
    columns = ["id", "name", "age", "email"]
    return spark.createDataFrame(data2, columns)


@pytest.fixture
def framework_config():
    config_file = "file://./dq/tests/resources/example.conf"
    return config_file


def test_framework_success(spark, sample_dataframe_with_success, framework_config):
    dqf = DQFramework(spark, framework_config, sample_dataframe_with_success)
    cum_metris = dqf.run()
    assert all(isinstance(metric["details"], dict) for metric in cum_metris)
    overallsuccess = True
    for metric in cum_metris:
        assert "success" in metric, "Error: no success key in metric"
        if not (metric["success"]):
            overallsuccess = False
    assert overallsuccess == True, "Error: Some checks have failed"


def test_framework_failure(spark, sample_dataframe_with_failure, framework_config):
    dqf = DQFramework(spark, framework_config, sample_dataframe_with_failure)
    cum_metris = dqf.run()
    overallsuccess = True
    for metric in cum_metris:
        assert "success" in metric, "Error: no success key in metric"
        if not (metric["success"]):
            overallsuccess = False
    assert overallsuccess == False, "Error: All checks should not have passed"


pytestmark = pytest.mark.spark
