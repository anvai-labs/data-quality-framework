# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from pyhocon import ConfigFactory
from dq.engine.deequ.deequ_engine import DeequEngine
from dq.engine.deequ.deequ_check import DeequCheck
import json


@pytest.fixture
def sample_success_dataframe(spark):
    data = [
        ("Alice", 34, "alice@example.com"),
        ("Bob", 45, "bob@example.com"),
        ("Catherine", 4, "cathy@example.com"),
    ]
    columns = ["name", "age", "email"]
    return spark.createDataFrame(data, columns)


@pytest.fixture
def sample_failure_dataframe(spark):
    data = [
        ("Alice", -2, "alice@example.com"),
        ("Bob", 45, "bob@example.com"),
        ("Catherine", None, "not-a-valid-email"),
    ]
    columns = ["name", "age", "email"]
    return spark.createDataFrame(data, columns)


@pytest.fixture
def deequ_single_check_config():
    config_str = """
    deequ {
        name = "myrule1"
        engine = "deequ"
        single_check_mode = True
        checks = [
            {
                constraint = "containsEmail"
                column = "email"
                assertion = "lambda i : i == 1"
            },
            {
                constraint = "isPositive"
                column = "age"
            },
            {
                alias = "check-datatype"
                constraint = "hasDataType"
                column = "age"
                description = "Check if age is integer type"
                hint = "Check failed as age may not be integer type"
                datatype = Integral
                assertion = "lambda i : i == 1"
            },
	        {
		        constraint = "hasCompleteness",
                column = "name",
                assertion = "lambda i : i == 1"
                hint = "Check failed as name may not be complete"
                description = "Check if name is complete"

            }
        ]
    }

    """
    return ConfigFactory.parse_string(config_str).get("deequ", {})


@pytest.fixture
def deequ_multi_check_config():
    config_str = """
    deequ {
        name = "myrule1"
        engine = "deequ"
        single_check_mode = False
        checks = [
            {
                constraint = "containsEmail"
                column = "email"
                assertion = "lambda i : i == 1"
            },
            {
                constraint = "isPositive"
                column = "age"
            },
            {
                alias = "check-datatype"
                constraint = "hasDataType"
                column = "age"
                description = "Check if age is integer type"
                hint = "Check failed as age may not be integer type"
                datatype = Integral
                assertion = "lambda i : i == 1"
            },
	        {
		        constraint = "hasCompleteness",
                column = "name",
                assertion = "lambda i : i == 1"
                hint = "Check failed as name may not be complete"
                description = "Check if name is complete"

            }
        ]
    }

    """
    return ConfigFactory.parse_string(config_str).get("deequ", {})


def test_single_check_deequ_engine_success(
    sample_success_dataframe, deequ_single_check_config
):
    engine = DeequEngine(deequ_single_check_config)
    summarymetrics = engine.apply(sample_success_dataframe, repository=None)
    overallsuccess = True
    for metric in summarymetrics:
        assert metric["success"] == True, f"{metric} failed."
        if not (metric["success"]):
            print("Error in : " + json.dumps(metric))
            overallsuccess = False

    assert True == overallsuccess, "Atleast one metric failed."


def test_multi_check_deequ_engine_success(
    sample_success_dataframe, deequ_multi_check_config
):
    engine = DeequEngine(deequ_multi_check_config)
    summarymetrics = engine.apply(sample_success_dataframe, repository=None)
    overallsuccess = True
    for metric in summarymetrics:
        assert metric["success"] == True, f"{metric} failed."
        if not (metric["success"]):
            print("Error in : " + json.dumps(metric))
            overallsuccess = False

    assert True == overallsuccess, "Atleast one metric failed."


def test_single_check_deequ_engine_failure(
    sample_failure_dataframe, deequ_single_check_config
):
    engine = DeequEngine(deequ_single_check_config)
    summarymetrics = engine.apply(sample_failure_dataframe, repository=None)
    overallsuccess = True
    for metric in summarymetrics:
        if not (metric["success"]):
            overallsuccess = False

    assert False == overallsuccess, "Atleast one metric should have failed."


def test_multi_check_deequ_engine_failure(
    sample_failure_dataframe, deequ_multi_check_config
):
    engine = DeequEngine(deequ_multi_check_config)
    summarymetrics = engine.apply(sample_failure_dataframe, repository=None)
    overallsuccess = True
    for metric in summarymetrics:
        if not (metric["success"]):
            overallsuccess = False

    assert False == overallsuccess, "Atleast one metric should have failed."


pytestmark = pytest.mark.spark
