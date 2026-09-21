# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

import os

os.environ["SPARK_VERSION"] = "3.5"
import pytest
from pyhocon import ConfigFactory

from dq.engine.custom.custom_engine import CustomEngine
from dq.tests.helpers import assert_overall_success
import pydeequ

from pyspark.sql import SparkSession


def test_distinct_groupby_constraint(spark, custom_config, sample_dataframe_group):
    custom_engine = CustomEngine(custom_config)
    results = custom_engine.apply(sample_dataframe_group, None)
    assert all(isinstance(metric["details"], dict) for metric in results)
    assert len(results) == 2


def test_rate_of_change(
    spark, custom_config_rate_of_change, multi_column_dataframe_rate_of_change
):
    custom_engine = CustomEngine(custom_config_rate_of_change)
    results = custom_engine.apply(multi_column_dataframe_rate_of_change, None)
    rate_of_change = [
        result
        for result in results
        if result["details"]["instance"].startswith("RateOfChange")
    ]
    assert len(rate_of_change) == 11, "one bounded summary per column"
    failing = [result for result in rate_of_change if not result["success"]]
    assert failing, "the 40% recovery spike must fail the max threshold"
    stale = [
        result
        for result in results
        if result["details"]["instance"].startswith("DistinctnessByGroup")
    ]
    assert len(stale) == 10, "one bounded summary per column for the second check"


def test_wide_col_negative_values(
    spark, custom_config_wide_col_negative_values, numeric_dataframe
):
    custom_engine = CustomEngine(custom_config_wide_col_negative_values)
    results = custom_engine.apply(numeric_dataframe, None)
    assert_overall_success(results, "wide-table negative values")


pytestmark = pytest.mark.spark
