# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

import pytest
from pyhocon import ConfigFactory
from dq.engine.schemavalidation.schemavalidation_engine import SchemavalidationEngine
import json
from pyspark.sql.types import StructType, StringType, LongType, StructField


def test_schemavalidation_with_nullvalue_and_disabled_notnull_success(spark):
    data = [("John", 25, "2021-01-01"), ("Doe", None, "2021-01-02"), ("Jane", 40, None)]
    data_schema = StructType(
        [
            StructField("name", StringType(), False),
            StructField("age", LongType(), True),
            StructField("signup_date", StringType(), True),
        ]
    )
    df = spark.createDataFrame(data=data, schema=data_schema)
    df.createOrReplaceTempView("temp_data_table")
    scemavalidation_config = ConfigFactory.parse_string("""
    {
        name = "schemavalidation with null values and not null enabled constraint for receving failure"
        engine = schemavalidation
        single_check_mode = True
        schema = {
            catalog_type = spark
            table = "temp_data_table"
        }
    }
    """)
    schema_validation_engine = SchemavalidationEngine(scemavalidation_config)
    summarymetrics = schema_validation_engine.apply(df, repository=None)
    overallsuccess = True
    for metric in summarymetrics:
        if not (metric["success"]):
            print("Error in : " + json.dumps(metric))
            overallsuccess = False

    assert True == overallsuccess


def test_schemavalidation_without_nullvalue_and_enabled_notnull_success(spark):
    data = [
        ("John", 25, "2021-01-01"),
        ("Doe", 30, "2021-01-02"),
        ("Jane", 40, "2024-05-05"),
    ]
    data_schema = StructType(
        [
            StructField("name", StringType(), False),
            StructField("age", LongType(), False),
            StructField("signup_date", StringType(), False),
        ]
    )
    df = spark.createDataFrame(data=data, schema=data_schema)
    df.createOrReplaceTempView("temp_data_table")
    scemavalidation_config = ConfigFactory.parse_string("""
    {
        name = "schemavalidation with null values and not null enabled constraint for receving failure"
        engine = schemavalidation
        single_check_mode = True
        schema = {
            catalog_type = spark
            table = "temp_data_table"
        }
    }
    """)
    schema_validation_engine = SchemavalidationEngine(scemavalidation_config)
    summarymetrics = schema_validation_engine.apply(df, repository=None)
    overallsuccess = True
    for metric in summarymetrics:
        if not (metric["success"]):
            print("Error in : " + json.dumps(metric))
            overallsuccess = False

    assert True == overallsuccess


pytestmark = pytest.mark.spark
