# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

import pytest
from unittest.mock import patch, MagicMock
from pyhocon import ConfigFactory
from pyspark.sql.functions import to_date, to_timestamp, round
from dq.engine.schemavalidation.schemavalidation_engine import SchemavalidationEngine
import json
from pyspark.sql.types import (
    StructType,
    StringType,
    VarcharType,
    CharType,
    IntegerType,
    LongType,
    FloatType,
    DoubleType,
    DecimalType,
    BooleanType,
    NullType,
    ByteType,
    StructField,
    DateType,
    TimestampType,
    TimestampNTZType,
)


@pytest.fixture
def df_long_incoming(spark):
    # Example DataFrame with incoming data
    incoming_data = [("John", 25), ("Doe", 30), ("Jane", 40)]
    incoming_data_schema = StructType(
        [
            StructField("name", StringType(), False),
            StructField("age", LongType(), True),
        ]
    )
    df = spark.createDataFrame(data=incoming_data, schema=incoming_data_schema)
    return df


@pytest.fixture
def list_check_true_single_check_mode_schemavalidation_config():
    config_str = """
    {
        name = list_check_true_single_check_mode_schemavalidation
        engine = schemavalidation
        single_check_mode = False
        schema = {
            table = temp_data_table
            catalog_type = spark
            foreign_key_constraints=[
                {
                    src_column      = age
                    ref_table       = temp_users
                    ref_column      = age
                    use_list_check  = True
                }
            ]
        }
    }
    """
    return ConfigFactory.parse_string(config_str)


@pytest.fixture
def list_check_false_single_check_mode_schemavalidation_config():
    config_str = """
    {
        name = list_check_false_single_check_mode_schemavalidation
        engine = schemavalidation
        single_check_mode = False
        schema = {
            table = temp_data_table
            catalog_type = spark
            foreign_key_constraints=[
                {
                    src_column      = age
                    ref_table       = temp_users
                    ref_column      = age
                    use_list_check  = False
                }
            ]
        }
    }
    """
    return ConfigFactory.parse_string(config_str)


@pytest.fixture
def list_check_true_multiple_check_mode_schemavalidation_config():
    config_str = """
    {
        name = list_check_true_multiple_check_mode_schemavalidation
        engine = schemavalidation
        single_check_mode = False
        schema = {
            table = temp_data_table
            catalog_type = spark
            foreign_key_constraints=[
                {
                    src_column      = age
                    ref_table       = temp_users
                    ref_column      = age
                    use_list_check  = True
                }
            ]
        }
    }
    """
    return ConfigFactory.parse_string(config_str)


@pytest.fixture
def list_check_false_multiple_check_mode_schemavalidation_config():
    config_str = """
    {
        name = list_check_false_multiple_check_mode_schemavalidation
        engine = schemavalidation
        single_check_mode = False
        schema = {
            table = temp_data_table
            catalog_type = spark
            foreign_key_constraints=[
                {
                    src_column      = age
                    ref_table       = temp_users
                    ref_column      = age
                    use_list_check  = False
                }
            ]
        }
    }
    """
    return ConfigFactory.parse_string(config_str)


def process_schemavalidation_success(spark, df, config):
    # Initialize Schema Validation Engine with single_check_mode = False for granular reporting
    schema_validation_engine = SchemavalidationEngine(config)
    df.createOrReplaceTempView("temp_data_table")
    dfusers = spark.sql("select name, age from temp_data_table")
    dfusers.createOrReplaceTempView("temp_users")

    # Apply Schema Validation including multi-column unique and foreign key constraints
    summarymetrics = schema_validation_engine.apply(df, repository=None)
    overallsuccess = True
    for metric in summarymetrics:
        print(json.dumps(metric, indent=2))
        assert metric["success"] == True, f"{metric} failed."
        if not (metric["success"]):
            print("Error in : " + json.dumps(metric))
            overallsuccess = False
    return overallsuccess


def process_schemavalidation_failure(spark, df, config):
    # Initialize Schema Validation Engine with single_check_mode = False for granular reporting
    schema_validation_engine = SchemavalidationEngine(config)
    df.createOrReplaceTempView("temp_data_table")

    dfusers = spark.sql("select name, age from temp_data_table limit 2")
    dfusers.createOrReplaceTempView("temp_users")
    #
    # Apply Schema Validation including multi-column unique and foreign key constraints
    summarymetrics = schema_validation_engine.apply(df, repository=None)
    overallsuccess = True
    for metric in summarymetrics:
        if not (metric["success"]):
            print("Error in : " + json.dumps(metric))
            overallsuccess = False

    return overallsuccess


def test_schemavalidation_fk_use_list_check_true_single_check_mode_success(
    spark, df_long_incoming, list_check_true_single_check_mode_schemavalidation_config
):
    assert True == process_schemavalidation_success(
        spark,
        df_long_incoming,
        list_check_true_single_check_mode_schemavalidation_config,
    ), "Atleast one metric should have failed."


def test_schemavalidation_fk_use_list_check_false_single_check_mode_success(
    spark, df_long_incoming, list_check_false_single_check_mode_schemavalidation_config
):
    assert True == process_schemavalidation_success(
        spark,
        df_long_incoming,
        list_check_false_single_check_mode_schemavalidation_config,
    ), "Atleast one metric should have failed."


def test_schemavalidation_fk_use_list_check_true_single_check_mode_failure(
    spark, df_long_incoming, list_check_true_single_check_mode_schemavalidation_config
):
    assert False == process_schemavalidation_failure(
        spark,
        df_long_incoming,
        list_check_true_single_check_mode_schemavalidation_config,
    ), "Atleast one metric should have failed."


def test_schemavalidation_fk_use_list_check_false_single_check_mode_failure(
    spark, df_long_incoming, list_check_false_single_check_mode_schemavalidation_config
):
    assert False == process_schemavalidation_failure(
        spark,
        df_long_incoming,
        list_check_false_single_check_mode_schemavalidation_config,
    ), "Atleast one metric should have failed."


def test_schemavalidation_fk_use_list_check_true_multiple_check_mode_success(
    spark, df_long_incoming, list_check_true_multiple_check_mode_schemavalidation_config
):
    assert True == process_schemavalidation_success(
        spark,
        df_long_incoming,
        list_check_true_multiple_check_mode_schemavalidation_config,
    ), "Atleast one metric should have failed."


def test_schemavalidation_fk_use_list_check_false_multiple_check_mode_success(
    spark,
    df_long_incoming,
    list_check_false_multiple_check_mode_schemavalidation_config,
):
    assert True == process_schemavalidation_success(
        spark,
        df_long_incoming,
        list_check_false_multiple_check_mode_schemavalidation_config,
    ), "Atleast one metric should have failed."


def test_schemavalidation_fk_use_list_check_true_multiple_check_mode_failure(
    spark, df_long_incoming, list_check_true_multiple_check_mode_schemavalidation_config
):
    assert False == process_schemavalidation_failure(
        spark,
        df_long_incoming,
        list_check_true_multiple_check_mode_schemavalidation_config,
    ), "Atleast one metric should have failed."


def test_schemavalidation_fk_use_list_check_false_multiple_check_mode_failure(
    spark,
    df_long_incoming,
    list_check_false_multiple_check_mode_schemavalidation_config,
):
    assert False == process_schemavalidation_failure(
        spark,
        df_long_incoming,
        list_check_false_multiple_check_mode_schemavalidation_config,
    ), "Atleast one metric should have failed."


pytestmark = pytest.mark.spark
