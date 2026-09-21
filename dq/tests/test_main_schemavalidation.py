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
    incoming_data = [
        ("John", 25, "2021-01-01"),
        ("Doe", 30, "2021-01-02"),
        ("Jane", 40, "2021-01-03"),
    ]
    incoming_data_schema = StructType(
        [
            StructField("name", StringType(), False),
            StructField("age", LongType(), False),
            StructField("signup_date", StringType(), False),
        ]
    )
    df = spark.createDataFrame(data=incoming_data, schema=incoming_data_schema)
    return df


@pytest.fixture
def df_incoming(spark):
    # Example DataFrame with incoming data
    incoming_data = [
        ("John", 25, "2021-01-01"),
        ("Doe", 30, "2021-01-02"),
        ("Jane", 40, "2021-01-03"),
    ]
    df = spark.createDataFrame(incoming_data, ["name", "age", "signup_date"])
    return df


@pytest.fixture
def df_integer_incoming(spark):
    # Example DataFrame with incoming data
    incoming_data = [
        ("John", 25, "2021-01-01"),
        ("Doe", 30, "2021-01-02"),
        ("Jane", 40, "2021-01-03"),
    ]
    incoming_data_schema = StructType(
        [
            StructField("name", StringType(), False),
            StructField("age", IntegerType(), False),
            StructField("signup_date", StringType(), False),
        ]
    )
    df = spark.createDataFrame(data=incoming_data, schema=incoming_data_schema)
    return df


@pytest.fixture
def df_float_incoming(spark):
    # Example DataFrame with incoming data
    incoming_data = [
        ("John", 25.1, "2021-01-01"),
        ("Doe", 30.9, "2021-01-02"),
        ("Jane", 40.03, "2021-01-03"),
    ]
    incoming_data_schema = StructType(
        [
            StructField("name", StringType(), False),
            StructField("age", FloatType(), False),
            StructField("signup_date", StringType(), False),
        ]
    )
    df = spark.createDataFrame(data=incoming_data, schema=incoming_data_schema)
    df = df.withColumn("age", round("age", 2))

    return df


@pytest.fixture
def df_double_incoming(spark):
    # Example DataFrame with incoming data
    incoming_data = [
        ("John", 25.1, "2021-01-01"),
        ("Doe", 30.9, "2021-01-02"),
        ("Jane", 40.03, "2021-01-03"),
    ]
    incoming_data_schema = StructType(
        [
            StructField("name", StringType(), False),
            StructField("age", DoubleType(), False),
            StructField("signup_date", StringType(), False),
        ]
    )
    df = spark.createDataFrame(data=incoming_data, schema=incoming_data_schema)
    df = df.withColumn("age", round("age", 2))
    return df


@pytest.fixture
def df_decimal_incoming(spark):
    # Example DataFrame with incoming data
    incoming_data = [
        ("John", 25.1, "2021-01-01"),
        ("Doe", 30.9, "2021-01-02"),
        ("Jane", 40.03, "2021-01-03"),
    ]
    incoming_data_schema = StructType(
        [
            StructField("name", StringType(), False),
            StructField("age", StringType(), False),
            StructField("signup_date", StringType(), False),
        ]
    )
    df = spark.createDataFrame(data=incoming_data, schema=incoming_data_schema)
    df = df.withColumn("age", df["age"].cast(DecimalType(5, 2)))

    return df


@pytest.fixture
def df_date_incoming(spark):
    # Example DataFrame with incoming data
    incoming_data = [
        ("John", 25.1, "2021-01-01"),
        ("Doe", 30.9, "2021-01-02"),
        ("Jane", 40.03, "2021-01-03"),
    ]
    incoming_data_schema = StructType(
        [
            StructField("name", StringType(), False),
            StructField("age", DoubleType(), False),
            StructField("signup_date", StringType(), False),
        ]
    )
    df = spark.createDataFrame(data=incoming_data, schema=incoming_data_schema)
    df = df.withColumn("signup_date", to_date("signup_date", "yyyy-MM-dd"))
    return df


@pytest.fixture
def df_timestamp_incoming(spark):
    # Example DataFrame with incoming data
    incoming_data = [
        ("John", 25.1, "2021-01-01"),
        ("Doe", 30.9, "2021-01-02"),
        ("Jane", 40.03, "2021-01-03"),
    ]
    incoming_data_schema = StructType(
        [
            StructField("name", StringType(), False),
            StructField("age", DoubleType(), False),
            StructField("signup_date", StringType(), False),
        ]
    )
    df = spark.createDataFrame(data=incoming_data, schema=incoming_data_schema)
    df = df.withColumn("signup_date", to_timestamp("signup_date", "yyyy-MM-dd"))
    return df


@pytest.fixture
def single_schemavalidation_config():
    config_str = """{
        name = schema-validation-single_check_mode
        engine = schemavalidation
        single_check_mode = True
        schema {
            table   =   temp_data_table
            catalog_type = spark
            unique_constraints=[[name, signup_date], [age]]
            foreign_key_constraints=[

                {
                    src_column = name
                    ref_table = temp_users
                    ref_column = name
                    use_list_check = True  # Explicit override (will be ignored if row count > threshold)
                },
                {
                    src_column = signup_date
                    ref_table = temp_signup_dates
                    ref_column = signup_date
                },
                {
                    src_column = age
                    ref_table = temp_users
                    ref_column = age
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
    dfsignups = spark.sql("SELECT signup_date from temp_data_table")
    dfsignups.createOrReplaceTempView("temp_signup_dates")
    # Apply Schema Validation including multi-column unique and foreign key constraints
    summarymetrics = schema_validation_engine.apply(df, repository=None)
    overallsuccess = True
    for metric in summarymetrics:
        print(json.dumps(metric, indent=2))
        # assert metric['success'] == True , f"{metric} failed."
        if not (metric["success"]):
            print("Error in : " + json.dumps(metric))
            overallsuccess = False
    if not overallsuccess:
        print("dataframe\n")
        df.show()
        print("schema\n")
        df.printSchema()
        print("summarymetric\n")
        print(json.dumps(summarymetrics, indent=2))
    return overallsuccess


def process_schemavalidation_failure(spark, df, config):
    # Initialize Schema Validation Engine with single_check_mode = False for granular reporting
    schema_validation_engine = SchemavalidationEngine(config)
    df.createOrReplaceTempView("temp_data_table")

    dfusers = spark.sql("select name, age from temp_data_table limit 2")
    dfusers.createOrReplaceTempView("temp_users")
    dfsignups = spark.sql("SELECT signup_date from temp_data_table limit 1")
    dfsignups.createOrReplaceTempView("temp_signup_dates")
    # Apply Schema Validation including multi-column unique and foreign key constraints
    summarymetrics = schema_validation_engine.apply(df, repository=None)
    overallsuccess = True
    for metric in summarymetrics:
        if not (metric["success"]):
            print("Error in : " + json.dumps(metric))
            overallsuccess = False

    return overallsuccess


def test_schemavalidation_single_check_mode_success(
    spark, df_incoming, single_schemavalidation_config
):
    assert True == process_schemavalidation_success(
        spark, df_incoming, single_schemavalidation_config
    ), "Atleast one metric failed."


def test_schemavalidation_single_check_mode_failure(
    spark, df_incoming, single_schemavalidation_config
):
    assert False == process_schemavalidation_failure(
        spark, df_incoming, single_schemavalidation_config
    ), "Atleast one metric should have failed."


def test_schemavalidation_integer_type_success(
    spark, df_integer_incoming, single_schemavalidation_config
):
    assert True == process_schemavalidation_success(
        spark, df_integer_incoming, single_schemavalidation_config
    ), "Atleast one metric failed."


def test_schemavalidation_integer_type_failure(
    spark, df_integer_incoming, single_schemavalidation_config
):
    assert False == process_schemavalidation_failure(
        spark, df_integer_incoming, single_schemavalidation_config
    ), "Atleast one metric should have failed."


def test_schemavalidation_float_type_success(spark):
    data = [
        ("John", 25.002, "2021-01-01"),
        ("Doe", 30.93, "2021-01-02"),
        ("Jane", 40.03, "2023-03-03"),
    ]
    data_schema = StructType(
        [
            StructField("name", StringType(), False),
            StructField("age", FloatType(), False),
            StructField("signup_date", StringType(), False),
        ]
    )
    df = spark.createDataFrame(data=data, schema=data_schema)
    df.createOrReplaceTempView("temp_data_table")
    scemavalidation_config = ConfigFactory.parse_string("""
    {
        name = "schemavalidation with null values and not null enabled constraint for receving failure"
        engine = schemavalidation
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

    assert True == overallsuccess, "Atleast one metric failed."


def test_schemavalidation_float_type_failure(
    spark, df_float_incoming, single_schemavalidation_config
):
    assert False == process_schemavalidation_failure(
        spark, df_float_incoming, single_schemavalidation_config
    ), "Atleast one metric should have failed."


def test_schemavalidation_double_type_success(
    spark, df_double_incoming, single_schemavalidation_config
):
    assert True == process_schemavalidation_success(
        spark, df_double_incoming, single_schemavalidation_config
    ), "Atleast one metric failed."


def test_schemavalidation_double_type_failure(
    spark, df_double_incoming, single_schemavalidation_config
):
    assert False == process_schemavalidation_failure(
        spark, df_double_incoming, single_schemavalidation_config
    ), "Atleast one metric should have failed."


def test_schemavalidation_decimal_type_success(
    spark, df_decimal_incoming, single_schemavalidation_config
):
    assert True == process_schemavalidation_success(
        spark, df_decimal_incoming, single_schemavalidation_config
    ), "Atleast one metric failed."


def test_schemavalidation_decimal_type_failure(
    spark, df_decimal_incoming, single_schemavalidation_config
):
    assert False == process_schemavalidation_failure(
        spark, df_decimal_incoming, single_schemavalidation_config
    ), "Atleast one metric should have failed."


def test_schemavalidation_date_type_success(
    spark, df_date_incoming, single_schemavalidation_config
):
    assert True == process_schemavalidation_success(
        spark, df_date_incoming, single_schemavalidation_config
    ), "Atleast one metric failed."


def test_schemavalidation_date_type_failure(
    spark, df_date_incoming, single_schemavalidation_config
):
    assert False == process_schemavalidation_failure(
        spark, df_date_incoming, single_schemavalidation_config
    ), "Atleast one metric should have failed."


def test_schemavalidation_timestamp_type_success(
    spark, df_timestamp_incoming, single_schemavalidation_config
):
    assert True == process_schemavalidation_success(
        spark, df_timestamp_incoming, single_schemavalidation_config
    ), "Atleast one metric failed."


def test_schemavalidation_timestamp_type_failure(
    spark, df_timestamp_incoming, single_schemavalidation_config
):
    assert False == process_schemavalidation_failure(
        spark, df_timestamp_incoming, single_schemavalidation_config
    ), "Atleast one metric should have failed."


def test_nullable_false_schemavalidation_single_check_success(spark):
    # Example DataFrame with incoming data
    incomingnullable_data = [
        ("John", 25.1, "2021-01-01"),
        ("Doe", 30.1, "2021-01-02"),
        ("Jane", 40.1, "2021-01-03"),
    ]
    colnames = ["name", "age", "signup_date"]
    tablename = "temp_null_false_data_table_success"
    df = spark.createDataFrame(incomingnullable_data, colnames)
    df.createOrReplaceTempView(tablename)
    not_null_constraint_config = ConfigFactory.parse_string(f"""
    {{
        name = "schemavalidation with not null values and not null enabled constraint for success"
        engine = schemavalidation
        schema = {{
            catalog_type = spark
            table = "{tablename}"
            not_null_columns = {json.dumps(colnames, indent=2)}
        }}
    }}
    """)
    schema_validation_engine = SchemavalidationEngine(not_null_constraint_config)
    summarymetrics = schema_validation_engine.apply(df, repository=None)
    overallsuccess = True
    for metric in summarymetrics:
        if not (metric["success"]):
            print("Error in : " + json.dumps(metric))
            overallsuccess = False

    assert True == overallsuccess, f"atleast one metric failed"


def test_nullable_false_schemavalidation_single_check_failure(spark):
    # Example DataFrame with incoming data
    incomingnullable_data = [
        ("John", None, "2021-01-01"),
        (None, 30.1, "2021-01-02"),
        ("Jane", 40.1, None),
    ]
    colnames = ["name", "age", "signup_date"]
    tablename = "temp_null_false_data_table_failure"
    df = spark.createDataFrame(incomingnullable_data, colnames)
    df.createOrReplaceTempView(tablename)
    not_null_constraint_config = ConfigFactory.parse_string(f"""
    {{
        name = "schemavalidation with not null values and not null enabled constraint for success"
        engine = schemavalidation
        schema = {{
            catalog_type = spark
            table = "{tablename}"
            not_null_columns = {json.dumps(colnames, indent=2)}
        }}
    }}
    """)
    print(str(not_null_constraint_config))
    schema_validation_engine = SchemavalidationEngine(not_null_constraint_config)
    summarymetrics = schema_validation_engine.apply(df, repository=None)
    overallsuccess = True
    for metric in summarymetrics:
        if not (metric["success"]):
            print("Error in : " + json.dumps(metric))
            overallsuccess = False

    if overallsuccess:
        print(json.dumps(summarymetrics, indent=2))
    assert False == overallsuccess, f"atleast one metric should have failed"


pytestmark = pytest.mark.spark
