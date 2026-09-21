# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Native Spark execution of portable count plans translated from HOCON checks.

Only the counts/v1 subset translates: isComplete, hasCompleteness with one
literal comparison, and hasSize with one integer comparison. Unsupported
constraints fail closed instead of silently degrading. Requires the optional
Spark extra (``pip install data-quality-framework[spark]``); no Deequ JAR is
needed because this path never invokes PyDeequ.
"""

from decimal import Decimal
import json
import sys

from pyhocon import ConfigFactory

from dq.exceptions import DQFrameworkError
from dq.spark_adapter import execute_plan
from dq.plan import DatasetRef
from dq.portable_config import translate_plan

CONFIG = """
    dqrules = [
        {
            name = "portable-spark-demo"
            checks = [
                { alias = "enough_rows", constraint = "hasSize", assertion = "lambda x: x >= 5", level = "Error" }
                { alias = "id_complete", constraint = "isComplete", column = "id", level = "Error" }
                { alias = "score_mostly_present", constraint = "hasCompleteness", column = "score", assertion = "lambda x: x >= 0.5", level = "Warning" }
            ]
        }
    ]
"""


def main():
    from pyspark.sql import SparkSession
    from pyspark.sql.types import (
        DecimalType,
        DoubleType,
        StringType,
        StructField,
        StructType,
    )

    spark = SparkSession.builder.master("local[*]").appName("dq-portable").getOrCreate()
    try:
        schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("score", DoubleType(), True),
                StructField("amount", DecimalType(10, 2), True),
            ]
        )
        rows = [
            ("a", 1.5, Decimal("1.25")),
            ("", float("nan"), None),
            ("b", float("inf"), Decimal("3.50")),
            (None, float("-inf"), Decimal("0.00")),
            ("c", None, Decimal("2.00")),
        ]
        dataframe = spark.createDataFrame(rows, schema)

        # Counts/v1 semantics: null and NaN are not present, empty strings and
        # string "NaN" are. Fixture expectations: rows=5, id=4, score=3.
        checks = ConfigFactory.parse_string(CONFIG)["dqrules"][0]["checks"]
        plan = translate_plan(checks, DatasetRef("demo_rows", "a" * 64))
        outcomes = execute_plan(plan, {"demo_rows": dataframe})
        print(
            json.dumps(
                {
                    "plan_sha256": plan.fingerprint,
                    "checks": [outcome.to_legacy() for outcome in outcomes],
                },
                indent=2,
            )
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    try:
        main()
    except DQFrameworkError as error:
        # Unsupported constraints fail closed with a clear translation error.
        print(
            f"portable translation rejected the configuration: {error}", file=sys.stderr
        )
        sys.exit(2)
