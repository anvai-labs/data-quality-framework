# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Sample PySpark job demonstrating Data Quality Framework usage."""
import os
import sys

# Ensure SPARK_VERSION is set for PyDeequ
os.environ["SPARK_VERSION"] = "3.5"

from pyspark.sql import SparkSession
from dq.dq_framework import DQFramework


def main():
    # Create local Spark session
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("dq-framework-example") \
        .config("spark.jars", "lib/deequ-2.0.21-spark-3.5.jar") \
        .getOrCreate()

    # Create sample data
    data = [
        ("1", "Alice", 34, "alice@example.com"),
        ("2", "Bob", 45, "bob@example.com"),
        ("3", "Catherine", None, "cathy@example.com"),
        ("4", "David", 28, None),
    ]
    df = spark.createDataFrame(data, ["id", "name", "age", "email"])

    # Define validation rules
    config = """
    dqframework {
      dqrules = [
        {
          name = "completeness_checks"
          engine = "deequ"
          dataframes = ["default"]
          checks = [
            { constraint_name = "id_complete", constraint = "isComplete", column = "id", level = "Error" }
            { constraint_name = "id_unique", constraint = "isUnique", column = "id", level = "Error" }
            { constraint_name = "name_complete", constraint = "isComplete", column = "name", level = "Warning" }
            { constraint_name = "email_complete", constraint = "isComplete", column = "email", level = "Warning" }
          ]
        }
      ]
    }
    """

    # Run data quality checks
    framework = DQFramework(spark, config, default_dataframe=df)
    results = framework.run()

    # Print results
    passed = sum(1 for r in results if r.get("success", False))
    failed = len(results) - passed

    sep = "=" * 60
    print(f"\n{sep}")
    print(f"Data Quality Check Results")
    print(f"{sep}")
    print(f"Total checks: {len(results)}")
    print(f"Passed:       {passed}")
    print(f"Failed:       {failed}")
    print(f"{sep}\n")

    for result in results:
        status = "PASS" if result.get("success", False) else "FAIL"
        check_name = result.get("check", "Unknown")
        print(f"  [{status}] {check_name}")

    spark.stop()
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
