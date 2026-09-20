# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Command-line interface for Data Quality Framework."""

import argparse
import sys
import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def main():
    """Main entry point for dq-framework CLI."""
    parser = argparse.ArgumentParser(
        description="Data Quality Framework - Run data quality checks on Spark DataFrames",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  dq-framework config.conf
  dq-framework file://path/to/config.conf
  dq-framework s3://bucket/path/to/config.conf --spark-master spark://host:7077
        """,
    )
    parser.add_argument(
        "config",
        help="Path to HOCON configuration file (supports file://, s3://, abfss://, http://)",
    )
    parser.add_argument(
        "--spark-master",
        default="local[*]",
        help="Spark master URL (default: local[*])",
    )
    parser.add_argument(
        "--app-name",
        default="dq-framework",
        help="Spark application name (default: dq-framework)",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose output"
    )
    parser.add_argument(
        "--report-json",
        help="Write a versioned, machine-readable evidence report to this path",
    )
    parser.add_argument(
        "--dataset-id",
        help="Immutable dataset identifier (required with --report-json)",
    )
    parser.add_argument(
        "--dataset-sha256",
        help="Dataset SHA-256 digest (required with --report-json)",
    )

    args = parser.parse_args()

    if args.report_json and not (args.dataset_id and args.dataset_sha256):
        parser.error("--report-json requires --dataset-id and --dataset-sha256")
    if (args.dataset_id or args.dataset_sha256) and not args.report_json:
        parser.error("dataset evidence options require --report-json")

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    try:
        from pyspark.sql import SparkSession
        from dq.dq_framework import DQFramework
        from dq.report import build_report, sha256_file_reference, write_report
        from dq.validate import is_local_config_reference, validate_config

        if is_local_config_reference(args.config) and not validate_config(args.config):
            logger.error("Configuration validation failed; Spark was not started")
            return 1

        logger.info(f"Initializing Spark session with master: {args.spark_master}")
        spark = (
            SparkSession.builder.master(args.spark_master)
            .appName(args.app_name)
            .getOrCreate()
        )

        logger.info(f"Loading configuration from: {args.config}")
        framework = DQFramework(spark, args.config)

        logger.info("Running data quality checks...")
        results = framework.run()

        if not results:
            logger.error("Validation emitted zero check outcomes")
            return 1

        # Print results summary
        passed = sum(1 for r in results if r.get("success", False))
        failed = len(results) - passed

        if args.report_json:
            report = build_report(
                results=results,
                config_reference=args.config,
                config_sha256=sha256_file_reference(args.config),
                dataset_id=args.dataset_id,
                dataset_sha256=args.dataset_sha256,
                application_id=spark.sparkContext.applicationId,
                spark_version=spark.version,
            )
            write_report(args.report_json, report)
            logger.info("Wrote data-quality evidence report: %s", args.report_json)

        print(f"\n{'='*60}")
        print(f"Data Quality Check Results")
        print(f"{'='*60}")
        print(f"Total checks: {len(results)}")
        print(f"Passed: {passed}")
        print(f"Failed: {failed}")
        print(f"{'='*60}\n")

        if failed > 0:
            print("Failed checks:")
            for result in results:
                if not result.get("success", False):
                    print(f"  - {result.get('check', 'Unknown')}")
            return 1

        return 0

    except ImportError as e:
        logger.error(f"Missing dependency: {e}")
        logger.error(
            "Install Spark support with: pip install data-quality-framework[spark]"
        )
        return 1
    except Exception as e:
        logger.error(f"Error running data quality checks: {e}")
        if args.verbose:
            import traceback

            traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
