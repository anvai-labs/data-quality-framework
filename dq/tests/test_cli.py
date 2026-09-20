# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

import json
import sys
from unittest.mock import MagicMock, patch

import pytest

from dq import cli


def run_cli(arguments, *, valid=True, results=None):
    spark = MagicMock()
    spark.sparkContext.applicationId = "local-test"
    spark.version = "3.5.9"
    builder = MagicMock()
    builder.master.return_value.appName.return_value.getOrCreate.return_value = spark

    with (
        patch.object(sys, "argv", ["dq-framework", *arguments]),
        patch("pyspark.sql.SparkSession") as spark_session,
        patch("dq.validate.validate_config", return_value=valid),
        patch("dq.dq_framework.DQFramework") as framework,
    ):
        spark_session.builder = builder
        framework.return_value.run.return_value = results or []
        return cli.main(), builder, framework


def test_cli_rejects_invalid_config_before_starting_spark(tmp_path):
    config = tmp_path / "rules.conf"
    config.write_text("invalid\n", encoding="utf-8")

    exit_code, builder, framework = run_cli([str(config)], valid=False)

    assert exit_code == 1
    builder.master.assert_not_called()
    framework.assert_not_called()


def test_cli_rejects_zero_outcomes(tmp_path):
    config = tmp_path / "rules.conf"
    config.write_text("dqframework {}\n", encoding="utf-8")

    exit_code, _, _ = run_cli([str(config)], results=[])

    assert exit_code == 1


def test_cli_defers_remote_config_loading_to_framework():
    exit_code, builder, framework = run_cli(
        ["s3://bucket/rules.conf"], results=[{"check": "unique", "success": True}]
    )

    assert exit_code == 0
    builder.master.assert_called_once()
    framework.assert_called_once()


def test_cli_writes_failed_evidence_and_returns_failure(tmp_path):
    config = tmp_path / "rules.conf"
    config.write_text("dqframework { dqrules = [1] }\n", encoding="utf-8")
    destination = tmp_path / "report.json"

    exit_code, _, _ = run_cli(
        [
            str(config),
            "--report-json",
            str(destination),
            "--dataset-id",
            "bars:spy:2026q3",
            "--dataset-sha256",
            "a" * 64,
        ],
        results=[{"check": "coverage", "success": False}],
    )

    assert exit_code == 1
    evidence = json.loads(destination.read_text(encoding="utf-8"))
    assert evidence["status"] == "failed"
    assert evidence["dataset"]["id"] == "bars:spy:2026q3"
    assert evidence["summary"]["failed"] == 1


def test_cli_requires_complete_evidence_identity(tmp_path):
    config = tmp_path / "rules.conf"
    config.write_text("dqframework {}\n", encoding="utf-8")

    with patch.object(
        sys,
        "argv",
        ["dq-framework", str(config), "--report-json", str(tmp_path / "out.json")],
    ):
        with pytest.raises(SystemExit) as error:
            cli.main()

    assert error.value.code == 2


def test_cli_rejects_dataset_identity_without_report(tmp_path):
    config = tmp_path / "rules.conf"
    config.write_text("dqframework {}\n", encoding="utf-8")

    with patch.object(
        sys,
        "argv",
        ["dq-framework", str(config), "--dataset-id", "bars:spy:2026q3"],
    ):
        with pytest.raises(SystemExit) as error:
            cli.main()

    assert error.value.code == 2
