# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

import hashlib
import json
from datetime import datetime, timezone

import pytest

from dq.exceptions import ConfigurationError, ValidationError
from dq.report import build_report, sha256_file_reference, write_report

DATASET_SHA = "a" * 64


def report(results):
    return build_report(
        results=results,
        config_reference="rules.conf",
        config_sha256="B" * 64,
        dataset_id="bars:spy:2026q3",
        dataset_sha256=DATASET_SHA,
        application_id="local-1",
        spark_version="3.5.9",
        generated_at=datetime(2026, 9, 20, 12, 0, tzinfo=timezone.utc),
    )


def test_report_binds_dataset_rule_set_and_all_outcomes():
    evidence = report(
        [
            {"check": "unique", "success": True},
            {"check": "complete", "success": False, "details": "one gap"},
        ]
    )

    assert evidence["schema_version"] == "dq-report/v1"
    assert evidence["generated_at"] == "2026-09-20T12:00:00Z"
    assert evidence["status"] == "failed"
    assert evidence["rule_set"]["sha256"] == "b" * 64
    assert evidence["dataset"]["sha256"] == DATASET_SHA
    assert evidence["runtime"]["framework"]
    assert evidence["summary"] == {"total": 2, "passed": 1, "failed": 1}
    assert len(evidence["checks"]) == 2


@pytest.mark.parametrize(
    "results, message",
    [
        ([], "zero check outcomes"),
        ([{"check": "unknown"}], "boolean 'success'"),
        ([{"check": "wrong", "success": "yes"}], "boolean 'success'"),
    ],
)
def test_report_rejects_ambiguous_outcomes(results, message):
    with pytest.raises(ValidationError, match=message):
        report(results)


def test_report_rejects_invalid_or_unserializable_identity():
    with pytest.raises(ValidationError, match="dataset_sha256"):
        build_report(
            results=[{"success": True}],
            config_reference="rules.conf",
            config_sha256="b" * 64,
            dataset_id="dataset",
            dataset_sha256="not-a-digest",
            application_id="local-1",
            spark_version="3.5.9",
        )

    with pytest.raises(ValidationError, match="not JSON serializable"):
        report([{"success": True, "details": object()}])


def test_local_config_digest_and_atomic_report_write(tmp_path):
    config = tmp_path / "rules.conf"
    config.write_text("dqframework { dqrules = [] }\n", encoding="utf-8")
    expected = hashlib.sha256(config.read_bytes()).hexdigest()

    assert sha256_file_reference(str(config)) == expected
    assert sha256_file_reference(f"file://{config}") == expected

    destination = tmp_path / "evidence" / "report.json"
    evidence = report([{"check": "unique", "success": True}])
    write_report(str(destination), evidence)
    assert json.loads(destination.read_text(encoding="utf-8")) == evidence
    assert list(destination.parent.glob(".report.json.*.tmp")) == []


def test_remote_config_cannot_be_claimed_as_immutable_evidence():
    with pytest.raises(ConfigurationError, match="local configuration"):
        sha256_file_reference("https://example.test/rules.conf")


@pytest.mark.parametrize("number", [float("nan"), float("inf"), -float("inf")])
def test_report_rejects_nonfinite_diagnostics(number):
    with pytest.raises(ValidationError, match="finite"):
        report([{"check": "finite", "success": True, "details": number}])


def test_report_preserves_typed_and_legacy_equivalence():
    from dq.outcomes import CheckOutcome

    legacy = {"check": "complete", "success": True, "details": {"count": 3}}
    assert report([CheckOutcome.from_legacy(legacy)]) == report([legacy])
