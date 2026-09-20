# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Versioned, deterministic execution evidence for data-quality gates."""

from __future__ import annotations

import hashlib
import json
import os
import platform
import tempfile
from datetime import datetime, timezone
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from urllib.parse import urlparse

from dq.exceptions import ConfigurationError, ValidationError

REPORT_SCHEMA_VERSION = "dq-report/v1"


def framework_version() -> str:
    """Return the installed distribution version or an explicit source marker."""
    try:
        return version("data-quality-framework")
    except PackageNotFoundError:
        return "source"


def sha256_file_reference(config_reference: str) -> str:
    """Return the SHA-256 digest of a local configuration reference.

    Evidence reports intentionally reject mutable remote configuration.  A
    remote rule set must be materialized to an immutable local file first.
    """
    parsed = urlparse(config_reference)
    if parsed.scheme not in ("", "file"):
        raise ConfigurationError(
            "Evidence reports require a local configuration file; "
            f"unsupported scheme: {parsed.scheme}"
        )

    path = Path(parsed.path if parsed.scheme == "file" else config_reference)
    try:
        content = path.read_bytes()
    except OSError as error:
        raise ConfigurationError(
            f"Could not read configuration for evidence digest: {error}"
        ) from error
    return hashlib.sha256(content).hexdigest()


def validate_sha256(value: str, label: str) -> str:
    """Normalize and validate a caller-supplied SHA-256 digest."""
    normalized = value.lower()
    if len(normalized) != 64 or any(c not in "0123456789abcdef" for c in normalized):
        raise ValidationError(f"{label} must be a 64-character SHA-256 digest")
    return normalized


def build_report(
    *,
    results: list[dict],
    config_reference: str,
    config_sha256: str,
    dataset_id: str,
    dataset_sha256: str,
    application_id: str,
    spark_version: str,
    generated_at: datetime | None = None,
) -> dict:
    """Build a schema-versioned report and fail on ambiguous outcomes."""
    if not results:
        raise ValidationError("Validation emitted zero check outcomes")

    normalized_results = []
    for index, result in enumerate(results):
        if "success" not in result or not isinstance(result["success"], bool):
            raise ValidationError(
                f"Check outcome {index} is missing a boolean 'success' field"
            )
        # JSON round-trip rejects unserializable engine output instead of
        # silently stringifying it into a misleading evidence record.
        try:
            normalized_results.append(json.loads(json.dumps(result)))
        except (TypeError, ValueError) as error:
            raise ValidationError(
                f"Check outcome {index} is not JSON serializable: {error}"
            ) from error

    normalized_config_sha = validate_sha256(config_sha256, "config_sha256")
    normalized_dataset_sha = validate_sha256(dataset_sha256, "dataset_sha256")
    passed = sum(1 for result in normalized_results if result["success"])
    total = len(normalized_results)
    failed = total - passed
    timestamp = generated_at or datetime.now(timezone.utc)
    if timestamp.tzinfo is None:
        raise ValidationError("generated_at must include a timezone")

    return {
        "schema_version": REPORT_SCHEMA_VERSION,
        "generated_at": timestamp.astimezone(timezone.utc)
        .isoformat()
        .replace("+00:00", "Z"),
        "status": "passed" if failed == 0 else "failed",
        "rule_set": {
            "reference": config_reference,
            "sha256": normalized_config_sha,
        },
        "dataset": {
            "id": dataset_id,
            "sha256": normalized_dataset_sha,
        },
        "runtime": {
            "framework": framework_version(),
            "python": platform.python_version(),
            "spark": spark_version,
            "application_id": application_id,
        },
        "summary": {"total": total, "passed": passed, "failed": failed},
        "checks": normalized_results,
    }


def write_report(path: str, report: dict) -> None:
    """Write a complete report atomically on the local filesystem."""
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=destination.parent,
            prefix=f".{destination.name}.",
            suffix=".tmp",
            delete=False,
        ) as output:
            temporary = Path(output.name)
            json.dump(report, output, indent=2, sort_keys=True)
            output.write("\n")
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, destination)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
