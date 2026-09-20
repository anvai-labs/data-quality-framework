# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Offline contracts for the engine-neutral evidence boundary."""

import json
import subprocess
import sys
from dataclasses import FrozenInstanceError

import pytest

from dq.exceptions import ValidationError
from dq.outcomes import CheckOutcome, normalize_outcomes


@pytest.mark.parametrize("success", [True, False])
def test_snapshot_preserves_legacy_shape_without_aliasing(success):
    original = {
        "check": "complete",
        "success": success,
        "details": {"values": [None, 1, 0.25, "ok", False]},
        "extension": {"version": 2},
    }
    expected = json.loads(json.dumps(original))
    outcome = CheckOutcome.from_legacy(original)
    original["details"]["values"].append("changed")
    exported = outcome.to_legacy()
    exported["extension"]["version"] = 3
    assert outcome.to_legacy() == expected
    assert outcome.success is success
    assert hash(outcome) == hash(CheckOutcome.from_legacy(expected))
    with pytest.raises(FrozenInstanceError):
        outcome._json = "{}"


@pytest.mark.parametrize(
    "value, message",
    [
        (None, "dictionary"),
        ([], "dictionary"),
        ({}, "boolean 'success'"),
        ({"success": 1}, "boolean 'success'"),
        ({"success": "true"}, "boolean 'success'"),
        ({"success": True, "details": float("nan")}, "finite"),
        ({"success": True, "details": float("inf")}, "finite"),
        ({"success": True, "details": {1: "value"}}, "string keys"),
        ({"success": True, "details": object()}, "not JSON serializable"),
        ({"success": True, "details": (1, 2)}, "not JSON serializable"),
        ({"success": True, "details": "\ud800"}, "UTF-8"),
    ],
)
def test_invalid_outcomes_fail_closed(value, message):
    with pytest.raises(ValidationError, match=message):
        CheckOutcome.from_legacy(value)


def test_direct_construction_cannot_bypass_validation():
    with pytest.raises(ValidationError, match="boolean 'success'"):
        CheckOutcome({"success": "yes"})


def test_cyclic_or_deep_diagnostics_are_bounded():
    cycle = []
    cycle.append(cycle)
    with pytest.raises(ValidationError, match="depth"):
        CheckOutcome.from_legacy({"success": True, "details": cycle})


@pytest.mark.parametrize(
    "details, message",
    [(list(range(10_001)), "nodes"), ("x" * 1_048_576, "bytes")],
    ids=["node-limit", "byte-limit"],
)
def test_diagnostic_size_is_bounded(details, message):
    with pytest.raises(ValidationError, match=message):
        CheckOutcome.from_legacy({"success": True, "details": details})


@pytest.mark.parametrize("values", [None, {}, "wrong", iter([])])
def test_batch_requires_materialized_sequence(values):
    with pytest.raises(ValidationError, match="list or tuple"):
        normalize_outcomes(values)


def test_batch_requires_nonempty_bounded_outcomes():
    with pytest.raises(ValidationError, match="zero outcomes"):
        normalize_outcomes([])
    with pytest.raises(ValidationError, match="10000"):
        normalize_outcomes([{"success": True}] * 10_001)
    typed = CheckOutcome.from_legacy({"success": False})
    assert normalize_outcomes([{"success": True}, typed]) == (
        CheckOutcome.from_legacy({"success": True}),
        typed,
    )


def test_core_import_needs_no_optional_engine_dependencies():
    code = """
import sys
class BlockEngines:
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in {'pyspark', 'pydeequ', 'great_expectations', 'boto3'}:
            raise AssertionError(fullname)
sys.meta_path.insert(0, BlockEngines())
from dq.outcomes import CheckOutcome
assert CheckOutcome({'success': True}).success
"""
    subprocess.run([sys.executable, "-c", code], check=True, timeout=30)


def test_batch_total_bytes_are_bounded(monkeypatch):
    outcome = CheckOutcome({"success": True})
    monkeypatch.setattr("dq.outcomes.MAX_BATCH_BYTES", outcome.serialized_size)
    assert normalize_outcomes([outcome]) == (outcome,)
    with pytest.raises(ValidationError, match="batch bytes"):
        normalize_outcomes([outcome, outcome])


def test_oversized_single_string_rejected_before_serialization():
    with pytest.raises(ValidationError, match="bytes"):
        CheckOutcome({"success": True, "details": "x" * 1_048_577})


def test_json_encoding_error_is_translated():
    with pytest.raises(ValidationError, match="not JSON serializable"):
        CheckOutcome({"success": True, "details": 10**5000})


def test_diagnostic_fields_are_not_in_repr():
    outcome = CheckOutcome({"success": False, "details": "private diagnostic"})
    assert "private diagnostic" not in repr(outcome)


def test_field_order_does_not_change_snapshot_identity():
    assert CheckOutcome({"success": True, "check": "x"}) == CheckOutcome(
        {"check": "x", "success": True}
    )
