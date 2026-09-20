# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Immutable, engine-independent snapshots of bounded validation summaries.

Legacy extension fields are retained, but only strict JSON values may cross this
boundary. This is not a portable rule model or a raw-row quarantine store.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass, field

from dq.exceptions import ValidationError

MAX_OUTCOMES = 10_000
MAX_OUTCOME_BYTES = 1_048_576
MAX_BATCH_BYTES = 16 * MAX_OUTCOME_BYTES
MAX_DIAGNOSTIC_NODES = 10_000
MAX_DIAGNOSTIC_DEPTH = 16


def _validate_json(value: object, depth: int, budget: list[int]) -> None:
    if depth > MAX_DIAGNOSTIC_DEPTH:
        raise ValidationError("Check outcome exceeds diagnostic depth limit")
    budget[0] -= 1
    if budget[0] < 0:
        raise ValidationError("Check outcome exceeds diagnostic nodes limit")
    if value is None or type(value) in (bool, int):
        return
    if type(value) is float:
        if not math.isfinite(value):
            raise ValidationError("Check outcome numbers must be finite")
        return
    if type(value) is str:
        if len(value) > MAX_OUTCOME_BYTES:
            raise ValidationError("Check outcome exceeds diagnostic bytes limit")
        try:
            value.encode("utf-8")
        except UnicodeEncodeError as error:
            raise ValidationError(
                "Check outcome strings must be valid UTF-8"
            ) from error
        return
    if type(value) is dict:
        for key, item in value.items():
            if type(key) is not str:
                raise ValidationError("Check outcome dictionaries require string keys")
            _validate_json(key, depth + 1, budget)
            _validate_json(item, depth + 1, budget)
        return
    if type(value) is list:
        for item in value:
            _validate_json(item, depth + 1, budget)
        return
    raise ValidationError("Check outcome is not JSON serializable")


@dataclass(frozen=True, slots=True, init=False)
class CheckOutcome:
    """Validated immutable snapshot; exports always return independent copies.

    Boolean success remains authoritative for v2 compatibility. Stable portable rule
    IDs, metric semantics, and execution provenance types belong to the next slice.
    The canonical JSON bytes retain legacy extension fields without mutable aliases.
    """

    _json: bytes = field(repr=False)
    success: bool

    def __init__(self, result: dict):
        if type(result) is not dict:
            raise ValidationError("Check outcome must be a dictionary")
        if type(result.get("success")) is not bool:
            raise ValidationError("Check outcome is missing a boolean 'success' field")
        _validate_json(result, 0, [MAX_DIAGNOSTIC_NODES])
        try:
            encoder = json.JSONEncoder(
                sort_keys=True, ensure_ascii=False, allow_nan=False
            )
            chunks = []
            size = 0
            for chunk in encoder.iterencode(result):
                encoded = chunk.encode("utf-8")
                size += len(encoded)
                if size > MAX_OUTCOME_BYTES:
                    raise ValidationError(
                        "Check outcome exceeds diagnostic bytes limit"
                    )
                chunks.append(encoded)
            payload = b"".join(chunks)
        except (TypeError, ValueError) as error:
            raise ValidationError("Check outcome is not JSON serializable") from error
        object.__setattr__(self, "_json", payload)
        object.__setattr__(self, "success", result["success"])

    @classmethod
    def from_legacy(cls, result: dict) -> CheckOutcome:
        return cls(result)

    @property
    def serialized_size(self) -> int:
        return len(self._json)

    def to_legacy(self) -> dict:
        return json.loads(self._json)


def normalize_outcomes(results: list | tuple) -> tuple[CheckOutcome, ...]:
    """Validate an entire bounded batch before exposing any results."""
    if type(results) not in (list, tuple):
        raise ValidationError("Check outcomes must be a list or tuple")
    if not results:
        raise ValidationError("Validation emitted zero outcomes (zero check outcomes)")
    if len(results) > MAX_OUTCOMES:
        raise ValidationError(f"Check outcomes exceed limit of {MAX_OUTCOMES}")
    outcomes = []
    total_bytes = 0
    for result in results:
        outcome = (
            result if type(result) is CheckOutcome else CheckOutcome.from_legacy(result)
        )
        total_bytes += outcome.serialized_size
        if total_bytes > MAX_BATCH_BYTES:
            raise ValidationError("Check outcomes exceed batch bytes limit")
        outcomes.append(outcome)
    return tuple(outcomes)
