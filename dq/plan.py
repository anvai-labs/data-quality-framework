# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Portable exact count semantics, independent of DataFrame execution engines.

This opt-in API neither translates legacy HOCON nor executes datasets. Adapters
must compute global counts for the bound snapshot and advertise counts/v1 only
after proving null/NaN, empty-input, and partition invariance contracts.
Groups/v1 adds grouped distinct-count bounds (ADR-004): three exact
threshold-independent metrics per (column, grouping) and rules that compare one
bound, passing vacuously on empty input exactly like the legacy constraint.
Ranges/v1 adds value-range bounds (ADR-005): exact column count, minimum, and
maximum metrics with rules that compare one bound, ignoring nulls and NaN and
passing vacuously on empty columns.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from fractions import Fraction
import hashlib
import json
import re

from dq.exceptions import ConfigurationError, ValidationError
from dq.outcomes import CheckOutcome, normalize_outcomes

SEMANTICS_VERSION = "counts/v1"
SEMANTIC_GROUPS_VERSION = "groups/v1"
SEMANTIC_RANGES_VERSION = "ranges/v1"
MAX_RULES = 10_000
MAX_COUNT = 2**64 - 1
_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,127}")
_RULE_ID = re.compile(r"[A-Za-z_][A-Za-z0-9_.:-]{0,127}")


def _identifier(value: str, label: str, pattern=_IDENTIFIER) -> None:
    if type(value) is not str or pattern.fullmatch(value) is None:
        raise ConfigurationError(f"{label} must be a bounded portable identifier")


def _text(value: str, label: str) -> None:
    if (
        type(value) is not str
        or not 1 <= len(value) <= 128
        or any(not 33 <= ord(character) <= 126 for character in value)
    ):
        raise ConfigurationError(f"{label} must be a bounded printable token")


class MetricKind(Enum):
    ROW_COUNT = "row_count"
    PRESENT_COUNT = "present_count"
    GROUP_COUNT = "group_count"
    GROUP_MIN_DISTINCT = "group_min_distinct"
    GROUP_MAX_DISTINCT = "group_max_distinct"
    COLUMN_COUNT = "column_count"
    COLUMN_MIN = "column_min"
    COLUMN_MAX = "column_max"


class RuleKind(Enum):
    SIZE = "size"
    COMPLETENESS = "completeness"
    GROUPED_DISTINCT = "grouped_distinct"
    VALUE_RANGE = "value_range"


class Comparison(Enum):
    GE = "ge"
    GT = "gt"
    LE = "le"
    LT = "lt"
    EQ = "eq"


class Severity(Enum):
    ERROR = "Error"
    WARNING = "Warning"


@dataclass(frozen=True, slots=True)
class DatasetRef:
    """Logical alias plus digest of an immutable, schema-bound snapshot."""

    name: str
    sha256: str

    def __post_init__(self):
        _identifier(self.name, "dataset name")
        if type(self.sha256) is not str or not re.fullmatch(
            r"[0-9a-fA-F]{64}", self.sha256
        ):
            raise ConfigurationError("dataset snapshot requires a SHA-256 digest")
        object.__setattr__(self, "sha256", self.sha256.lower())


@dataclass(frozen=True, slots=True)
class ColumnRef:
    """Single case-sensitive field; nested paths/SQL expressions are unsupported."""

    name: str

    def __post_init__(self):
        _identifier(self.name, "column name")


@dataclass(frozen=True, slots=True)
class Predicate:
    operator: Comparison
    threshold: Decimal

    def __post_init__(self):
        if type(self.operator) is not Comparison:
            raise ConfigurationError("predicate requires a Comparison tag")
        if (
            type(self.threshold) is not Decimal
            or not self.threshold.is_finite()
            or len(self.threshold.as_tuple().digits) > 38
            or not -38 <= self.threshold.as_tuple().exponent <= 38
        ):
            raise ConfigurationError("threshold requires a bounded finite Decimal")

    @property
    def threshold_text(self) -> str:
        # normalize() can round using the caller's Decimal context. Formatting
        # plus removal of insignificant fractional zeros cannot change value.
        if self.threshold == 0:
            return "0"
        text = format(self.threshold, "f")
        return text.rstrip("0").rstrip(".") if "." in text else text

    def matches(self, value: Fraction) -> bool:
        if type(value) is not Fraction:
            raise ValidationError("predicate evaluation requires an exact Fraction")
        threshold = Fraction(self.threshold)
        return {
            Comparison.GE: value >= threshold,
            Comparison.GT: value > threshold,
            Comparison.LE: value <= threshold,
            Comparison.LT: value < threshold,
            Comparison.EQ: value == threshold,
        }[self.operator]


@dataclass(frozen=True, slots=True)
class MetricKey:
    dataset: DatasetRef
    kind: MetricKind
    column: ColumnRef | None = None
    semantic_version: str = SEMANTICS_VERSION
    group_by: tuple[ColumnRef, ...] = ()

    def __post_init__(self):
        if type(self.semantic_version) is not str:
            raise ConfigurationError("unsupported metric semantic version")
        if type(self.dataset) is not DatasetRef or type(self.kind) is not MetricKind:
            raise ConfigurationError("metric requires typed dataset and kind")
        if self.kind in (
            MetricKind.GROUP_COUNT,
            MetricKind.GROUP_MIN_DISTINCT,
            MetricKind.GROUP_MAX_DISTINCT,
        ):
            if self.semantic_version != SEMANTIC_GROUPS_VERSION:
                raise ConfigurationError("group metrics require groups/v1")
            if (
                type(self.group_by) is not tuple
                or not self.group_by
                or any(type(column) is not ColumnRef for column in self.group_by)
            ):
                raise ConfigurationError("group metrics require grouped columns")
            if len(set(self.group_by)) != len(self.group_by):
                raise ConfigurationError("group metrics reject duplicate columns")
            if self.kind is MetricKind.GROUP_COUNT:
                if self.column is not None:
                    raise ConfigurationError("group count cannot specify a column")
            elif type(self.column) is not ColumnRef:
                raise ConfigurationError("group distinct requires a column")
            return
        if self.kind in (
            MetricKind.COLUMN_COUNT,
            MetricKind.COLUMN_MIN,
            MetricKind.COLUMN_MAX,
        ):
            if self.semantic_version != SEMANTIC_RANGES_VERSION:
                raise ConfigurationError("range metrics require ranges/v1")
            if self.group_by:
                raise ConfigurationError("range metrics cannot specify groups")
            if type(self.column) is not ColumnRef:
                raise ConfigurationError("range metrics require a column")
            return
        if self.semantic_version != SEMANTICS_VERSION:
            raise ConfigurationError("unsupported metric semantic version")
        if self.group_by:
            raise ConfigurationError("counts/v1 metrics cannot specify groups")
        if self.kind is MetricKind.ROW_COUNT:
            if self.column is not None:
                raise ConfigurationError("row count cannot specify a column")
        elif type(self.column) is not ColumnRef:
            raise ConfigurationError("present count requires a ColumnRef")


@dataclass(frozen=True, slots=True)
class RuleSpec:
    rule_id: str
    dataset: DatasetRef
    kind: RuleKind
    predicate: Predicate
    column: ColumnRef | None = None
    severity: Severity = Severity.ERROR
    group_by: tuple[ColumnRef, ...] = ()
    target: MetricKind | None = None

    def __post_init__(self):
        _identifier(self.rule_id, "rule ID", _RULE_ID)
        if (
            type(self.dataset) is not DatasetRef
            or type(self.kind) is not RuleKind
            or type(self.predicate) is not Predicate
            or type(self.severity) is not Severity
        ):
            raise ConfigurationError(
                "rule requires typed dataset, kind, predicate, severity"
            )
        if self.kind is RuleKind.GROUPED_DISTINCT:
            threshold = self.predicate.threshold
            if (
                type(self.column) is not ColumnRef
                or type(self.group_by) is not tuple
                or not self.group_by
                or any(type(column) is not ColumnRef for column in self.group_by)
                or self.target
                not in (MetricKind.GROUP_MIN_DISTINCT, MetricKind.GROUP_MAX_DISTINCT)
                or not 0 <= threshold <= MAX_COUNT
                or threshold != threshold.to_integral_value()
            ):
                raise ConfigurationError(
                    "grouped distinct requires a column, grouped columns, a "
                    "group distinct target, and an integer threshold"
                )
            return
        if self.kind is RuleKind.VALUE_RANGE:
            if (
                type(self.column) is not ColumnRef
                or self.group_by
                or self.target not in (MetricKind.COLUMN_MIN, MetricKind.COLUMN_MAX)
            ):
                raise ConfigurationError(
                    "value range requires a column and a column bound target"
                )
            return
        self._validate_counts_kind()

    def _validate_counts_kind(self):
        if self.kind is not RuleKind.COMPLETENESS:
            threshold = self.predicate.threshold
            if (
                self.column is not None
                or self.group_by
                or self.target is not None
                or not 0 <= threshold <= MAX_COUNT
                or threshold != threshold.to_integral_value()
            ):
                raise ConfigurationError(
                    "size requires an integer threshold and no column"
                )
            return
        if self.group_by or self.target is not None:
            raise ConfigurationError("completeness cannot target grouped metrics")
        if type(self.column) is not ColumnRef or not 0 <= self.predicate.threshold <= 1:
            raise ConfigurationError(
                "completeness requires a column and threshold in [0,1]"
            )

    @property
    def required_metrics(self) -> tuple[MetricKey, ...]:
        rows = MetricKey(self.dataset, MetricKind.ROW_COUNT)
        if self.kind is RuleKind.SIZE:
            return (rows,)
        if self.kind is RuleKind.GROUPED_DISTINCT:
            if self.target is MetricKind.GROUP_MAX_DISTINCT:
                target_kind = MetricKind.GROUP_MAX_DISTINCT
            else:
                target_kind = MetricKind.GROUP_MIN_DISTINCT
            return (
                MetricKey(
                    self.dataset,
                    MetricKind.GROUP_COUNT,
                    semantic_version=SEMANTIC_GROUPS_VERSION,
                    group_by=self.group_by,
                ),
                MetricKey(
                    self.dataset,
                    target_kind,
                    self.column,
                    SEMANTIC_GROUPS_VERSION,
                    self.group_by,
                ),
            )
        if self.kind is RuleKind.VALUE_RANGE:
            if self.target is MetricKind.COLUMN_MAX:
                target_kind = MetricKind.COLUMN_MAX
            else:
                target_kind = MetricKind.COLUMN_MIN
            return (
                MetricKey(
                    self.dataset,
                    MetricKind.COLUMN_COUNT,
                    self.column,
                    SEMANTIC_RANGES_VERSION,
                ),
                MetricKey(
                    self.dataset,
                    target_kind,
                    self.column,
                    SEMANTIC_RANGES_VERSION,
                ),
            )
        return (rows, MetricKey(self.dataset, MetricKind.PRESENT_COUNT, self.column))

    def to_dict(self) -> dict:
        document = {
            "rule_id": self.rule_id,
            "dataset": {"name": self.dataset.name, "sha256": self.dataset.sha256},
            "kind": self.kind.value,
            "column": self.column.name if self.column else None,
            "predicate": {
                "operator": self.predicate.operator.value,
                "threshold": self.predicate.threshold_text,
            },
            "severity": self.severity.value,
        }
        if self.kind is RuleKind.GROUPED_DISTINCT:
            document["group_by"] = [column.name for column in self.group_by]
            document["target"] = self.target.value
        if self.kind is RuleKind.VALUE_RANGE:
            document["target"] = self.target.value
        return document


@dataclass(frozen=True, slots=True)
class CapabilitySet:
    """Adapter declaration, not a certificate that its implementation conforms."""

    adapter: str
    version: str
    metrics: frozenset[MetricKind]
    semantic_version: str = SEMANTICS_VERSION

    def __post_init__(self):
        _identifier(self.adapter, "adapter name", _RULE_ID)
        _text(self.version, "adapter version")
        _text(self.semantic_version, "semantic version")
        if type(self.metrics) is not frozenset or any(
            type(metric) is not MetricKind for metric in self.metrics
        ):
            raise ConfigurationError("capabilities require immutable MetricKind tags")


@dataclass(frozen=True, slots=True)
class ExecutionPlan:
    """Canonical logical plan; deduplication does not assert a physical scan count."""

    rules: tuple[RuleSpec, ...]
    metrics: tuple[MetricKey, ...] = field(init=False)
    semantic_version: str = field(init=False)
    _fingerprint: str = field(init=False, repr=False, compare=False)

    def __post_init__(self):
        if type(self.rules) is not tuple or not 1 <= len(self.rules) <= MAX_RULES:
            raise ConfigurationError(f"plan requires a tuple of 1..{MAX_RULES} rules")
        identifiers = set()
        datasets = {}
        for rule in self.rules:
            if type(rule) is not RuleSpec or rule.rule_id in identifiers:
                raise ConfigurationError("plan requires typed rules with unique IDs")
            identifiers.add(rule.rule_id)
            previous = datasets.setdefault(rule.dataset.name, rule.dataset.sha256)
            if previous != rule.dataset.sha256:
                raise ConfigurationError("dataset alias cannot bind multiple snapshots")
        rules = tuple(sorted(self.rules, key=lambda rule: rule.rule_id))
        metrics = tuple(
            dict.fromkeys(metric for rule in rules for metric in rule.required_metrics)
        )
        versions = {metric.semantic_version for metric in metrics}
        if len(versions) != 1:
            raise ConfigurationError("plan cannot mix metric semantic versions")
        object.__setattr__(self, "rules", rules)
        object.__setattr__(self, "metrics", metrics)
        object.__setattr__(self, "semantic_version", versions.pop())
        object.__setattr__(
            self,
            "_fingerprint",
            hashlib.sha256(self.canonical_json().encode("utf-8")).hexdigest(),
        )

    def canonical_json(self) -> str:
        return json.dumps(
            {
                "semantic_version": self.semantic_version,
                "rules": [rule.to_dict() for rule in self.rules],
            },
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )

    @property
    def fingerprint(self) -> str:
        return self._fingerprint

    def __hash__(self) -> int:
        return hash(self._fingerprint)

    def validate_for(self, capabilities: CapabilitySet) -> None:
        if (
            type(capabilities) is not CapabilitySet
            or capabilities.semantic_version != self.semantic_version
            or not {metric.kind for metric in self.metrics} <= capabilities.metrics
        ):
            raise ConfigurationError(
                "adapter capabilities cannot preserve plan semantics"
            )

    def evaluate(
        self, values: dict, capabilities: CapabilitySet
    ) -> tuple[CheckOutcome, ...]:
        """Evaluate supplied global metrics; does not verify their dataset provenance.

        Counts/v1 counts all rows; present excludes null and floating-point NaN.
        Completeness is present/rows, compared exactly, and fails on empty input.
        Groups/v1 compares grouped distinct bounds exactly and passes vacuously
        on empty input. No rule severity can convert a failed outcome to passing
        v1 evidence.
        """
        self.validate_for(capabilities)
        if type(values) is not dict or len(values) != len(self.metrics):
            raise ValidationError("execution must supply exactly the required metrics")
        values = values.copy()
        if set(values) != set(self.metrics):
            raise ValidationError("execution must supply exactly the required metrics")
        for metric, value in values.items():
            if self.semantic_version == SEMANTIC_RANGES_VERSION and metric.kind in (
                MetricKind.COLUMN_MIN,
                MetricKind.COLUMN_MAX,
            ):
                if type(value) is not Fraction:
                    raise ValidationError(
                        "range bounds must be exact Fractions, not "
                        f"{type(value).__name__}"
                    )
            elif type(value) is not int or not 0 <= value <= MAX_COUNT:
                raise ValidationError("metric counts must be unsigned 64-bit integers")
        if self.semantic_version == SEMANTIC_GROUPS_VERSION:
            return self._evaluate_grouped(values, capabilities)
        if self.semantic_version == SEMANTIC_RANGES_VERSION:
            return self._evaluate_ranges(values, capabilities)
        for metric, value in values.items():
            if (
                metric.kind is MetricKind.PRESENT_COUNT
                and value > values[MetricKey(metric.dataset, MetricKind.ROW_COUNT)]
            ):
                raise ValidationError("present count cannot exceed row count")
        fingerprint = self.fingerprint
        outcomes = []
        for rule in self.rules:
            rows = values[rule.required_metrics[0]]
            numerator = (
                rows if rule.kind is RuleKind.SIZE else values[rule.required_metrics[1]]
            )
            denominator = 1 if rule.kind is RuleKind.SIZE else rows
            observed = {"numerator": numerator, "denominator": denominator}
            success = bool(denominator) and rule.predicate.matches(
                Fraction(numerator, denominator)
            )
            outcomes.append(
                CheckOutcome(
                    {
                        "check": rule.rule_id,
                        "rule_id": rule.rule_id,
                        "success": success,
                        "details": {
                            **rule.to_dict(),
                            "observed": observed,
                            "reason": "evaluated" if denominator else "empty_dataset",
                            "semantic_version": SEMANTICS_VERSION,
                            "plan_sha256": fingerprint,
                            "adapter": {
                                "name": capabilities.adapter,
                                "version": capabilities.version,
                            },
                        },
                    }
                )
            )
        return normalize_outcomes(outcomes)

    def _evaluate_grouped(
        self, values: dict, capabilities: CapabilitySet
    ) -> tuple[CheckOutcome, ...]:
        fingerprint = self.fingerprint
        outcomes = []
        for rule in self.rules:
            count_key, target_key = rule.required_metrics
            group_count = values[count_key]
            if group_count == 0:
                success = True
                reason = "no_groups"
                numerator = 0
            else:
                numerator = values[target_key]
                success = rule.predicate.matches(Fraction(numerator))
                reason = "evaluated"
            outcomes.append(
                CheckOutcome(
                    {
                        "check": rule.rule_id,
                        "rule_id": rule.rule_id,
                        "success": success,
                        "details": {
                            **rule.to_dict(),
                            "observed": {"numerator": numerator, "denominator": 1},
                            "reason": reason,
                            "semantic_version": SEMANTIC_GROUPS_VERSION,
                            "plan_sha256": fingerprint,
                            "adapter": {
                                "name": capabilities.adapter,
                                "version": capabilities.version,
                            },
                        },
                    }
                )
            )
        return normalize_outcomes(outcomes)

    def _evaluate_ranges(
        self, values: dict, capabilities: CapabilitySet
    ) -> tuple[CheckOutcome, ...]:
        fingerprint = self.fingerprint
        outcomes = []
        for rule in self.rules:
            count_key, target_key = rule.required_metrics
            value_count = values[count_key]
            if value_count == 0:
                success = True
                reason = "no_values"
                bound = Fraction(0)
            else:
                bound = values[target_key]
                success = rule.predicate.matches(bound)
                reason = "evaluated"
            outcomes.append(
                CheckOutcome(
                    {
                        "check": rule.rule_id,
                        "rule_id": rule.rule_id,
                        "success": success,
                        "details": {
                            **rule.to_dict(),
                            "observed": {
                                "numerator": bound.numerator,
                                "denominator": bound.denominator,
                            },
                            "reason": reason,
                            "semantic_version": SEMANTIC_RANGES_VERSION,
                            "plan_sha256": fingerprint,
                            "adapter": {
                                "name": capabilities.adapter,
                                "version": capabilities.version,
                            },
                        },
                    }
                )
            )
        return normalize_outcomes(outcomes)
