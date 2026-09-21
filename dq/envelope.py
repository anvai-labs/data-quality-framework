# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Ruleset envelope: composes multiple ExecutionPlans into one certified
ruleset identity with an aggregate verdict (ADR-004/005 follow-up).

The envelope solves the semantic-version fragmentation problem: a real
ruleset mixes counts/v1 completeness, groups/v1 distinct bounds, and
ranges/v1 value bounds, but plans are single-semantics. The envelope lets
callers evaluate all subplans through one port and get one verdict, while
each subplan keeps its own fingerprint, outcomes, and capability contract.
"""

from __future__ import annotations

import hashlib

from dq.exceptions import ConfigurationError
from dq.plan import CapabilitySet, ExecutionPlan


class RulesetEnvelope:
    """Composes multiple ExecutionPlans into one certified ruleset identity.

    All plans must share the same dataset snapshot digest. The envelope's
    fingerprint is derived from the sorted child fingerprints, so plan
    order does not affect identity. Evaluation dispatches to each child
    plan's ``evaluate()`` and returns one aggregate verdict.
    """

    def __init__(self, plans: tuple[ExecutionPlan, ...]):
        if (
            not isinstance(plans, tuple)
            or len(plans) < 1
            or not all(isinstance(p, ExecutionPlan) for p in plans)
        ):
            raise ConfigurationError(
                "RulesetEnvelope requires a non-empty tuple of ExecutionPlan"
            )
        digests = set()
        for plan in plans:
            for metric in plan.metrics:
                digests.add(metric.dataset.sha256)
        if len(digests) != 1:
            raise ConfigurationError(
                "all plans in a RulesetEnvelope must share the same "
                "dataset snapshot digest"
            )
        self._plans = tuple(sorted(plans, key=lambda p: p.fingerprint))
        child_fps = sorted(p.fingerprint for p in self._plans)
        identity_input = "|".join(child_fps)
        self._fingerprint = hashlib.sha256(identity_input.encode("utf-8")).hexdigest()

    @property
    def plans(self) -> tuple[ExecutionPlan, ...]:
        return self._plans

    @property
    def fingerprint(self) -> str:
        """Deterministic composite fingerprint from sorted child fingerprints."""
        return self._fingerprint

    def evaluate(
        self,
        values: dict,
        capabilities_by_version: dict[str, CapabilitySet],
    ) -> list:
        """Evaluate all subplans and return one aggregate verdict.

        Each subplan is evaluated with the capabilities matching its
        semantic version. Returns a flat list of CheckOutcome dicts, one
        per check, across all subplans.
        """
        all_outcomes = []
        for plan in self._plans:
            caps = capabilities_by_version.get(plan.semantic_version)
            if caps is None:
                raise ConfigurationError(
                    f"no capabilities provided for semantic version "
                    f"{plan.semantic_version!r}"
                )
            plan_values = {
                metric: value
                for metric, value in values.items()
                if metric in plan.metrics
            }
            outcomes = plan.evaluate(plan_values, caps)
            all_outcomes.extend(outcomes)
        return all_outcomes
