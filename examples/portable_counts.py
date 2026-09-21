# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Offline count-plan demonstration; not dataset execution or admission evidence."""

from decimal import Decimal
import json

from dq.plan import (
    CapabilitySet,
    ColumnRef,
    Comparison,
    DatasetRef,
    ExecutionPlan,
    MetricKind,
    Predicate,
    RuleKind,
    RuleSpec,
)


def main():
    dataset = DatasetRef("demo_rows", "a" * 64)  # illustrative, not a verified digest
    plan = ExecutionPlan(
        (
            RuleSpec(
                "nonempty", dataset, RuleKind.SIZE, Predicate(Comparison.GE, Decimal(1))
            ),
            RuleSpec(
                "complete",
                dataset,
                RuleKind.COMPLETENESS,
                Predicate(Comparison.GE, Decimal("1")),
                ColumnRef("value"),
            ),
        )
    )
    # A real adapter must compute these global counts and prove counts/v1 semantics.
    capabilities = CapabilitySet("fixture_adapter", "1", frozenset(MetricKind))
    counts = {metric: 3 for metric in plan.metrics}
    outcomes = plan.evaluate(counts, capabilities)
    print(
        json.dumps(
            {
                "demo_only": True,
                "plan_sha256": plan.fingerprint,
                "checks": [outcome.to_legacy() for outcome in outcomes],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
