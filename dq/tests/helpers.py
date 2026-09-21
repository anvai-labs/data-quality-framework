# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Shared outcome assertions for engine and adapter tests (TD-ARCH-9/U7).

These helpers replace the repeated manual success loops and per-file
outcome-keying logic so that a failing check fails the test with a clear
message instead of relying on printed diagnostics.
"""


def legacy_by_rule(outcomes):
    """Map portable outcomes by their rule_id to legacy dictionaries."""
    return {
        outcome.to_legacy()["details"]["rule_id"]: outcome.to_legacy()
        for outcome in outcomes
    }


def summaries_by_instance(results):
    """Map engine summaries by their details.instance string."""
    return {result["details"]["instance"]: result for result in results}


def assert_overall_success(results, context=""):
    """Assert every summary succeeded, naming the failures otherwise."""
    failures = [result for result in results if result["success"] is not True]
    assert not failures, f"{context}: failing outcomes {failures}"
