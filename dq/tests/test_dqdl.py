# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

from pyhocon import ConfigFactory
import pytest

from dq.engine.dqdl.dqdl_engine import DqdlEngine
from dq.exceptions import ConfigurationError
from dq.exceptions import ValidationError
from unittest.mock import Mock, patch


@pytest.mark.parametrize("ruleset", [None, "", "   ", 123])
def test_invalid_ruleset_is_rejected_before_execution(ruleset):
    with pytest.raises(ConfigurationError, match="non-empty 'ruleset'"):
        DqdlEngine({"ruleset": ruleset}).apply(object())


@pytest.mark.parametrize("limit", [0, -1, True, 10001, "10"])
def test_invalid_summary_limit_is_rejected_before_execution(limit):
    with pytest.raises(ConfigurationError, match="max_outcomes"):
        DqdlEngine({"ruleset": "Rules=[RowCount > 0]", "max_outcomes": limit}).apply(
            object()
        )


def test_repository_request_is_not_silently_ignored():
    with pytest.raises(ConfigurationError, match="persistence is unsupported"):
        DqdlEngine({}).apply(object(), repository={"dataset": "quality"})


@pytest.mark.parametrize(
    ("details", "message"),
    [
        ([], "zero rule outcomes"),
        ([{"Outcome": "Passed", "Rule": "one"}] * 3, "exceed max_outcomes"),
        ([{"Outcome": "Skipped", "Rule": "one"}], "Unknown DQDL outcome"),
        ([{"Rule": "one"}], "Unknown DQDL outcome"),
        ([{"Outcome": "Passed"}], "no rule identity"),
        ([{"Outcome": "Passed", "Rule": " "}], "no rule identity"),
    ],
)
def test_ambiguous_or_excessive_results_fail_closed(details, message):
    result = Mock()
    result.limit.return_value.collect.return_value = [
        Mock(asDict=Mock(return_value=detail)) for detail in details
    ]
    with patch(
        "dq.engine.dqdl.dqdl_engine.EvaluateDataQuality.process", return_value=result
    ):
        with pytest.raises(ValidationError, match=message):
            DqdlEngine({"ruleset": "Rules=[RowCount > 0]", "max_outcomes": 2}).apply(
                Mock()
            )
    result.limit.assert_called_once_with(3)


def test_failed_dqdl_rule_returns_false(spark):
    dataframe = spark.createDataFrame([(1,)], ["id"])
    outcomes = DqdlEngine({"ruleset": "Rules=[RowCount >= 2]"}).apply(dataframe)
    assert len(outcomes) == 1
    assert outcomes[0]["success"] is False


def test_dqdl_engine_runs_supported_rules(spark):
    dataframe = spark.createDataFrame([(1, "ready"), (2, "ready")], ["id", "state"])
    config = ConfigFactory.parse_string('''
        {
          name = "dqdl-contract"
          engine = "dqdl"
          ruleset = """
            Rules=[
              RowCount >= 2,
              IsComplete "id"
            ]
          """
        }
        ''')

    outcomes = DqdlEngine(config).apply(dataframe)

    assert len(outcomes) == 2
    assert all(outcome["success"] is True for outcome in outcomes)
    assert all(outcome["check"] for outcome in outcomes)


def test_dqdl_engine_requires_non_empty_ruleset():
    config = ConfigFactory.parse_string('{ engine = "dqdl" }')

    with pytest.raises(
        ConfigurationError, match="DQDL engine requires a non-empty 'ruleset'"
    ):
        DqdlEngine(config).apply(object())


def test_dqdl_engine_maps_failed_summary_without_collecting_input(monkeypatch):
    class FakeRow:
        def asDict(self, recursive=False):
            assert recursive is True
            return {"Outcome": "Failed", "EvaluatedRule": "RowCount >= 3"}

    class FakeOutcomes:
        def limit(self, count):
            assert count == 1001
            return self

        def collect(self):
            return [FakeRow()]

    class FakeDataFrame:
        sparkSession = object()

    monkeypatch.setattr(
        "dq.engine.dqdl.dqdl_engine.EvaluateDataQuality.process",
        lambda spark, dataframe, ruleset: FakeOutcomes(),
    )
    config = ConfigFactory.parse_string(
        '{ engine = "dqdl", ruleset = "Rules=[RowCount >= 3]" }'
    )

    assert DqdlEngine(config).apply(FakeDataFrame()) == [
        {
            "check": "RowCount >= 3",
            "success": False,
            "details": {"Outcome": "Failed", "EvaluatedRule": "RowCount >= 3"},
        }
    ]
