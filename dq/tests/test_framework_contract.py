# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

from unittest.mock import MagicMock, patch

import pytest

from dq.dq_framework import DQFramework
from dq.exceptions import ConfigurationError, DataFrameNotFoundError, ValidationError


def framework_with_rules(rules):
    framework = DQFramework.__new__(DQFramework)
    framework._config = {"dqframework.dqrules": rules}
    framework._spark = MagicMock()
    framework._spark.sparkContext.applicationId = "local-test"
    framework.get_dataframe = MagicMock(return_value=MagicMock())
    return framework


def test_library_rejects_empty_rule_set():
    framework = framework_with_rules([])
    with pytest.raises(ConfigurationError, match="No dqrules"):
        framework.run()


def test_library_rejects_rule_without_engine():
    framework = framework_with_rules([{"checks": [{"constraint": "isComplete"}]}])
    with pytest.raises(ConfigurationError, match="missing required key 'engine'"):
        framework.run()


@pytest.mark.parametrize(
    "metrics, message",
    [
        ([], "zero outcomes"),
        ([{"check": "ambiguous"}], "boolean 'success'"),
        ([{"check": "ambiguous", "success": "yes"}], "boolean 'success'"),
    ],
)
def test_library_rejects_ambiguous_engine_outcomes(metrics, message):
    framework = framework_with_rules(
        [{"engine": "deequ", "checks": [{"constraint": "isComplete"}]}]
    )
    engine = MagicMock()
    engine.apply.return_value = metrics

    with patch("dq.dq_framework.EngineLoader") as loader:
        loader.return_value.load_engine.return_value = engine
        with pytest.raises(ValidationError, match=message):
            framework.run()


def test_library_preserves_explicit_failed_outcome():
    framework = framework_with_rules(
        [{"engine": "deequ", "checks": [{"constraint": "isComplete"}]}]
    )
    engine = MagicMock()
    engine.apply.return_value = [{"check": "complete", "success": False}]

    with patch("dq.dq_framework.EngineLoader") as loader:
        loader.return_value.load_engine.return_value = engine
        results = framework.run()

    assert results[0]["success"] is False
    assert results[0]["jobid"] == "local-test"
    assert isinstance(results[0]["ts"], int)


def test_configured_dataframe_load_failure_is_not_skipped():
    framework = DQFramework.__new__(DQFramework)
    framework._config = {"dqframework.dataframes": {"bars": "market.bars"}}
    framework.default_dataframe = None
    framework._resolve_dataframe = MagicMock(side_effect=OSError("catalog unavailable"))

    with pytest.raises(DataFrameNotFoundError, match="configured DataFrame 'bars'"):
        framework._load_dataframes()


def test_injected_default_takes_precedence_over_configured_default_reference():
    framework = DQFramework.__new__(DQFramework)
    injected = MagicMock()
    framework._config = {
        "dqframework.dataframes": {
            "default": "placeholder.table",
            "bars": "market.bars",
        }
    }
    framework.default_dataframe = injected
    bars = MagicMock()
    framework._resolve_dataframe = MagicMock(return_value=bars)

    loaded = framework._load_dataframes()

    assert loaded == {"default": injected, "bars": bars}
    framework._resolve_dataframe.assert_called_once_with("bars", "market.bars")


def test_framework_does_not_mutate_engine_owned_outcomes():
    framework = framework_with_rules([{"engine": "deequ"}])
    original = {"check": "complete", "success": True, "details": {"count": 1}}
    engine = MagicMock()
    engine.apply.return_value = [original]
    with patch("dq.dq_framework.EngineLoader") as loader:
        loader.return_value.load_engine.return_value = engine
        results = framework.run()
    results[0]["details"]["count"] = 2
    assert original == {"check": "complete", "success": True, "details": {"count": 1}}


def test_framework_rejects_nonfinite_metrics_even_on_success():
    framework = framework_with_rules([{"engine": "deequ"}])
    engine = MagicMock()
    engine.apply.return_value = [{"success": True, "details": float("nan")}]
    with patch("dq.dq_framework.EngineLoader") as loader:
        loader.return_value.load_engine.return_value = engine
        with pytest.raises(ValidationError, match="finite"):
            framework.run()


@pytest.mark.parametrize("limit", ["MAX_OUTCOMES", "MAX_BATCH_BYTES"])
def test_framework_bounds_cumulative_results_across_rules(monkeypatch, limit):
    framework = framework_with_rules([{"engine": "deequ"}, {"engine": "deequ"}])
    engine = MagicMock()
    engine.apply.return_value = [{"success": True}]
    monkeypatch.setattr("dq.dq_framework." + limit, 1)
    with patch("dq.dq_framework.EngineLoader") as loader:
        loader.return_value.load_engine.return_value = engine
        with pytest.raises(ValidationError, match="summary limits"):
            framework.run()
