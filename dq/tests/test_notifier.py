# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Gate-event notifier contracts (ADR-008)."""

from unittest.mock import MagicMock, patch

import pytest

from dq.exceptions import ConfigurationError
from dq.notifier import (
    CompositeNotifier,
    GateEvent,
    GateNotifier,
    LoggingNotifier,
    WebhookNotifier,
    notifier_from_config,
)


def make_event(**overrides):
    defaults = {
        "run_key": 42,
        "dataset": "bars",
        "status": "passed",
        "failed": (),
        "total": 3,
        "ts": 1700000000000,
        "job_id": "local-1234",
    }
    defaults.update(overrides)
    return GateEvent(**defaults)


def test_gate_event_from_outcomes_collects_failures():
    outcomes = [
        {"check": "complete", "success": True},
        {"check": "unique", "success": False},
        {"check": "size", "success": True},
        {"check": "range", "success": False},
    ]
    event = GateEvent.from_outcomes(42, "bars", outcomes, 1700000000000, "job-1")
    assert event.status == "failed"
    assert event.failed == ("unique", "range")
    assert event.total == 4


def test_gate_event_passing_run_has_empty_failures():
    outcomes = [{"check": "complete", "success": True}]
    event = GateEvent.from_outcomes(42, "bars", outcomes, 0, "job-1")
    assert event.status == "passed"
    assert event.failed == ()


def test_logging_notifier_produces_structured_output():
    notifier = LoggingNotifier()
    event = make_event()
    notifier.notify(event)


def test_webhook_notifier_posts_json():
    event = make_event()
    with patch("urllib.request.urlopen") as mock_urlopen:
        notifier = WebhookNotifier("https://hooks.example.com/dq")
        notifier.notify(event)
        mock_urlopen.assert_called_once()
        request = mock_urlopen.call_args[0][0]
        assert request.get_method() == "POST"
        assert request.headers["Content-type"] == "application/json"


def test_webhook_notifier_swallows_delivery_failures():
    event = make_event()
    with patch("urllib.request.urlopen", side_effect=OSError("network down")):
        notifier = WebhookNotifier("https://hooks.example.com/unreachable")
        notifier.notify(event)


def test_webhook_notifier_swallows_http_errors():
    event = make_event()
    import urllib.error

    with patch(
        "urllib.request.urlopen",
        side_effect=urllib.error.URLError("HTTP 500"),
    ):
        notifier = WebhookNotifier("https://hooks.example.com/failing")
        notifier.notify(event)


def test_composite_notifier_fans_out_and_swallows_failures():
    good = LoggingNotifier()
    bad = WebhookNotifier("https://unreachable.example.com/dq")
    composite = CompositeNotifier(good, bad)
    event = make_event()
    with patch("urllib.request.urlopen", side_effect=OSError("down")):
        composite.notify(event)


def test_notifier_from_config_builds_logging_notifier():
    notifier = notifier_from_config([])
    assert isinstance(notifier, LoggingNotifier)


def test_notifier_from_config_builds_webhook_notifier(monkeypatch):
    monkeypatch.setenv("DQ_WEBHOOK_URL", "https://hooks.example.com/dq")
    notifier = notifier_from_config([{"type": "webhook", "url_env": "DQ_WEBHOOK_URL"}])
    assert isinstance(notifier, WebhookNotifier)


def test_notifier_from_config_fails_closed_without_webhook_url(monkeypatch):
    monkeypatch.delenv("DQ_WEBHOOK_URL", raising=False)
    with pytest.raises(ConfigurationError, match="DQ_WEBHOOK_URL"):
        notifier_from_config([{"type": "webhook", "url_env": "DQ_WEBHOOK_URL"}])


def test_notifier_from_config_rejects_unknown_types():
    with pytest.raises(ConfigurationError, match="not supported"):
        notifier_from_config([{"type": "sms"}])
