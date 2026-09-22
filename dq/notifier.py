# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Gate-event notifier port (ADR-008).

Separate from the sink port: a sink persists evidence; a notifier announces
gate events. Notification failures are logged and swallowed — they must
never alter a run's outcome or block evidence durability.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

from dq.exceptions import ConfigurationError


@dataclass(frozen=True, slots=True)
class GateEvent:
    """Immutable record of a gate result, built from validated outcomes."""

    run_key: int
    dataset: str
    status: str
    failed: tuple[str, ...]
    total: int
    ts: int
    job_id: str

    @classmethod
    def from_outcomes(
        cls,
        run_key: int,
        dataset: str,
        outcomes: list[dict[str, Any]],
        ts: int,
        job_id: str,
    ) -> GateEvent:
        failed_checks = tuple(
            outcome["check"]
            for outcome in outcomes
            if outcome.get("success") is not True
        )
        status = "passed" if not failed_checks else "failed"
        return cls(
            run_key=run_key,
            dataset=dataset,
            status=status,
            failed=failed_checks,
            total=len(outcomes),
            ts=ts,
            job_id=job_id,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_key": self.run_key,
            "dataset": self.dataset,
            "status": self.status,
            "failed": list(self.failed),
            "total": self.total,
            "ts": self.ts,
            "job_id": self.job_id,
        }


class GateNotifier(ABC):
    """Port for announcing gate events to humans or downstream systems.

    Implementations must not raise: notification failures are logged and
    swallowed so they never alter a run's outcome.
    """

    @abstractmethod
    def notify(self, event: GateEvent) -> None:
        """Announce one gate event. Must not raise on delivery failure."""
        raise NotImplementedError


class LoggingNotifier(GateNotifier):
    """Default notifier: structured log line, always succeeds."""

    def __init__(self):
        import logging

        self._logger = logging.getLogger("dq.gate_events")

    def notify(self, event: GateEvent) -> None:
        self._logger.info(
            "gate_event dataset=%s status=%s run_key=%d failed=%d/%d",
            event.dataset,
            event.status,
            event.run_key,
            len(event.failed),
            event.total,
        )


class CompositeNotifier(GateNotifier):
    """Fan-out to multiple notifiers; each failure is logged and swallowed."""

    def __init__(self, *notifiers: GateNotifier):
        self._notifiers = notifiers

    def notify(self, event: GateEvent) -> None:
        import logging

        logger = logging.getLogger("dq.gate_events")
        for notifier in self._notifiers:
            try:
                notifier.notify(event)
            except Exception as error:
                logger.warning(
                    "notifier %s failed for run_key=%d: %s",
                    type(notifier).__name__,
                    event.run_key,
                    error,
                )


class WebhookNotifier(GateNotifier):
    """JSON POST to a webhook URL; fail-open with logged errors."""

    def __init__(self, url: str):
        self._url = url

    def notify(self, event: GateEvent) -> None:
        import json
        import logging
        import urllib.request

        logger = logging.getLogger("dq.gate_events")
        payload = json.dumps(event.to_dict()).encode("utf-8")
        request = urllib.request.Request(
            self._url,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            urllib.request.urlopen(request, timeout=10)
        except Exception as error:
            logger.warning(
                "gate-event webhook delivery failed for run_key=%d: %s",
                event.run_key,
                error,
            )


def notifier_from_config(notifier_config: list) -> GateNotifier:
    """Build a notifier from configuration (fail closed on unknown types)."""
    if not notifier_config:
        return LoggingNotifier()
    notifiers = []
    for entry in notifier_config:
        kind = entry.get("type", None) if isinstance(entry, dict) else None
        if kind == "logging":
            notifiers.append(LoggingNotifier())
        elif kind == "webhook":
            url_env = entry.get("url_env", None)
            if not url_env:
                raise ConfigurationError(
                    "webhook notifier requires a 'url_env' key naming the "
                    "environment variable that holds the webhook URL"
                )
            import os

            url = os.environ.get(url_env, None)
            if not url:
                raise ConfigurationError(
                    f"environment variable {url_env!r} must contain the " "webhook URL"
                )
            notifiers.append(WebhookNotifier(url))
        else:
            raise ConfigurationError(
                f"notifier type {kind!r} is not supported; use 'logging' or "
                "'webhook'"
            )
    if len(notifiers) == 1:
        return notifiers[0]
    return CompositeNotifier(*notifiers)
