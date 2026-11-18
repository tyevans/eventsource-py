"""A failing event must not leak payload values through health, logs, or the DLQ.

Each test runs a real catch-up delivery against in-memory adapters. The subscriber
raises a pydantic ValidationError that echoes the payload marker. The assertions
inspect the public health report, the log records, and the DLQ entry.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from typing import Any
from uuid import uuid4

import pytest
from pydantic import BaseModel, ValidationError

from eventsource.adapters.memory import InMemoryEventBus, InMemoryEventStore
from eventsource.adapters.memory.checkpoints import InMemoryCheckpointRepository
from eventsource.adapters.memory.dlq import InMemoryDLQRepository
from eventsource.application.subscriptions import SubscriptionConfig, SubscriptionManager
from eventsource.application.subscriptions.error_handling_types import (
    ErrorHandlingConfig,
    ErrorHandlingStrategy,
)
from eventsource.domain import StreamId, register_event
from eventsource.domain.event import DomainEvent
from eventsource.ports.handlers import EventSubscriber
from eventsource.ports.positions import ExpectedVersion

MARKER = "victim-secret@example.com"
_ERROR_HANDLER_LOGGER = "eventsource.application.subscriptions.error_handling_dlq"


@register_event
class HealthLedgerPosted(DomainEvent):
    aggregate_type: str = "HealthLedger"
    owner: str = ""


class _Ledger(BaseModel):
    owner_id: str
    account_id: int


class RejectingSubscriber(EventSubscriber):
    """Raises a ValidationError whose message echoes the marker."""

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [HealthLedgerPosted]

    async def handle(self, event: DomainEvent) -> None:
        try:
            _Ledger.model_validate({"owner_id": MARKER, "account_id": MARKER})
        except ValidationError as error:
            raise error
        raise AssertionError("expected a validation error")


async def _run_failing_catchup(
    manager_kwargs: dict[str, Any],
) -> tuple[SubscriptionManager, InMemoryEventStore]:
    store = InMemoryEventStore()
    event = HealthLedgerPosted(aggregate_id=uuid4(), owner=MARKER)
    await store.append(
        StreamId(aggregate_id=event.aggregate_id, category="HealthLedger"),
        [event],
        ExpectedVersion.no_stream(),
    )
    manager = SubscriptionManager(
        event_store=store,
        event_bus=InMemoryEventBus(),
        checkpoint_repo=InMemoryCheckpointRepository(),
        **manager_kwargs,
    )
    await manager.subscribe(
        RejectingSubscriber(),
        config=SubscriptionConfig(start_from="beginning", continue_on_error=True),
        name="Ledger",
    )
    await manager.start()
    return manager, store


async def _wait_until(condition: Callable[[], Any]) -> None:
    for _ in range(500):
        if condition():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("condition not reached before timeout")


def _assert_marker_absent(records: list[logging.LogRecord]) -> None:
    assert records, "expected eventsource log records from the failure path"
    for record in records:
        assert MARKER not in record.getMessage(), record.getMessage()
        assert MARKER not in repr(record.args)
        assert MARKER not in repr(record.__dict__)


async def test_health_report_and_error_logs_hold_only_error_summary(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG, logger="eventsource")
    manager, _ = await _run_failing_catchup({})
    try:
        await _wait_until(lambda: manager.get_comprehensive_health()["recent_errors"]["Ledger"])
        health = manager.get_comprehensive_health()
    finally:
        await manager.stop()

    recent = health["recent_errors"]["Ledger"]
    assert recent[0]["error_type"] == "ValidationError"
    assert recent[0]["error_message"].startswith("ValidationError(")
    assert MARKER not in repr(health)
    handler_records = [r for r in caplog.records if r.name == _ERROR_HANDLER_LOGGER]
    _assert_marker_absent(handler_records)


async def test_every_subscription_log_record_holds_only_error_summary(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG, logger="eventsource")
    manager, _ = await _run_failing_catchup({})
    try:
        await _wait_until(lambda: manager.get_comprehensive_health()["recent_errors"]["Ledger"])
    finally:
        await manager.stop()

    _assert_marker_absent([r for r in caplog.records if r.name.startswith("eventsource")])


async def test_dlq_entry_error_message_holds_only_error_summary() -> None:
    dlq = InMemoryDLQRepository()
    manager, _ = await _run_failing_catchup(
        {
            "dlq_repo": dlq,
            "error_handling_config": ErrorHandlingConfig(strategy=ErrorHandlingStrategy.DLQ_ONLY),
        }
    )
    try:
        await _wait_until(lambda: manager.get_comprehensive_health()["recent_errors"]["Ledger"])
        entries = await dlq.get_failed_events(projection_name="Ledger")
    finally:
        await manager.stop()

    assert len(entries) == 1
    assert "ValidationError(" in entries[0].error_message
    assert MARKER not in entries[0].error_message
