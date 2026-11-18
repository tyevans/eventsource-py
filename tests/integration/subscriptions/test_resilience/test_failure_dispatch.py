"""Handler failures reach the error handler, the DLQ, and error callbacks.

Both runners (catch-up and live) must hand each failed handler call to the
subscription's error handler before the checkpoint moves past the event.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import pytest

from eventsource.application.subscriptions import SubscriptionConfig, SubscriptionManager
from eventsource.application.subscriptions.error_handling import (
    ErrorHandlingConfig,
    ErrorInfo,
    ErrorSeverity,
)
from tests.integration.subscriptions.conftest import (
    SubTestOrderCreated,
    populate_event_store,
    publish_live_event,
)

if TYPE_CHECKING:
    from eventsource.adapters.memory.bus import InMemoryEventBus
    from eventsource.adapters.memory.checkpoints import InMemoryCheckpointRepository
    from eventsource.adapters.memory.dlq import InMemoryDLQRepository
    from eventsource.adapters.memory.store import InMemoryEventStore
    from eventsource.domain.event import DomainEvent

pytestmark = pytest.mark.asyncio

SUB_NAME = "FailOnOrderNumber"
BAD_ORDER = "ORD-BAD"


class FailOnOrderNumber:
    """Subscriber whose handle() raises for one order number and records the rest."""

    def __init__(self, bad_order_number: str) -> None:
        self.bad_order_number = bad_order_number
        self.delivered: list[str] = []

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [SubTestOrderCreated]

    async def handle(self, event: DomainEvent) -> None:
        assert isinstance(event, SubTestOrderCreated)
        if event.order_number == self.bad_order_number:
            raise ValueError(f"poison event {event.order_number}")
        self.delivered.append(event.order_number)


async def _wait_for_dlq_rows(dlq: InMemoryDLQRepository, count: int, timeout: float = 5.0) -> None:
    async with asyncio.timeout(timeout):
        while len(await dlq.get_failed_events(projection_name=SUB_NAME)) < count:
            await asyncio.sleep(0.01)


def _manager_with_dlq(
    store: InMemoryEventStore,
    bus: InMemoryEventBus,
    checkpoints: InMemoryCheckpointRepository,
    dlq: InMemoryDLQRepository,
    callbacks: list[ErrorInfo],
) -> SubscriptionManager:
    # Notify on MEDIUM so a ValueError (a MEDIUM error by default) reaches callbacks.
    manager = SubscriptionManager(
        store,
        bus,
        checkpoints,
        dlq_repo=dlq,
        error_handling_config=ErrorHandlingConfig(notify_on_severity=ErrorSeverity.MEDIUM),
    )

    async def record(info: ErrorInfo) -> None:
        callbacks.append(info)

    manager.on_error(record)
    return manager


async def test_catchup_handler_failure_writes_one_dlq_row_and_fires_callback(
    in_memory_event_store: InMemoryEventStore,
    in_memory_event_bus: InMemoryEventBus,
    in_memory_checkpoint_repo: InMemoryCheckpointRepository,
    in_memory_dlq_repo: InMemoryDLQRepository,
) -> None:
    """A failing event during catch-up is dead-lettered once and reported to callbacks."""
    await populate_event_store(in_memory_event_store, 3, start_index=0)
    subscriber = FailOnOrderNumber("ORD-00001")
    callbacks: list[ErrorInfo] = []
    manager = _manager_with_dlq(
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        in_memory_dlq_repo,
        callbacks,
    )
    subscription = await manager.subscribe(subscriber, SubscriptionConfig(start_from="beginning"))
    try:
        await manager.start()
        await _wait_for_dlq_rows(in_memory_dlq_repo, 1)
        await asyncio.sleep(0.1)  # no second row may appear after the first

        dlq_rows = await in_memory_dlq_repo.get_failed_events(projection_name=SUB_NAME)
        assert len(dlq_rows) == 1
        assert [info.event_type for info in callbacks] == ["SubTestOrderCreated"]
        assert subscriber.delivered == ["ORD-00000", "ORD-00002"]
        assert subscription.events_failed == 1
        assert subscription.events_processed == 2
    finally:
        await manager.stop()


async def test_live_handler_failure_writes_one_dlq_row_and_fires_callback(
    in_memory_event_store: InMemoryEventStore,
    in_memory_event_bus: InMemoryEventBus,
    in_memory_checkpoint_repo: InMemoryCheckpointRepository,
    in_memory_dlq_repo: InMemoryDLQRepository,
) -> None:
    """A failing event on the live path is dead-lettered once and reported to callbacks."""
    subscriber = FailOnOrderNumber(BAD_ORDER)
    callbacks: list[ErrorInfo] = []
    manager = _manager_with_dlq(
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        in_memory_dlq_repo,
        callbacks,
    )
    subscription = await manager.subscribe(subscriber, SubscriptionConfig(start_from="end"))
    try:
        await manager.start()
        await publish_live_event(in_memory_event_store, in_memory_event_bus, "ORD-LIVE-0")
        await publish_live_event(in_memory_event_store, in_memory_event_bus, BAD_ORDER)
        await publish_live_event(in_memory_event_store, in_memory_event_bus, "ORD-LIVE-2")
        await _wait_for_dlq_rows(in_memory_dlq_repo, 1)
        await asyncio.sleep(0.1)  # no second row may appear after the first

        dlq_rows = await in_memory_dlq_repo.get_failed_events(projection_name=SUB_NAME)
        assert len(dlq_rows) == 1
        assert len(callbacks) == 1
        assert subscriber.delivered == ["ORD-LIVE-0", "ORD-LIVE-2"]
        assert subscription.events_failed == 1
        assert subscription.events_processed == 2
    finally:
        await manager.stop()
