"""Fixtures for resilience integration tests."""

from __future__ import annotations

from collections.abc import AsyncGenerator

import pytest_asyncio

from eventsource.adapters.memory.bus import InMemoryEventBus
from eventsource.adapters.memory.checkpoints import InMemoryCheckpointRepository
from eventsource.adapters.memory.dlq import InMemoryDLQRepository
from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.subscriptions import SubscriptionManager


@pytest_asyncio.fixture
async def in_memory_dlq_repo() -> AsyncGenerator[InMemoryDLQRepository]:
    """Create an in-memory DLQ repository for testing."""
    repo = InMemoryDLQRepository(enable_tracing=False)
    yield repo
    await repo.clear()


@pytest_asyncio.fixture
async def subscription_manager_with_dlq(
    in_memory_event_store: InMemoryEventStore,
    in_memory_event_bus: InMemoryEventBus,
    in_memory_checkpoint_repo: InMemoryCheckpointRepository,
    in_memory_dlq_repo: InMemoryDLQRepository,
) -> AsyncGenerator[SubscriptionManager]:
    """Create a subscription manager with DLQ support for testing."""
    manager = SubscriptionManager(
        event_store=in_memory_event_store,
        event_bus=in_memory_event_bus,
        checkpoint_repo=in_memory_checkpoint_repo,
        dlq_repo=in_memory_dlq_repo,
    )
    yield manager
    if manager.is_running:
        await manager.stop()
