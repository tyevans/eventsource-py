"""DatabaseProjection: a checkpoint failure after commit must not re-apply the handler."""

from uuid import uuid4

import pytest
from pydantic import Field
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

from eventsource.adapters.memory.checkpoints import InMemoryCheckpointRepository
from eventsource.adapters.memory.dlq import InMemoryDLQRepository
from eventsource.adapters.sql.projection import DatabaseProjection
from eventsource.application.projections.retry import ExponentialBackoffRetryPolicy
from eventsource.application.subscriptions.retry_types import RetryConfig
from eventsource.domain.decorators import handles
from eventsource.domain.event import DomainEvent


class OrderCreated(DomainEvent):
    aggregate_type: str = "Order"
    order_number: str = Field(..., description="Order number")


class AlwaysFailingCheckpoints(InMemoryCheckpointRepository):
    """Checkpoint store whose write always fails."""

    async def update_checkpoint(self, *args: object, **kwargs: object) -> None:
        raise ConnectionError("checkpoint store unreachable")


class CountingDatabaseProjection(DatabaseProjection):
    """Non-idempotent read model: counts every handler application."""

    def __init__(self, **kwargs: object) -> None:
        super().__init__(**kwargs)  # type: ignore[arg-type]
        self.applied = 0

    @handles(OrderCreated)
    async def _on_order_created(self, event: OrderCreated) -> None:
        self.applied += 1


@pytest.fixture
async def engine() -> AsyncEngine:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    yield engine
    await engine.dispose()


def _retry_policy(max_retries: int = 2) -> ExponentialBackoffRetryPolicy:
    return ExponentialBackoffRetryPolicy(
        config=RetryConfig(
            max_retries=max_retries, initial_delay=0.001, max_delay=0.001, jitter=0.0
        )
    )


async def test_checkpoint_failure_does_not_reapply_handler(engine: AsyncEngine) -> None:
    dlq_repo = InMemoryDLQRepository()
    projection = CountingDatabaseProjection(
        session_factory=async_sessionmaker(engine),
        checkpoint_repo=AlwaysFailingCheckpoints(),
        dlq_repo=dlq_repo,
        retry_policy=_retry_policy(max_retries=2),
    )

    with pytest.raises(ConnectionError, match="checkpoint store unreachable"):
        await projection.handle(OrderCreated(aggregate_id=uuid4(), order_number="ORD-001"))

    # The transaction committed on the first attempt; the handler must not run again.
    assert projection.applied == 1
    # The event was applied, so it must not be recorded as a failed event.
    assert await dlq_repo.get_failed_events() == []


async def test_successful_event_applies_once_and_records_checkpoint(engine: AsyncEngine) -> None:
    checkpoints = InMemoryCheckpointRepository()
    projection = CountingDatabaseProjection(
        session_factory=async_sessionmaker(engine),
        checkpoint_repo=checkpoints,
        retry_policy=_retry_policy(max_retries=2),
    )
    event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-002")

    await projection.handle(event)

    assert projection.applied == 1
    assert await checkpoints.get_checkpoint("CountingDatabaseProjection") == event.event_id
