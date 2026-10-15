"""Shared fixtures, sample events, and helper projection classes for projection base unit tests."""

from typing import Any
from uuid import UUID

from pydantic import Field

from eventsource.adapters.memory.checkpoints import InMemoryCheckpointRepository
from eventsource.adapters.sql.projection import DatabaseProjection
from eventsource.application.projections.base import (
    CheckpointTrackingProjection,
    DeclarativeProjection,
)
from eventsource.domain.decorators import handles
from eventsource.domain.event import DomainEvent


class OrderCreated(DomainEvent):
    aggregate_type: str = "Order"
    order_number: str = Field(..., description="Order number")


class OrderShipped(DomainEvent):
    aggregate_type: str = "Order"
    tracking_number: str = Field(..., description="Tracking number")


class OrderCancelled(DomainEvent):
    aggregate_type: str = "Order"
    reason: str = Field(..., description="Cancellation reason")


class CheckpointTestProjection(CheckpointTrackingProjection):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.events_processed: list[DomainEvent] = []

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [OrderCreated]

    async def _process_event(self, event: DomainEvent) -> None:
        self.events_processed.append(event)


class TestProjection(CheckpointTestProjection):
    __test__ = False


class FailingCheckpointProjection(CheckpointTrackingProjection):
    MAX_RETRIES = 3
    RETRY_BACKOFF_BASE = 0

    def __init__(
        self,
        *args: Any,
        max_retries: int | None = None,
        error_message: str = "Processing failed",
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.MAX_RETRIES = max_retries if max_retries is not None else self.MAX_RETRIES
        self.error_message, self.attempt_count = error_message, 0

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [OrderCreated]

    async def _process_event(self, event: DomainEvent) -> None:
        self.attempt_count += 1
        raise ValueError(self.error_message)


class FailingProjection(FailingCheckpointProjection):
    pass


class BrokenCheckpoints(InMemoryCheckpointRepository):
    async def update_checkpoint(self, *args: object, **kwargs: object) -> None:
        raise ConnectionError("checkpoint store unreachable")


class CountingProjection(CheckpointTrackingProjection):
    MAX_RETRIES = 2
    RETRY_BACKOFF_BASE = 0

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.applied: list[UUID] = []

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [OrderCreated]

    async def _process_event(self, event: DomainEvent) -> None:
        self.applied.append(event.event_id)


class MyCustomProjection(CheckpointTrackingProjection):
    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [OrderCreated]

    async def _process_event(self, event: DomainEvent) -> None:
        pass


class SimpleDeclarativeProjection(DeclarativeProjection):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.events_handled: list[DomainEvent] = []

    @handles(OrderCreated)
    async def _handle_order_created(self, event: OrderCreated) -> None:
        self.events_handled.append(event)


class MultiHandlerDeclarativeProjection(DeclarativeProjection):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.created_events: list[OrderCreated] = []
        self.shipped_events: list[OrderShipped] = []

    @handles(OrderCreated)
    async def _handle_order_created(self, event: OrderCreated) -> None:
        self.created_events.append(event)

    @handles(OrderShipped)
    async def _handle_order_shipped(self, event: OrderShipped) -> None:
        self.shipped_events.append(event)


class TwoParamDeclarativeProjection(DeclarativeProjection):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.events_handled: list[DomainEvent] = []
        self.conn_values: list[object] = []

    @handles(OrderCreated)
    async def _handle_order_created(self, conn: Any, event: OrderCreated) -> None:
        self.conn_values.append(conn)
        self.events_handled.append(event)


class PublicHandlerDeclarativeProjection(DeclarativeProjection):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.events_handled: list[DomainEvent] = []

    @handles(OrderCreated)
    async def handle_order_created(self, event: OrderCreated) -> None:
        self.events_handled.append(event)


class FailingDeclarativeProjection(DeclarativeProjection):
    MAX_RETRIES = 1
    RETRY_BACKOFF_BASE = 0

    @handles(OrderCreated)
    async def _handle_order_created(self, event: OrderCreated) -> None:
        raise ValueError("Handler error")


class FailingDatabaseProjection(DatabaseProjection):
    MAX_RETRIES = 1
    RETRY_BACKOFF_BASE = 0

    @handles(OrderCreated)
    async def _handle_order_created(self, conn: Any, event: OrderCreated) -> None:
        raise ValueError("Handler error")


class SimpleDatabaseProjection(DatabaseProjection):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.received_conn: list[object] = []
        self.events_handled: list[DomainEvent] = []

    @handles(OrderCreated)
    async def _handle_order_created(self, conn: Any, event: OrderCreated) -> None:
        self.received_conn.append(conn)
        self.events_handled.append(event)


class SingleParamDatabaseProjection(DatabaseProjection):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.events_handled: list[DomainEvent] = []

    @handles(OrderCreated)
    async def _handle_order_created(self, event: OrderCreated) -> None:
        self.events_handled.append(event)


class MultiHandlerDatabaseProjection(DatabaseProjection):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.received_conns: list[object] = []

    @handles(OrderCreated)
    async def _handle_order_created(self, conn: Any, event: OrderCreated) -> None:
        self.received_conns.append(conn)

    @handles(OrderShipped)
    async def _handle_order_shipped(self, conn: Any, event: OrderShipped) -> None:
        self.received_conns.append(conn)


class MixedDatabaseProjection(DatabaseProjection):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.one_param_events: list[DomainEvent] = []
        self.two_param_events: list[DomainEvent] = []
        self.received_conns: list[object] = []

    @handles(OrderCreated)
    async def _handle_order_created(self, conn: Any, event: OrderCreated) -> None:
        self.received_conns.append(conn)
        self.two_param_events.append(event)

    @handles(OrderShipped)
    async def _handle_order_shipped(self, event: OrderShipped) -> None:
        self.one_param_events.append(event)
