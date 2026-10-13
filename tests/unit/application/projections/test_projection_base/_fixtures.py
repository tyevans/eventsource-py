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


# Sample events for testing
class OrderCreated(DomainEvent):
    """Sample event for testing."""

    __test__ = False
    aggregate_type: str = "Order"
    order_number: str = Field(..., description="Order number")


class OrderShipped(DomainEvent):
    """Sample event for testing."""

    __test__ = False
    aggregate_type: str = "Order"
    tracking_number: str = Field(..., description="Tracking number")


class OrderCancelled(DomainEvent):
    """Sample event for testing - not handled by any projection."""

    __test__ = False
    aggregate_type: str = "Order"
    reason: str = Field(..., description="Cancellation reason")


# Helper projection classes
class CheckpointTestProjection(CheckpointTrackingProjection):
    """Test projection for checkpoint tracking."""

    __test__ = False

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.events_processed: list[DomainEvent] = []

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [OrderCreated]

    async def _process_event(self, event: DomainEvent) -> None:
        self.events_processed.append(event)


class TestProjection(CheckpointTestProjection):
    """Test projection named TestProjection for class-name-based assertions."""

    __test__ = False


class FailingCheckpointProjection(CheckpointTrackingProjection):
    """Test projection that fails on processing with configurable retries."""

    __test__ = False
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
        if max_retries is not None:
            self.MAX_RETRIES = max_retries
        self.error_message = error_message
        self.attempt_count = 0

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [OrderCreated]

    async def _process_event(self, event: DomainEvent) -> None:
        self.attempt_count += 1
        raise ValueError(self.error_message)


class FailingProjection(FailingCheckpointProjection):
    """Test projection named FailingProjection for class-name-based assertions."""

    __test__ = False


class BrokenCheckpoints(InMemoryCheckpointRepository):
    """In-memory checkpoint store that simulates an unreachable database."""

    __test__ = False

    async def update_checkpoint(self, *args: object, **kwargs: object) -> None:
        raise ConnectionError("checkpoint store unreachable")


class CountingProjection(CheckpointTrackingProjection):
    """Test projection that counts event invocations."""

    __test__ = False
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
    """Custom named projection for verifying class name default."""

    __test__ = False

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [OrderCreated]

    async def _process_event(self, event: DomainEvent) -> None:
        pass


class SimpleDeclarativeProjection(DeclarativeProjection):
    """Declarative projection for testing single handler."""

    __test__ = False

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.events_handled: list[DomainEvent] = []

    @handles(OrderCreated)
    async def _handle_order_created(self, event: OrderCreated) -> None:
        self.events_handled.append(event)


class MultiHandlerDeclarativeProjection(DeclarativeProjection):
    """Declarative projection for testing multiple handlers."""

    __test__ = False

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
    """Declarative projection for testing (conn, event) parameters."""

    __test__ = False

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.events_handled: list[DomainEvent] = []
        self.conn_values: list[object] = []

    @handles(OrderCreated)
    async def _handle_order_created(self, conn: Any, event: OrderCreated) -> None:
        self.conn_values.append(conn)
        self.events_handled.append(event)


class PublicHandlerDeclarativeProjection(DeclarativeProjection):
    """Declarative projection for testing public handler method."""

    __test__ = False

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.events_handled: list[DomainEvent] = []

    @handles(OrderCreated)
    async def handle_order_created(self, event: OrderCreated) -> None:
        self.events_handled.append(event)


class FailingDeclarativeProjection(DeclarativeProjection):
    """Declarative projection that raises in handler."""

    __test__ = False
    MAX_RETRIES = 1
    RETRY_BACKOFF_BASE = 0

    @handles(OrderCreated)
    async def _handle_order_created(self, event: OrderCreated) -> None:
        raise ValueError("Handler error")


class FailingDatabaseProjection(DatabaseProjection):
    """Database projection that raises in handler."""

    __test__ = False
    MAX_RETRIES = 1
    RETRY_BACKOFF_BASE = 0

    @handles(OrderCreated)
    async def _handle_order_created(self, conn: Any, event: OrderCreated) -> None:
        raise ValueError("Handler error")


class SimpleDatabaseProjection(DatabaseProjection):
    """Database projection for testing handlers."""

    __test__ = False

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.received_conn: list[object] = []
        self.events_handled: list[DomainEvent] = []

    @handles(OrderCreated)
    async def _handle_order_created(self, conn: Any, event: OrderCreated) -> None:
        self.received_conn.append(conn)
        self.events_handled.append(event)


class SingleParamDatabaseProjection(DatabaseProjection):
    """Database projection with single param handler."""

    __test__ = False

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.events_handled: list[DomainEvent] = []

    @handles(OrderCreated)
    async def _handle_order_created(self, event: OrderCreated) -> None:
        self.events_handled.append(event)


class MultiHandlerDatabaseProjection(DatabaseProjection):
    """Database projection with multiple handlers sharing connection."""

    __test__ = False

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
    """Database projection with mixed handler signatures."""

    __test__ = False

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
