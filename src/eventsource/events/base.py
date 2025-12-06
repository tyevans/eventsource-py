"""
Base classes for domain events.

Events are immutable records of things that have happened in the system.
They are the source of truth in event sourcing architecture.
"""

from datetime import UTC, datetime
from typing import Any, Self
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field


class DomainEvent(BaseModel):
    """
    Base class for all domain events.

    Events are immutable and capture everything needed to reconstruct state.
    All fields except event-specific payload are included in this base class.

    Attributes:
        event_id: Unique identifier for this event instance
        event_type: Type name of the event (e.g., 'OrderCreated')
        event_version: Schema version for this event type (for migrations)
        occurred_at: When the event occurred (UTC timestamp)
        aggregate_id: ID of the aggregate this event belongs to
        aggregate_type: Type of aggregate (e.g., 'Order')
        aggregate_version: Version of aggregate after this event
        tenant_id: Optional tenant ID for multi-tenancy support
        actor_id: User/system that triggered this event
        correlation_id: ID linking related events across aggregates
        causation_id: ID of the event that caused this event
        metadata: Additional event metadata dictionary

    Example:
        >>> from uuid import uuid4
        >>> from pydantic import Field
        >>>
        >>> class OrderCreated(DomainEvent):
        ...     event_type: str = "OrderCreated"
        ...     aggregate_type: str = "Order"
        ...     order_number: str
        ...     customer_id: UUID
        ...
        >>> event = OrderCreated(
        ...     aggregate_id=uuid4(),
        ...     order_number="ORD-001",
        ...     customer_id=uuid4(),
        ... )
    """

    model_config = ConfigDict(frozen=True)

    # Event metadata
    event_id: UUID = Field(
        default_factory=uuid4,
        description="Unique event identifier",
    )
    event_type: str = Field(
        ...,
        description="Type of event (e.g., 'BadgeIssued')",
    )
    event_version: int = Field(
        default=1,
        ge=1,
        description="Event schema version for migrations",
    )
    occurred_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        description="When event occurred (UTC)",
    )

    # Aggregate information
    aggregate_id: UUID = Field(
        ...,
        description="ID of the aggregate this event belongs to",
    )
    aggregate_type: str = Field(
        ...,
        description="Type of aggregate (e.g., 'BadgeIssuance')",
    )
    aggregate_version: int = Field(
        default=1,
        ge=1,
        description="Version of aggregate after this event",
    )

    # Multi-tenancy (optional for library)
    tenant_id: UUID | None = Field(
        default=None,
        description="Tenant this event belongs to (optional)",
    )

    # Actor (who caused the event)
    actor_id: str | None = Field(
        default=None,
        description="User/system that triggered this event",
    )

    # Correlation and causation for event chains
    correlation_id: UUID = Field(
        default_factory=uuid4,
        description="ID linking related events across aggregates",
    )
    causation_id: UUID | None = Field(
        default=None,
        description="ID of the event that caused this event",
    )

    # Additional metadata
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Additional event metadata",
    )

    def __str__(self) -> str:
        """String representation of event."""
        return (
            f"{self.event_type}(event_id={self.event_id}, "
            f"aggregate_id={self.aggregate_id}, "
            f"version={self.aggregate_version})"
        )

    def __repr__(self) -> str:
        """Detailed string representation."""
        return (
            f"{self.__class__.__name__}("
            f"event_id={self.event_id!r}, "
            f"event_type={self.event_type!r}, "
            f"aggregate_id={self.aggregate_id!r}, "
            f"aggregate_type={self.aggregate_type!r}, "
            f"aggregate_version={self.aggregate_version}, "
            f"tenant_id={self.tenant_id!r}, "
            f"occurred_at={self.occurred_at!r})"
        )

    def with_causation(self, causing_event: "DomainEvent") -> Self:
        """
        Create a copy of this event with causation tracking from another event.

        This method is used to establish causal relationships between events,
        which is essential for debugging and understanding event chains in
        distributed systems.

        Args:
            causing_event: The event that caused this event

        Returns:
            New event instance with causation_id and correlation_id set

        Example:
            >>> original_event = OrderCreated(...)
            >>> new_event = PaymentProcessed(...)
            >>> caused_event = new_event.with_causation(original_event)
            >>> assert caused_event.causation_id == original_event.event_id
            >>> assert caused_event.correlation_id == original_event.correlation_id
        """
        return self.model_copy(
            update={
                "causation_id": causing_event.event_id,
                "correlation_id": causing_event.correlation_id,
            }
        )

    def with_metadata(self, **kwargs: Any) -> Self:
        """
        Create a copy of this event with additional metadata.

        This is useful for adding trace context, request IDs, or other
        cross-cutting concerns without modifying the event's core fields.

        Args:
            **kwargs: Key-value pairs to add to metadata

        Returns:
            New event instance with updated metadata

        Example:
            >>> event = OrderCreated(...)
            >>> enriched = event.with_metadata(trace_id="abc123", source="api")
            >>> assert enriched.metadata["trace_id"] == "abc123"
        """
        new_metadata = {**self.metadata, **kwargs}
        return self.model_copy(update={"metadata": new_metadata})

    def with_aggregate_version(self, version: int) -> Self:
        """
        Create a copy of this event with a specific aggregate version.

        This is typically called by the aggregate when recording events
        to set the correct version number.

        Args:
            version: The aggregate version after this event

        Returns:
            New event instance with updated aggregate_version

        Example:
            >>> event = OrderCreated(...)
            >>> versioned = event.with_aggregate_version(5)
            >>> assert versioned.aggregate_version == 5
        """
        return self.model_copy(update={"aggregate_version": version})

    def to_dict(self) -> dict[str, Any]:
        """
        Convert event to dictionary for serialization.

        Uses Pydantic's JSON-compatible serialization mode to ensure
        all values (including UUIDs and datetimes) are JSON-serializable.

        Returns:
            Dictionary representation with all fields JSON-serializable

        Example:
            >>> event = OrderCreated(...)
            >>> data = event.to_dict()
            >>> assert isinstance(data["event_id"], str)  # UUID as string
            >>> assert isinstance(data["occurred_at"], str)  # datetime as ISO string
        """
        return self.model_dump(mode="json")

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Self:
        """
        Create event from dictionary.

        Uses Pydantic's validation to ensure all fields are correctly
        typed and validated.

        Args:
            data: Dictionary with event fields

        Returns:
            Event instance

        Raises:
            ValidationError: If data doesn't match event schema

        Example:
            >>> data = {"event_type": "OrderCreated", "aggregate_id": "...", ...}
            >>> event = OrderCreated.from_dict(data)
        """
        return cls.model_validate(data)

    def is_caused_by(self, event: "DomainEvent") -> bool:
        """
        Check if this event was caused by another event.

        Args:
            event: The potential causing event

        Returns:
            True if this event's causation_id matches the other event's event_id

        Example:
            >>> if caused_event.is_caused_by(original_event):
            ...     print("Event chain detected")
        """
        return self.causation_id == event.event_id

    def is_correlated_with(self, event: "DomainEvent") -> bool:
        """
        Check if this event is correlated with another event.

        Two events are correlated if they share the same correlation_id,
        meaning they are part of the same logical operation or saga.

        Args:
            event: The event to check correlation with

        Returns:
            True if both events share the same correlation_id

        Example:
            >>> if event1.is_correlated_with(event2):
            ...     print("Part of same saga")
        """
        return self.correlation_id == event.correlation_id
