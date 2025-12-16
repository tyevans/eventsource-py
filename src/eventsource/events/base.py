"""
Base classes for domain events.

Events are immutable records of things that have happened in the system.
They are the source of truth in event sourcing architecture.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any, ClassVar, Self
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, model_validator

logger = logging.getLogger(__name__)


class DomainEvent(BaseModel):
    """
    Base class for all domain events with automatic event_type derivation.

    Events are immutable and capture everything needed to reconstruct state.
    All fields except event-specific payload are included in this base class.

    The event_type field is automatically set to the class name if not
    explicitly provided. This reduces boilerplate while maintaining
    compatibility with existing code.

    Auto-derivation behavior:
        - If event_type is not defined or is empty, uses class name
        - If event_type differs from class name, logs a warning
        - Warning can be suppressed with suppress_event_type_warning = True

    Attributes:
        event_id: Unique identifier for this event instance
        event_type: Type name of the event (auto-derived from class name if not set)
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
        suppress_event_type_warning: Class variable to suppress mismatch warning

    Example without explicit event_type (auto-derived):
        >>> from uuid import uuid4
        >>>
        >>> class OrderCreated(DomainEvent):
        ...     aggregate_type: str = "Order"
        ...     order_number: str
        ...     customer_id: UUID
        ...
        >>> event = OrderCreated(
        ...     aggregate_id=uuid4(),
        ...     order_number="ORD-001",
        ...     customer_id=uuid4(),
        ... )
        >>> assert event.event_type == "OrderCreated"

    Example with explicit event_type (backward compatible):
        >>> class OrderCreated(DomainEvent):
        ...     event_type: str = "order_created_v2"
        ...     aggregate_type: str = "Order"
        ...     suppress_event_type_warning = True  # Silence mismatch warning
        ...     order_number: str
        ...     customer_id: UUID
        ...
        >>> event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001", customer_id=uuid4())
        >>> assert event.event_type == "order_created_v2"
    """

    model_config = ConfigDict(frozen=True)

    # Class variable to suppress mismatch warning when event_type differs from class name
    suppress_event_type_warning: ClassVar[bool] = False

    # Event metadata
    event_id: UUID = Field(
        default_factory=uuid4,
        description="Unique event identifier",
    )
    event_type: str = Field(
        default="",
        description="Type of event (auto-derived from class name if not set)",
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

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """
        Hook called when DomainEvent is subclassed.

        Auto-derives event_type from class name if not explicitly set.
        Logs warning if explicit event_type differs from class name
        (can be suppressed with suppress_event_type_warning = True).

        This method runs at class definition time and modifies the Field
        default for event_type to be the class name, eliminating the need
        to manually specify event_type = "ClassName" in every event class.
        """
        super().__init_subclass__(**kwargs)

        # Check if event_type was explicitly set in this class (not inherited)
        # We look at __annotations__ and class __dict__ to detect if the field
        # was declared in this specific class
        explicit_type: str | None = None
        has_explicit_type = False

        # Check if event_type is in this class's own __dict__
        # This catches both: event_type: str = "SomeName" and event_type = "SomeName"
        if "event_type" in cls.__dict__:
            value = cls.__dict__["event_type"]
            if isinstance(value, str):
                explicit_type = value
                has_explicit_type = True

        # If explicitly set, check if it matches class name and warn if not
        if has_explicit_type and explicit_type:
            if explicit_type != cls.__name__:
                # Check if warning should be suppressed
                suppress = getattr(cls, "suppress_event_type_warning", False)
                if not suppress:
                    logger.warning(
                        "Event class %s has event_type='%s' which differs from class name. "
                        "This may cause confusion. Set suppress_event_type_warning=True "
                        "to silence this warning.",
                        cls.__name__,
                        explicit_type,
                    )
        elif not has_explicit_type and "event_type" in cls.model_fields:
            # No explicit type set - auto-derive from class name
            # Update the model_fields to set the default
            # Create a new FieldInfo with updated default
            field_info = cls.model_fields["event_type"]
            # We need to update the default value
            # Pydantic v2 stores field info in model_fields
            field_info.default = cls.__name__

    @model_validator(mode="before")
    @classmethod
    def _ensure_event_type(cls, data: Any) -> Any:
        """
        Ensure event_type is set even when constructing from dict.

        This validator runs before Pydantic model construction and
        ensures the event_type field is populated with the class name
        if not provided in the input data AND the field default is empty.

        This handles cases like:
        - OrderCreated.model_validate({"aggregate_id": ..., ...})
        - OrderCreated.from_dict({...})
        - OrderCreated(**data) where data is a dict without event_type

        The validator respects explicit field defaults set by subclasses:
        - If a subclass sets event_type: str = "custom_type", that value is used
        - If no explicit default is set, the class name is used
        """
        if isinstance(data, dict):
            # Only set event_type if:
            # 1. It's not provided in the input data (or is empty string)
            # 2. AND the field default is empty string (meaning auto-derivation should apply)
            provided_event_type = data.get("event_type")
            if not provided_event_type:
                # Check if the field has a non-empty default (explicit type set by subclass)
                field_info = cls.model_fields.get("event_type")
                field_default = field_info.default if field_info else ""
                # Set to class name if:
                # - field default is empty (auto-derivation applies), OR
                # - an empty string was explicitly provided (we replace it)
                if not field_default or provided_event_type == "":
                    data = dict(data)  # Make a copy to avoid modifying input
                    data["event_type"] = cls.__name__
        return data

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

    def with_causation(self, causing_event: DomainEvent) -> Self:
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

    def is_caused_by(self, event: DomainEvent) -> bool:
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

    def is_correlated_with(self, event: DomainEvent) -> bool:
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
