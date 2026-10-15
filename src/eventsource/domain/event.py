"""Base classes for immutable domain events in event sourcing architecture."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any, ClassVar, Self
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, model_validator

from eventsource.domain.stream_id import CATEGORY_PATTERN
from eventsource.domain.types import (
    AggregateId,
    CausationId,
    CorrelationId,
    EventId,
    TenantId,
)

logger = logging.getLogger(__name__)

_CATEGORY_REGEX = CATEGORY_PATTERN


class DomainEvent(BaseModel):
    """
    Base class for all domain events with automatic ``event_type`` derivation.

    Events are immutable and capture everything needed to reconstruct state.
    Declare ``event_type`` explicitly only when the wire name must differ from
    the class name, and set ``suppress_event_type_warning = True`` to silence
    the mismatch warning.
    """

    model_config = ConfigDict(frozen=True, allow_inf_nan=False, extra="forbid")

    suppress_event_type_warning: ClassVar[bool] = False

    event_id: EventId = Field(default_factory=uuid4, description="Unique event identifier")
    event_type: str = Field(
        default="", description="Type of event (auto-derived from class name if not set)"
    )
    event_version: int = Field(default=1, ge=1, description="Event schema version for migrations")
    occurred_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC), description="When event occurred (UTC)"
    )

    aggregate_id: AggregateId = Field(..., description="ID of the aggregate this event belongs to")
    aggregate_type: str = Field(..., description="Type of aggregate (e.g., 'BadgeIssuance')")
    aggregate_version: int = Field(
        default=1, ge=1, description="Version of aggregate after this event"
    )

    tenant_id: TenantId | None = Field(
        default=None, description="Tenant this event belongs to (optional)"
    )
    actor_id: str | None = Field(default=None, description="User/system that triggered this event")
    correlation_id: CorrelationId = Field(
        default_factory=uuid4, description="ID linking related events across aggregates"
    )
    causation_id: CausationId | None = Field(
        default=None, description="ID of the event that caused this event"
    )
    metadata: dict[str, Any] = Field(default_factory=dict, description="Additional event metadata")

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Warn if explicit ``event_type`` differs from class name."""
        super().__init_subclass__(**kwargs)

        explicit_type: str | None = None
        has_explicit_type = False

        if "event_type" in cls.__dict__:
            value = cls.__dict__["event_type"]
            if isinstance(value, str):
                explicit_type = value
                has_explicit_type = True

        if has_explicit_type and explicit_type and explicit_type != cls.__name__:
            suppress = getattr(cls, "suppress_event_type_warning", False)
            if not suppress:
                logger.warning(
                    "Event class %s has event_type='%s' which differs from class name. "
                    "This may cause confusion. Set suppress_event_type_warning=True "
                    "to silence this warning.",
                    cls.__name__,
                    explicit_type,
                )

    @model_validator(mode="after")
    def _validate_aggregate_type(self) -> Self:
        # After-validator so class-level defaults (`aggregate_type: str = "Order"`)
        # are checked without enabling validate_default=True across all fields.
        v = self.aggregate_type
        if not _CATEGORY_REGEX.match(v):
            raise ValueError(
                f"aggregate_type {v!r} is not a valid stream category "
                f"(must match {_CATEGORY_REGEX.pattern}); it is used as "
                f"StreamId.category on the event's stream"
            )
        return self

    @classmethod
    def event_type_name(cls) -> str:
        """
        Canonical wire name for this event class.

        Returns a non-empty explicit ``event_type`` field default if declared,
        else the class name. Safe to call only after class creation completes.
        """
        field_info = cls.model_fields.get("event_type")
        if field_info is not None and isinstance(field_info.default, str) and field_info.default:
            return field_info.default
        return cls.__name__

    @model_validator(mode="before")
    @classmethod
    def _ensure_event_type(cls, data: Any) -> Any:
        """Ensure event_type is set when constructing from a dictionary."""
        if isinstance(data, dict) and not data.get("event_type"):
            data = dict(data)
            data["event_type"] = cls.event_type_name()
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
        """Create a copy of this event with causation tracking from another event."""
        return self.model_copy(
            update={
                "causation_id": causing_event.event_id,
                "correlation_id": causing_event.correlation_id,
            }
        )

    def with_metadata(self, **kwargs: Any) -> Self:
        """Create a copy of this event with additional metadata."""
        return self.model_copy(update={"metadata": {**self.metadata, **kwargs}})

    def with_aggregate_version(self, version: int) -> Self:
        """Create a copy of this event with a specific aggregate version."""
        return self.model_copy(update={"aggregate_version": version})

    def to_dict(self) -> dict[str, Any]:
        """Convert event to JSON-serializable dictionary."""
        return self.model_dump(mode="json")

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Self:
        """Create event from dictionary using Pydantic validation."""
        return cls.model_validate(data)

    def is_caused_by(self, event: DomainEvent) -> bool:
        """Return True if this event's causation_id matches the other event's event_id."""
        return self.causation_id == event.event_id

    def is_correlated_with(self, event: DomainEvent) -> bool:
        """Return True if both events share the same correlation_id."""
        return self.correlation_id == event.correlation_id
