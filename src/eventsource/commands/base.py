"""
Command base model for CQRS-style command handling.

Commands are immutable intents. Unlike events they are never persisted:
a rejected command leaves no trace in the event store by design. There is
no command registry, no serialization support, and no command bus — see
ADR-0022 for the rationale and non-goals.

Subclassing DomainCommand is opt-in. ``DeciderAggregate.execute()`` and
``AggregateRoot.create_event(command=...)`` accept any object as a command;
a DomainCommand additionally gets provenance stamped onto the events it
produces (causation_id, correlation_id, actor_id, tenant_id).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Self
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field

if TYPE_CHECKING:
    from eventsource.events.base import DomainEvent


class DomainCommand(BaseModel):
    """
    Base class for immutable command objects.

    Attributes:
        command_id: Unique identity of this command instance. Becomes the
            causation_id of every event the command produces.
        issued_at: When the command was issued (UTC).
        correlation_id: Workflow chain identifier. Fresh commands start a
            new chain; saga-issued commands should use caused_by() to
            continue an existing one.
        actor_id: Optional identifier of who issued the command.
        tenant_id: Optional tenant. When unset, stamping falls back to the
            tenant context (see DeciderAggregate.execute / create_event).
    """

    model_config = ConfigDict(frozen=True)

    command_id: UUID = Field(default_factory=uuid4)
    issued_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    correlation_id: UUID = Field(default_factory=uuid4)
    actor_id: str | None = Field(default=None)
    tenant_id: UUID | None = Field(default=None)

    def caused_by(self, event: DomainEvent) -> Self:
        """
        Return a copy of this command that continues the event's workflow.

        Copies only the event's correlation_id. Commands deliberately have
        no causation_id field — event -> command -> event linkage within a
        workflow is by correlation (ADR-0022).
        """
        return self.model_copy(update={"correlation_id": event.correlation_id})
