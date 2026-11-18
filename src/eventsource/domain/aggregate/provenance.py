"""Provenance and event creation mixin for aggregates."""

from __future__ import annotations

from collections.abc import Collection
from typing import TYPE_CHECKING, Any
from uuid import UUID

from eventsource.domain.aggregate.types import TEvent
from eventsource.domain.command import DomainCommand
from eventsource.domain.event import DomainEvent
from eventsource.domain.exceptions import (
    AggregateIdMismatchError,
    AggregateTypeMismatchError,
)
from eventsource.domain.tenant_context import get_current_tenant, reject_tenant_contradiction

if TYPE_CHECKING:
    from typing import ClassVar


class AggregateProvenanceMixin:
    """Mixin providing event creation, validation, and provenance stamping."""

    if TYPE_CHECKING:

        @property
        def aggregate_id(self) -> UUID: ...

        aggregate_type: ClassVar[str]

        def get_next_version(self) -> int: ...
        def apply_event(self, event: DomainEvent, is_new: bool = True) -> None: ...

    def create_event(
        self,
        event_class: type[TEvent],
        *,
        command: object | None = None,
        **kwargs: Any,
    ) -> TEvent:
        """Create and apply an event with auto-populated aggregate fields and provenance."""
        self._reject_divergent_aggregate_type(event_class)

        event_kwargs: dict[str, Any] = {
            "aggregate_id": self.aggregate_id,
            "aggregate_type": self.aggregate_type,
            "aggregate_version": self.get_next_version(),
        }
        event_kwargs.update(self._provenance_updates(command, kwargs.keys()))
        event_kwargs.update(kwargs)

        event = event_class(**event_kwargs)
        self._reject_foreign_aggregate_id(event, command)
        self._reject_tenant_contradiction(event, command)
        self.apply_event(event, is_new=True)
        return event

    def _reject_tenant_contradiction(self, event: DomainEvent, command: object) -> None:
        """Raise if a command or event tenant contradicts the ambient tenant (PRD-0002).

        Delegates the rule to `reject_tenant_contradiction`, shared with
        `TenantDomainEvent.with_tenant_context`.
        """
        ambient = self._get_tenant_from_context()
        candidates: list[UUID | None] = []
        if isinstance(command, DomainCommand):
            candidates.append(command.tenant_id)
        candidates.append(event.tenant_id)
        for tenant in candidates:
            reject_tenant_contradiction(ambient, tenant, event_ids=[event.event_id])

    def _reject_foreign_aggregate_id(self, event: DomainEvent, command: object) -> None:
        """Raise if the event names an aggregate other than this one."""
        if event.aggregate_id == self.aggregate_id:
            return
        raise AggregateIdMismatchError(
            type(event).__name__,
            event.aggregate_id,
            type(self).__name__,
            self.aggregate_id,
            type(command).__name__ if command is not None else None,
        )

    def _reject_divergent_aggregate_type(self, event_class: type[DomainEvent]) -> None:
        """Raise if the event class declares a different aggregate_type (ADR-0046)."""
        field = event_class.model_fields.get("aggregate_type")
        declared = getattr(field, "default", None) if field is not None else None
        if isinstance(declared, str) and declared and declared != self.aggregate_type:
            raise AggregateTypeMismatchError(
                event_class.__name__,
                declared,
                type(self).__name__,
                self.aggregate_type,
            )

    def _provenance_updates(
        self,
        command: object,
        explicitly_set: Collection[str],
    ) -> dict[str, Any]:
        """Shared stamping semantics for create_event() and DeciderAggregate._stamp()."""
        updates: dict[str, Any] = {}
        if isinstance(command, DomainCommand):
            if "causation_id" not in explicitly_set:
                updates["causation_id"] = command.command_id
            if "correlation_id" not in explicitly_set:
                updates["correlation_id"] = command.correlation_id
            if "actor_id" not in explicitly_set and command.actor_id is not None:
                updates["actor_id"] = command.actor_id
        if "tenant_id" not in explicitly_set:
            tenant: UUID | None = None
            if isinstance(command, DomainCommand) and command.tenant_id is not None:
                tenant = command.tenant_id
            if tenant is None:
                tenant = self._get_tenant_from_context()
            if tenant is not None:
                updates["tenant_id"] = tenant
        return updates

    def _get_tenant_from_context(self) -> UUID | None:
        """Get tenant ID from the current tenant context, if any is set."""
        return get_current_tenant()


__all__ = ["AggregateProvenanceMixin"]
