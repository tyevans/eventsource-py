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
from eventsource.domain.tenant_context import get_current_tenant

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
        """
        Create and apply an event with auto-populated aggregate fields.

        This is a convenience method that eliminates repetitive boilerplate
        when creating events in command methods. It automatically sets:

        - aggregate_id from self.aggregate_id
        - aggregate_type from self.aggregate_type
        - aggregate_version from self.get_next_version()
        - tenant_id from tenant_context (if available and not explicitly set)
        - causation_id, correlation_id, actor_id, tenant_id from command (if provided)

        The event is automatically applied to the aggregate after creation.

        Args:
            event_class: The event class to instantiate
            command: Optional DomainCommand to extract provenance from
            **kwargs: Event-specific fields (can override auto-populated fields)

        Returns:
            The created and applied event

        Example:
            Before (manual approach):
                >>> def ship(self, tracking_number: str) -> None:
                ...     if self.state.status != "paid":
                ...         raise ValueError("Cannot ship unpaid order")
                ...     event = OrderShipped(
                ...         aggregate_id=self.aggregate_id,
                ...         aggregate_type=self.aggregate_type,
                ...         aggregate_version=self.get_next_version(),
                ...         tracking_number=tracking_number,
                ...     )
                ...     self.apply_event(event)

            After (with create_event):
                >>> def ship(self, tracking_number: str, cmd: ShipOrder) -> None:
                ...     if self.state.status != "paid":
                ...         raise ValueError("Cannot ship unpaid order")
                ...     self.create_event(OrderShipped, command=cmd, tracking_number=tracking_number)

        Note:
            Explicit kwargs always override auto-populated values.
            Overriding auto-stamped fields (e.g. `aggregate_version`) is an
            escape hatch for tests and migrations — in normal domain code,
            let the aggregate stamp them. Precedence: explicit kwargs > command > tenant context > auto fields.
        """
        self._reject_divergent_aggregate_type(event_class)

        # Start with auto-populated aggregate fields
        event_kwargs: dict[str, Any] = {
            "aggregate_id": self.aggregate_id,
            "aggregate_type": self.aggregate_type,
            "aggregate_version": self.get_next_version(),
        }
        event_kwargs.update(self._provenance_updates(command, kwargs.keys()))
        # User kwargs override auto-populated values
        event_kwargs.update(kwargs)

        # Create and apply the event
        event = event_class(**event_kwargs)
        self._reject_foreign_aggregate_id(event, command)
        self.apply_event(event, is_new=True)

        return event

    def _reject_foreign_aggregate_id(self, event: DomainEvent, command: object) -> None:
        """Raise if the event names an aggregate other than this one.

        Unlike `aggregate_type`, `aggregate_id` is not restamped -- an
        explicitly-supplied id survives to the store, where it decides the
        stream the event lands in. An event emitted here that names another
        aggregate is appended to a stream that disowns it, and no save/load
        round-trip can see it: the emitting aggregate never reads that
        stream, and the named one never receives the event.

        Reading `event.aggregate_id` rather than a per-aggregate declaration
        of what may be targeted is what makes this work for every aggregate
        without opt-in.
        """
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
        """Raise if the event class declares a different aggregate_type.

        The aggregate is the single source for `aggregate_type` (ADR 0046),
        so this value is about to be overwritten. Overwriting it silently
        turns a wrong declaration into a wrong stream category that no
        round-trip test can see; the declaration is either redundant or a
        bug, and both deserve to be said out loud.
        """
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
        """
        Shared stamping semantics for create_event() and DeciderAggregate._stamp().

        Fields listed in explicitly_set are never overwritten. Tenant
        precedence: explicit > DomainCommand.tenant_id > ambient tenant
        context (unconditional fallback regardless of command type).
        Causation/correlation/actor come only from a DomainCommand.
        """
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
        """
        Get tenant ID from the current tenant context, if any is set.

        Returns:
            Tenant ID from context, or None if no tenant context is set
        """
        return get_current_tenant()


__all__ = ["AggregateProvenanceMixin"]
