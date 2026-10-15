"""
Tenant-aware repository wrapper for multi-tenant applications.

This module provides TenantAwareRepository, a wrapper class that enforces
tenant isolation on the *write* path by validating tenant consistency on save.
It provides no read isolation; see ADR 0018 and ADR 0057.
"""

from __future__ import annotations

import logging
from typing import Any
from uuid import UUID

from eventsource.application.aggregates.repository import AggregateRepository
from eventsource.domain.aggregate import AggregateRoot
from eventsource.domain.exceptions import AggregateNotFoundError, TenantMismatchError
from eventsource.domain.tenant_context import get_required_tenant

logger = logging.getLogger(__name__)


class TenantAwareRepository[TAggregate: AggregateRoot[Any]]:
    """Repository wrapper that enforces tenant isolation on the write path.

    Wraps an existing AggregateRepository to add tenant validation:
    - On save(): Validates all uncommitted events have matching tenant_id.
    - On read: Optionally requires a tenant context to be set
      (if require_tenant_context=True).

    Read isolation is NOT provided (ADR 0018, ADR 0057):
    ``require_tenant_context=True`` asserts only that *a* tenant scope is
    active; it does not compare that scope against the aggregate being read
    or filter events. Enforce read isolation at the storage layer (RLS or
    schema/database per tenant).
    """

    def __init__(
        self,
        repository: AggregateRepository[TAggregate],
        *,
        require_tenant_context: bool = False,
        validate_on_save: bool = True,
    ) -> None:
        """Initialize the tenant-aware wrapper.

        Args:
            repository: The underlying aggregate repository to wrap.
            require_tenant_context: If True, ``load()``, ``load_or_create()``,
                and ``exists()`` raise when no tenant scope is active
                (precondition check only; see ADR 0057). Default False.
            validate_on_save: If True (default), validate that all uncommitted
                events have the matching tenant_id before saving.
        """
        self._repository = repository
        self._require_tenant_context = require_tenant_context
        self._validate_on_save = validate_on_save

    @property
    def repository(self) -> AggregateRepository[TAggregate]:
        """Get the underlying AggregateRepository instance."""
        return self._repository

    @property
    def aggregate_type(self) -> str:
        """Get the aggregate type from the underlying repository."""
        return self._repository.aggregate_type

    async def save(self, aggregate: TAggregate) -> None:
        """Save aggregate, validating tenant consistency when enabled.

        Raises:
            TenantContextNotSetError: If no tenant context is set and validation is enabled.
            TenantMismatchError: If any uncommitted event has a mismatched tenant_id.
        """
        if self._validate_on_save:
            self._validate_tenant_consistency(aggregate)

        await self._repository.save(aggregate)

    def _validate_tenant_consistency(self, aggregate: TAggregate) -> None:
        """Validate all uncommitted events match current tenant context.

        Events without a tenant_id field are skipped (allowed).
        """
        expected_tenant = get_required_tenant()

        logger.debug(
            "Validating tenant consistency for %s (expected tenant: %s)",
            type(aggregate).__name__,
            expected_tenant,
        )

        mismatched_events: list[UUID] = []
        mismatched_tenant: UUID | None = None

        for event in aggregate.uncommitted_events:
            event_tenant = getattr(event, "tenant_id", None)
            if event_tenant is None:
                continue

            if event_tenant != expected_tenant:
                mismatched_events.append(event.event_id)
                if mismatched_tenant is None:
                    mismatched_tenant = event_tenant

        if mismatched_events and mismatched_tenant is not None:
            logger.warning(
                "Tenant mismatch detected for %s: expected %s, got %s. Affected events: %d",
                type(aggregate).__name__,
                expected_tenant,
                mismatched_tenant,
                len(mismatched_events),
            )
            raise TenantMismatchError(
                expected=expected_tenant,
                actual=mismatched_tenant,
                event_ids=mismatched_events,
            )

    async def load(self, aggregate_id: UUID) -> TAggregate:
        """Load aggregate, optionally requiring an active tenant scope (precondition only; ADR 0057)."""
        if self._require_tenant_context:
            tenant_id = get_required_tenant()
            logger.debug(
                "Loading aggregate %s with tenant context: %s",
                aggregate_id,
                tenant_id,
            )

        return await self._repository.load(aggregate_id)

    async def get_or_none(self, aggregate_id: UUID) -> TAggregate | None:
        """Get aggregate if it exists, or return None if not found."""
        try:
            return await self.load(aggregate_id)
        except AggregateNotFoundError:
            return None

    async def exists(self, aggregate_id: UUID) -> bool:
        """Check if aggregate exists, optionally requiring an active tenant scope."""
        if self._require_tenant_context:
            get_required_tenant()

        return await self._repository.exists(aggregate_id)

    async def load_or_create(self, aggregate_id: UUID) -> TAggregate:
        """Load an existing aggregate or create a new one."""
        if self._require_tenant_context:
            get_required_tenant()

        return await self._repository.load_or_create(aggregate_id)

    def create_new(self, aggregate_id: UUID) -> TAggregate:
        """Create a new, empty in-memory aggregate instance."""
        return self._repository.create_new(aggregate_id)

    async def execute(
        self,
        aggregate_id: UUID,
        command: Any,
        *,
        create_if_missing: bool = True,
    ) -> TAggregate:
        """
        Load (or create) an aggregate, execute a command on it, and save with
        tenant validation.

        Args:
            aggregate_id: ID of the aggregate to target
            command: Callable ``(TAggregate) -> Any`` or command value to execute
            create_if_missing: If True (default), uses ``load_or_create``;
                if False, uses ``load``.

        Returns:
            The aggregate after command execution and persistence
        """
        if create_if_missing:
            aggregate = await self.load_or_create(aggregate_id)
        else:
            aggregate = await self.load(aggregate_id)

        if callable(command) and not isinstance(command, type):
            command(aggregate)
        else:
            runner = getattr(aggregate, "execute", None)
            if not callable(runner):
                runner = getattr(aggregate, "apply_command", None)
            if not callable(runner):
                raise TypeError(
                    f"{type(aggregate).__name__} does not support execute(); "
                    "pass a callable (e.g. lambda agg: agg.do_something()) or use "
                    "an aggregate supporting execute(command) or apply_command(command), "
                    "such as DeciderAggregate."
                )
            runner(command)

        await self.save(aggregate)
        return aggregate

    def __repr__(self) -> str:
        """Return string representation."""
        return (
            f"TenantAwareRepository("
            f"repository={type(self._repository).__name__}, "
            f"require_tenant_context={self._require_tenant_context}, "
            f"validate_on_save={self._validate_on_save})"
        )


__all__ = ["TenantAwareRepository"]
