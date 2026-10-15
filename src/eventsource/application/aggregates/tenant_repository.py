"""Tenant-aware repository wrapper enforcing write-path tenant isolation (ADR 0018, ADR 0057)."""

from __future__ import annotations

import logging
from collections.abc import Iterable
from typing import Any
from uuid import UUID

from eventsource.application.aggregates.repository import AggregateRepository
from eventsource.domain.aggregate import AggregateRoot
from eventsource.domain.exceptions import AggregateNotFoundError, TenantMismatchError
from eventsource.domain.tenant_context import get_required_tenant

logger = logging.getLogger(__name__)


class TenantAwareRepository[TAggregate: AggregateRoot[Any]]:
    """Repository wrapper enforcing tenant isolation on the write path (ADR 0018, ADR 0057)."""

    def __init__(
        self,
        repository: AggregateRepository[TAggregate],
        *,
        require_tenant_context: bool = False,
        validate_on_save: bool = True,
    ) -> None:
        """Initialize the tenant-aware wrapper."""
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
        """Save aggregate, validating tenant consistency when enabled."""
        if self._validate_on_save:
            self._validate_tenant_consistency(aggregate)
        await self._repository.save(aggregate)

    async def save_all(self, *aggregates: TAggregate | Iterable[TAggregate]) -> None:
        """Save multiple aggregates in order, validating tenant consistency for each."""
        for item in aggregates:
            if isinstance(item, Iterable):
                for aggregate in item:
                    await self.save(aggregate)
            else:
                await self.save(item)

    def _validate_tenant_consistency(self, aggregate: TAggregate) -> None:
        """Validate all uncommitted events match current tenant context."""
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
            logger.debug("Loading aggregate %s with tenant context: %s", aggregate_id, tenant_id)
        return await self._repository.load(aggregate_id)

    async def get_or_none(self, aggregate_id: UUID) -> TAggregate | None:
        """Get aggregate if it exists, or return None if not found."""
        try:
            return await self.load(aggregate_id)
        except AggregateNotFoundError:
            return None

    async def load_many(
        self, aggregate_ids: Iterable[UUID], *, ignore_missing: bool = False
    ) -> dict[UUID, TAggregate]:
        """Load multiple aggregates by ID into a mapping keyed by aggregate ID."""
        results: dict[UUID, TAggregate] = {}
        for agg_id in aggregate_ids:
            if ignore_missing:
                aggregate = await self.get_or_none(agg_id)
                if aggregate is not None:
                    results[agg_id] = aggregate
            else:
                results[agg_id] = await self.load(agg_id)
        return results

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
        """Load (or create) an aggregate, execute a command on it, and save with tenant validation."""
        aggregate = (
            await self.load_or_create(aggregate_id)
            if create_if_missing
            else await self.load(aggregate_id)
        )

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
