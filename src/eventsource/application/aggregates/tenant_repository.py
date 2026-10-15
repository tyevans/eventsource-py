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
        self._repository, self._require_tenant_context = repository, require_tenant_context
        self._validate_on_save = validate_on_save

    @property
    def repository(self) -> AggregateRepository[TAggregate]:
        return self._repository

    @property
    def aggregate_type(self) -> str:
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
        expected_tenant, agg_name = get_required_tenant(), type(aggregate).__name__
        logger.debug("Validating tenant consistency for %s (expected tenant: %s)", agg_name, expected_tenant)  # fmt: skip
        mismatched_events: list[UUID] = []
        mismatched_tenant: UUID | None = None
        for event in aggregate.uncommitted_events:
            event_tenant = getattr(event, "tenant_id", None)
            if event_tenant is not None and event_tenant != expected_tenant:
                mismatched_events.append(event.event_id)
                if mismatched_tenant is None:
                    mismatched_tenant = event_tenant
        if mismatched_events and mismatched_tenant is not None:
            logger.warning(
                "Tenant mismatch detected for %s: expected %s, got %s. Affected events: %d",
                agg_name, expected_tenant, mismatched_tenant, len(mismatched_events),
            )  # fmt: skip
            raise TenantMismatchError(
                expected=expected_tenant, actual=mismatched_tenant, event_ids=mismatched_events
            )

    async def load(self, aggregate_id: UUID) -> TAggregate:
        """Load aggregate, optionally requiring an active tenant scope (precondition only; ADR 0057)."""
        if self._require_tenant_context:
            tenant_id = get_required_tenant()
            logger.debug("Loading aggregate %s with tenant context: %s", aggregate_id, tenant_id)
        return await self._repository.load(aggregate_id)

    async def get_or_none(self, aggregate_id: UUID) -> TAggregate | None:
        try:
            return await self.load(aggregate_id)
        except AggregateNotFoundError:
            return None

    async def get_or_raise(self, aggregate_id: UUID) -> TAggregate:
        return await self.load(aggregate_id)

    async def load_many(self, aggregate_ids: Iterable[UUID], *, ignore_missing: bool = False) -> dict[UUID, TAggregate]:  # fmt: skip
        results: dict[UUID, TAggregate] = {}
        for agg_id in aggregate_ids:
            if ignore_missing:
                if (aggregate := await self.get_or_none(agg_id)) is not None:
                    results[agg_id] = aggregate
            else:
                results[agg_id] = await self.load(agg_id)
        return results

    async def exists(self, aggregate_id: UUID) -> bool:
        if self._require_tenant_context:
            get_required_tenant()
        return await self._repository.exists(aggregate_id)

    async def get_version(self, aggregate_id: UUID) -> int:
        if self._require_tenant_context:
            get_required_tenant()
        return await self._repository.get_version(aggregate_id)

    async def load_or_create(self, aggregate_id: UUID) -> TAggregate:
        if self._require_tenant_context:
            get_required_tenant()
        return await self._repository.load_or_create(aggregate_id)

    def create_new(self, aggregate_id: UUID) -> TAggregate:
        return self._repository.create_new(aggregate_id)

    async def execute(
        self, aggregate_id: UUID, command: Any, *more_commands: Any, create_if_missing: bool = True
    ) -> TAggregate:
        """Load (or create) an aggregate, execute commands on it, and save with tenant validation."""
        agg = await (self.load_or_create(aggregate_id) if create_if_missing else self.load(aggregate_id))  # fmt: skip
        for cmd in (command, *more_commands):
            if callable(cmd) and not isinstance(cmd, type):
                cmd(agg)
            else:
                runner = getattr(agg, "execute", None) or getattr(agg, "apply_command", None)
                if not callable(runner):
                    raise TypeError(
                        f"{type(agg).__name__} does not support execute(); "
                        "pass a callable (e.g. lambda agg: agg.do_something()) or use "
                        "an aggregate supporting execute(command) or apply_command(command), "
                        "such as DeciderAggregate."
                    )
                runner(cmd)
        await self.save(agg)
        return agg

    def __repr__(self) -> str:
        return (
            f"TenantAwareRepository(repository={type(self._repository).__name__}, "
            f"require_tenant_context={self._require_tenant_context}, "
            f"validate_on_save={self._validate_on_save})"
        )


__all__ = ["TenantAwareRepository"]
