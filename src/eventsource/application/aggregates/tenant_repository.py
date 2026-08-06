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
from eventsource.domain.exceptions import TenantMismatchError
from eventsource.domain.tenant_context import get_required_tenant

logger = logging.getLogger(__name__)


class TenantAwareRepository[TAggregate: AggregateRoot[Any]]:
    """
    Repository wrapper that enforces tenant isolation.

    Wraps an existing AggregateRepository to add tenant validation:
    - On save(): Validates all uncommitted events have correct tenant_id
    - On read: Optionally requires a tenant context to be set
      (if require_tenant_context=True)

    This wrapper uses the composition pattern, delegating actual persistence
    to the underlying AggregateRepository while adding tenant-specific
    validation logic.

    Read isolation is NOT provided (ADR 0018, ADR 0057):
        ``require_tenant_context=True`` asserts that *a* tenant scope is
        active. It does not compare that scope against the aggregate being
        read, and it does not restrict which events come back. Loading an
        aggregate id belonging to tenant B from inside tenant A's scope
        succeeds and returns B's aggregate. Read isolation has to come from
        the storage layer -- PostgreSQL row-level security, or a separate
        schema or database per tenant.

    Thread Safety:
        The wrapper is thread-safe when the underlying repository is.
        Tenant context is managed per-async-task via ContextVar.

    Example:
        >>> from eventsource import tenant_scope
        >>> from eventsource.application.aggregates.tenant_repository import (
        ...     TenantAwareRepository,
        ... )
        >>> from eventsource.application.aggregates import AggregateRepository
        >>>
        >>> # Create tenant-aware wrapper
        >>> base_repo = AggregateRepository(event_store, OrderAggregate)
        >>> tenant_repo = TenantAwareRepository(base_repo)
        >>>
        >>> # Use with tenant context
        >>> async with tenant_scope(tenant_id):
        ...     order = await tenant_repo.load(order_id)
        ...     order.add_item(item)
        ...     await tenant_repo.save(order)  # Validates tenant consistency

    Attributes:
        _repository: The underlying AggregateRepository
        _require_tenant_context: Whether reads require a tenant context to be set
        _validate_on_save: Whether to validate tenant consistency on save
    """

    def __init__(
        self,
        repository: AggregateRepository[TAggregate],
        *,
        require_tenant_context: bool = False,
        validate_on_save: bool = True,
    ) -> None:
        """
        Initialize the tenant-aware wrapper.

        Args:
            repository: The underlying aggregate repository to wrap
            require_tenant_context: If True, ``load()``, ``load_or_create()``,
                           and ``exists()`` raise when no tenant scope is
                           active. This is a *precondition check only*: the
                           resolved tenant is never compared against the
                           aggregate, and no events are filtered. It does not
                           give you read isolation -- see the class docstring
                           and ADR 0057. Default False.
            validate_on_save: If True (default), validate that all uncommitted
                            events have the correct tenant_id before saving.

        Example:
            >>> # Default: validate on save, no context required on reads
            >>> tenant_repo = TenantAwareRepository(order_repo)
            >>>
            >>> # Strict mode: require a scope on reads, validate on writes
            >>> strict_repo = TenantAwareRepository(
            ...     order_repo,
            ...     require_tenant_context=True,
            ...     validate_on_save=True,
            ... )
            >>>
            >>> # Relaxed mode: no validation (useful during migration)
            >>> relaxed_repo = TenantAwareRepository(
            ...     order_repo,
            ...     validate_on_save=False,
            ... )
        """
        self._repository = repository
        self._require_tenant_context = require_tenant_context
        self._validate_on_save = validate_on_save

    @property
    def repository(self) -> AggregateRepository[TAggregate]:
        """
        Get the underlying repository.

        This allows access to repository-specific features like snapshot
        management or event publishing configuration.

        Returns:
            The wrapped AggregateRepository instance
        """
        return self._repository

    @property
    def aggregate_type(self) -> str:
        """
        Get the aggregate type from the underlying repository.

        Returns:
            The aggregate type string (e.g., "Order")
        """
        return self._repository.aggregate_type

    async def save(self, aggregate: TAggregate) -> None:
        """
        Save aggregate, validating tenant consistency.

        Validates that all uncommitted events have a tenant_id matching
        the current tenant context before delegating to the underlying
        repository.

        Args:
            aggregate: The aggregate to save

        Raises:
            TenantContextNotSetError: If no tenant context is set and
                validation is enabled
            TenantMismatchError: If any event has wrong tenant_id

        Example:
            >>> async with tenant_scope(tenant_id):
            ...     order.ship(tracking_number="TRACK123")
            ...     await tenant_repo.save(order)  # Validates events

        Note:
            If validate_on_save=False, this method simply delegates to
            the underlying repository without any tenant validation.
            Events without a tenant_id field are allowed (not validated).
        """
        if self._validate_on_save:
            self._validate_tenant_consistency(aggregate)

        await self._repository.save(aggregate)

    def _validate_tenant_consistency(self, aggregate: TAggregate) -> None:
        """
        Validate all uncommitted events match current tenant context.

        Checks each uncommitted event's tenant_id against the current
        tenant context. Events without a tenant_id field are skipped
        (allowed), supporting mixed tenant/non-tenant event scenarios.

        Args:
            aggregate: The aggregate with uncommitted events

        Raises:
            TenantContextNotSetError: If no tenant context is set
            TenantMismatchError: If any event has wrong tenant_id
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
            # Get tenant_id from event (may not exist)
            event_tenant = getattr(event, "tenant_id", None)

            # Events without tenant_id are allowed (not validated)
            if event_tenant is None:
                continue

            # Check for mismatch
            if event_tenant != expected_tenant:
                mismatched_events.append(event.event_id)
                # Track the first mismatched tenant for error message
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
        """
        Load aggregate, optionally requiring a tenant context to be set.

        If require_tenant_context=True, raises when no tenant scope is active.
        That is the entire check. The events replayed to rebuild the aggregate
        are NOT filtered by tenant, and the active tenant is never compared
        against the aggregate's own tenant.

        Args:
            aggregate_id: The aggregate's unique identifier

        Returns:
            The loaded aggregate

        Raises:
            TenantContextNotSetError: If require_tenant_context=True and no context
            AggregateNotFoundError: If aggregate doesn't exist

        Example:
            >>> async with tenant_scope(tenant_id):
            ...     order = await tenant_repo.load(order_id)
            ...     print(f"Loaded order: {order.state.order_number}")

        Warning:
            This method never provides read isolation, at either setting.
            Given an aggregate id belonging to another tenant, it returns that
            tenant's aggregate -- with require_tenant_context=True the only
            difference is that it first insists you are inside *some* scope.
            Enforce read isolation at the database level (PostgreSQL RLS, or a
            schema or database per tenant). See ADR 0018 and ADR 0057.
        """
        if self._require_tenant_context:
            # Validate context exists (raises TenantContextNotSetError if not)
            tenant_id = get_required_tenant()
            logger.debug(
                "Loading aggregate %s with tenant context: %s",
                aggregate_id,
                tenant_id,
            )
            # This is deliberately only a precondition check: tenant_id is
            # not passed down and no events are filtered (ADR 0057).

        return await self._repository.load(aggregate_id)

    async def exists(self, aggregate_id: UUID) -> bool:
        """
        Check if aggregate exists.

        If require_tenant_context=True, raises when no tenant scope is active,
        then delegates. The existence check itself is not scoped to a tenant:
        an aggregate belonging to another tenant reports True.

        Args:
            aggregate_id: The aggregate's unique identifier

        Returns:
            True if the aggregate exists in the store, for any tenant

        Raises:
            TenantContextNotSetError: If require_tenant_context=True and no context

        Example:
            >>> async with tenant_scope(tenant_id):
            ...     if await tenant_repo.exists(order_id):
            ...         order = await tenant_repo.load(order_id)
        """
        if self._require_tenant_context:
            # Validate context exists
            get_required_tenant()

        return await self._repository.exists(aggregate_id)

    async def load_or_create(self, aggregate_id: UUID) -> TAggregate:
        """
        Load an existing aggregate or create a new one.

        Delegates to the underlying repository's load_or_create method. If
        require_tenant_context=True, raises first when no tenant scope is
        active; as with load(), nothing is filtered by tenant.

        Args:
            aggregate_id: ID of the aggregate

        Returns:
            Existing aggregate if found, or new empty aggregate

        Raises:
            TenantContextNotSetError: If require_tenant_context=True and no context

        Example:
            >>> async with tenant_scope(tenant_id):
            ...     order = await tenant_repo.load_or_create(order_id)
            ...     if order.version == 0:
            ...         order.create(customer_id=customer_id)
        """
        if self._require_tenant_context:
            get_required_tenant()

        return await self._repository.load_or_create(aggregate_id)

    def create_new(self, aggregate_id: UUID) -> TAggregate:
        """
        Create a new, empty aggregate instance.

        This does not persist anything - it just creates an in-memory
        aggregate. Delegates directly to the underlying repository.

        Args:
            aggregate_id: ID for the new aggregate

        Returns:
            New aggregate instance with version 0

        Example:
            >>> order = tenant_repo.create_new(uuid4())
            >>> async with tenant_scope(tenant_id):
            ...     order.create(customer_id=customer_id, tenant_id=tenant_id)
            ...     await tenant_repo.save(order)
        """
        return self._repository.create_new(aggregate_id)

    def __repr__(self) -> str:
        """Return string representation."""
        return (
            f"TenantAwareRepository("
            f"repository={type(self._repository).__name__}, "
            f"require_tenant_context={self._require_tenant_context}, "
            f"validate_on_save={self._validate_on_save})"
        )


__all__ = ["TenantAwareRepository"]
