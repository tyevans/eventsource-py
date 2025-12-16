"""
Tenant-aware repository wrapper for multi-tenant applications.

This module provides TenantAwareRepository, a wrapper class that enforces
tenant isolation by validating tenant consistency on save operations.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Generic, TypeVar
from uuid import UUID

from eventsource.aggregates.base import AggregateRoot
from eventsource.aggregates.repository import AggregateRepository
from eventsource.multitenancy.context import get_required_tenant
from eventsource.multitenancy.exceptions import TenantMismatchError

if TYPE_CHECKING:
    from typing import Any

logger = logging.getLogger(__name__)

# Type variable for aggregate root (consistent with aggregates/repository.py)
TAggregate = TypeVar("TAggregate", bound="AggregateRoot[Any]")


class TenantAwareRepository(Generic[TAggregate]):
    """
    Repository wrapper that enforces tenant isolation.

    Wraps an existing AggregateRepository to add tenant validation:
    - On save(): Validates all uncommitted events have correct tenant_id
    - On load(): Optionally requires tenant context (if enforce_on_load=True)

    This wrapper uses the composition pattern, delegating actual persistence
    to the underlying AggregateRepository while adding tenant-specific
    validation logic.

    Thread Safety:
        The wrapper is thread-safe when the underlying repository is.
        Tenant context is managed per-async-task via ContextVar.

    Example:
        >>> from eventsource.multitenancy import TenantAwareRepository, tenant_scope
        >>> from eventsource.aggregates import AggregateRepository
        >>>
        >>> # Create tenant-aware wrapper
        >>> base_repo = AggregateRepository(event_store, OrderAggregate, "Order")
        >>> tenant_repo = TenantAwareRepository(base_repo)
        >>>
        >>> # Use with tenant context
        >>> async with tenant_scope(tenant_id):
        ...     order = await tenant_repo.load(order_id)
        ...     order.add_item(item)
        ...     await tenant_repo.save(order)  # Validates tenant consistency

    Attributes:
        _repository: The underlying AggregateRepository
        _enforce_on_load: Whether to require tenant context on load
        _validate_on_save: Whether to validate tenant consistency on save
    """

    def __init__(
        self,
        repository: AggregateRepository[TAggregate],
        *,
        enforce_on_load: bool = False,
        validate_on_save: bool = True,
    ) -> None:
        """
        Initialize the tenant-aware wrapper.

        Args:
            repository: The underlying aggregate repository to wrap
            enforce_on_load: If True, require tenant context when loading.
                           Currently validates context exists but does not
                           filter events (filtering requires EventStore changes).
                           Default False.
            validate_on_save: If True (default), validate that all uncommitted
                            events have the correct tenant_id before saving.

        Example:
            >>> # Default: validate on save, no enforcement on load
            >>> tenant_repo = TenantAwareRepository(order_repo)
            >>>
            >>> # Strict mode: require context on both load and save
            >>> strict_repo = TenantAwareRepository(
            ...     order_repo,
            ...     enforce_on_load=True,
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
        self._enforce_on_load = enforce_on_load
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
        Load aggregate, optionally requiring tenant context.

        If enforce_on_load=True, validates that a tenant context exists
        before loading. This does not filter events by tenant (which
        would require EventStore changes) but ensures operations occur
        within a proper tenant context.

        Args:
            aggregate_id: The aggregate's unique identifier

        Returns:
            The loaded aggregate

        Raises:
            TenantContextNotSetError: If enforce_on_load=True and no context
            AggregateNotFoundError: If aggregate doesn't exist

        Example:
            >>> async with tenant_scope(tenant_id):
            ...     order = await tenant_repo.load(order_id)
            ...     print(f"Loaded order: {order.state.order_number}")

        Note:
            If enforce_on_load=False (default), all events for the aggregate
            are loaded regardless of tenant. Use this when tenant isolation
            is enforced at the database level (e.g., PostgreSQL RLS).
        """
        if self._enforce_on_load:
            # Validate context exists (raises TenantContextNotSetError if not)
            tenant_id = get_required_tenant()
            logger.debug(
                "Loading aggregate %s with tenant context: %s",
                aggregate_id,
                tenant_id,
            )
            # Note: Actual filtering by tenant requires EventStore changes.
            # This validates the context exists, but doesn't filter events.
            # Future enhancement: Add tenant_id parameter to repository.load()

        return await self._repository.load(aggregate_id)

    async def exists(self, aggregate_id: UUID) -> bool:
        """
        Check if aggregate exists, considering tenant context.

        If enforce_on_load=True, validates that a tenant context exists
        before checking. Otherwise, delegates directly to the underlying
        repository.

        Args:
            aggregate_id: The aggregate's unique identifier

        Returns:
            True if aggregate exists (and within current tenant if enforced)

        Raises:
            TenantContextNotSetError: If enforce_on_load=True and no context

        Example:
            >>> async with tenant_scope(tenant_id):
            ...     if await tenant_repo.exists(order_id):
            ...         order = await tenant_repo.load(order_id)
        """
        if self._enforce_on_load:
            # Validate context exists
            get_required_tenant()

        return await self._repository.exists(aggregate_id)

    async def load_or_create(self, aggregate_id: UUID) -> TAggregate:
        """
        Load an existing aggregate or create a new one.

        Delegates to the underlying repository's load_or_create method,
        with optional tenant context validation.

        Args:
            aggregate_id: ID of the aggregate

        Returns:
            Existing aggregate if found, or new empty aggregate

        Raises:
            TenantContextNotSetError: If enforce_on_load=True and no context

        Example:
            >>> async with tenant_scope(tenant_id):
            ...     order = await tenant_repo.load_or_create(order_id)
            ...     if order.version == 0:
            ...         order.create(customer_id=customer_id)
        """
        if self._enforce_on_load:
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
            f"enforce_on_load={self._enforce_on_load}, "
            f"validate_on_save={self._validate_on_save})"
        )


__all__ = ["TenantAwareRepository"]
