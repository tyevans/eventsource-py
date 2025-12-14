"""
TenantStoreRouter - Routes operations based on tenant migration state.

The TenantStoreRouter is responsible for directing event store operations
to the appropriate store(s) based on each tenant's current migration state.
It acts as a transparent proxy that enables zero-downtime migrations.

Responsibilities:
    - Route read operations to the appropriate store
    - Route write operations to one or both stores (during dual-write)
    - Maintain store registry for performance
    - Handle routing lookup failures gracefully
    - Support write pause during cutover

Routing Behavior by State:
    - NORMAL: All operations go to configured store
    - BULK_COPY: Reads/writes go to source store
    - DUAL_WRITE: Writes go through DualWriteInterceptor, reads from source
    - CUTOVER_PAUSED: Writes blocked, reads from source
    - MIGRATED: All operations go to target store

Usage:
    >>> from eventsource.migration import TenantStoreRouter
    >>>
    >>> router = TenantStoreRouter(default_store, routing_repo)
    >>> router.register_store("dedicated", dedicated_store)
    >>>
    >>> # Operations route based on tenant state
    >>> await router.append_events(aggregate_id, "Order", events, 0)

See Also:
    - Task: P1-005-tenant-store-router.md
    - FRD: docs/tasks/multi-tenant-live-migration/multi-tenant-live-migration.md
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from datetime import datetime
from typing import TYPE_CHECKING
from uuid import UUID

from eventsource.events.base import DomainEvent
from eventsource.migration.models import TenantMigrationState
from eventsource.migration.write_pause import (
    PauseMetrics,
    WritePausedError,
    WritePauseManager,
)
from eventsource.observability import Tracer, create_tracer
from eventsource.observability.attributes import (
    ATTR_AGGREGATE_ID,
    ATTR_AGGREGATE_TYPE,
    ATTR_EVENT_COUNT,
    ATTR_EXPECTED_VERSION,
    ATTR_FROM_VERSION,
    ATTR_POSITION,
    ATTR_TENANT_ID,
)
from eventsource.stores.interface import (
    AppendResult,
    EventStore,
    EventStream,
    ReadOptions,
    StoredEvent,
)

if TYPE_CHECKING:
    from eventsource.migration.repositories.routing import TenantRoutingRepository

logger = logging.getLogger(__name__)


class StoreNotFoundError(Exception):
    """
    Raised when a store ID cannot be resolved to an EventStore.

    This error indicates that a routing configuration references a store ID
    that has not been registered with the router.

    Attributes:
        store_id: The store ID that could not be found.
    """

    def __init__(self, store_id: str):
        self.store_id = store_id
        super().__init__(f"Store not found: {store_id}")


class TenantStoreRouter(EventStore):
    """
    EventStore implementation that routes operations by tenant.

    Routes read and write operations to the appropriate store based on:
    - Tenant routing configuration (which store the tenant is on)
    - Migration state (NORMAL, BULK_COPY, DUAL_WRITE, CUTOVER_PAUSED, MIGRATED)

    During migration:
    - NORMAL: Route to configured store
    - BULK_COPY: Route reads/writes to source store
    - DUAL_WRITE: Route writes through DualWriteInterceptor
    - CUTOVER_PAUSED: Block writes, await completion
    - MIGRATED: Route to target store

    Example:
        >>> stores = {
        ...     "shared": shared_postgresql_store,
        ...     "dedicated-tenant-a": dedicated_store,
        ... }
        >>> router = TenantStoreRouter(
        ...     default_store=shared_postgresql_store,
        ...     routing_repo=routing_repo,
        ...     stores=stores,
        ... )
        >>>
        >>> # Operations route based on tenant
        >>> await router.append_events(aggregate_id, "Order", events, 0)
    """

    def __init__(
        self,
        default_store: EventStore,
        routing_repo: TenantRoutingRepository,
        stores: dict[str, EventStore] | None = None,
        *,
        default_store_id: str = "default",
        write_pause_timeout: float = 5.0,
        tracer: Tracer | None = None,
        enable_tracing: bool = True,
        write_pause_manager: WritePauseManager | None = None,
    ):
        """
        Initialize the router.

        Args:
            default_store: Default store for tenants without explicit routing
            routing_repo: Repository for routing configuration
            stores: Dictionary mapping store IDs to EventStore instances
            default_store_id: Identifier for the default store
            write_pause_timeout: Max seconds to wait during cutover pause
            tracer: Optional custom Tracer instance.
            enable_tracing: Whether to enable OpenTelemetry tracing
            write_pause_manager: Optional WritePauseManager instance for
                coordinating write pauses. If not provided, a default
                instance will be created.
        """
        # Composition-based tracing (replaces TracingMixin)
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled
        self._default_store = default_store
        self._default_store_id = default_store_id
        self._routing_repo = routing_repo
        self._stores: dict[str, EventStore] = stores.copy() if stores else {}
        self._stores[default_store_id] = default_store
        self._write_pause_timeout = write_pause_timeout

        # Write pause coordination using WritePauseManager
        self._write_pause_manager = write_pause_manager or WritePauseManager(
            default_timeout=write_pause_timeout
        )

        # Dual-write interceptors (set during migration)
        self._dual_write_interceptors: dict[UUID, EventStore] = {}

    # =========================================================================
    # Store Registry Management
    # =========================================================================

    def register_store(self, store_id: str, store: EventStore) -> None:
        """
        Register a store for routing.

        Args:
            store_id: Unique identifier for the store
            store: EventStore instance
        """
        self._stores[store_id] = store
        logger.debug(f"Registered store: {store_id}")

    def unregister_store(self, store_id: str) -> None:
        """
        Unregister a store.

        Args:
            store_id: Store identifier to remove

        Raises:
            ValueError: If attempting to unregister default store
        """
        if store_id == self._default_store_id:
            raise ValueError("Cannot unregister default store")
        self._stores.pop(store_id, None)
        logger.debug(f"Unregistered store: {store_id}")

    def get_store(self, store_id: str) -> EventStore | None:
        """
        Get a registered store by ID.

        Args:
            store_id: Store identifier

        Returns:
            EventStore instance or None if not registered
        """
        return self._stores.get(store_id)

    def list_stores(self) -> list[str]:
        """
        List all registered store IDs.

        Returns:
            List of store identifiers
        """
        return list(self._stores.keys())

    # =========================================================================
    # Dual-Write Interceptor Management
    # =========================================================================

    def set_dual_write_interceptor(
        self,
        tenant_id: UUID,
        interceptor: EventStore,
    ) -> None:
        """
        Set dual-write interceptor for a tenant.

        Called by MigrationCoordinator when entering dual-write phase.

        Args:
            tenant_id: Tenant UUID
            interceptor: DualWriteInterceptor instance
        """
        self._dual_write_interceptors[tenant_id] = interceptor
        logger.debug(f"Set dual-write interceptor for tenant {tenant_id}")

    def clear_dual_write_interceptor(self, tenant_id: UUID) -> None:
        """
        Remove dual-write interceptor for a tenant.

        Called after cutover completes or migration aborts.

        Args:
            tenant_id: Tenant UUID
        """
        self._dual_write_interceptors.pop(tenant_id, None)
        logger.debug(f"Cleared dual-write interceptor for tenant {tenant_id}")

    def has_dual_write_interceptor(self, tenant_id: UUID) -> bool:
        """
        Check if tenant has a dual-write interceptor set.

        Args:
            tenant_id: Tenant UUID

        Returns:
            True if interceptor is set
        """
        return tenant_id in self._dual_write_interceptors

    # =========================================================================
    # Write Pause Management
    # =========================================================================

    async def pause_writes(self, tenant_id: UUID) -> bool:
        """
        Pause writes for a tenant during cutover.

        Writers will block until resume_writes() is called or timeout.
        This operation is idempotent - calling it multiple times for the
        same tenant has no additional effect.

        Args:
            tenant_id: Tenant UUID

        Returns:
            True if a new pause was created, False if already paused.
        """
        return await self._write_pause_manager.pause_writes(tenant_id)

    async def resume_writes(self, tenant_id: UUID) -> PauseMetrics | None:
        """
        Resume writes for a tenant after cutover.

        Unblocks any waiting writers and returns metrics about the pause.

        Args:
            tenant_id: Tenant UUID

        Returns:
            PauseMetrics if tenant was paused, None if not paused.
        """
        return await self._write_pause_manager.resume_writes(tenant_id)

    def is_paused(self, tenant_id: UUID) -> bool:
        """
        Check if writes are paused for a tenant.

        Args:
            tenant_id: Tenant UUID

        Returns:
            True if writes are paused
        """
        return self._write_pause_manager.is_paused(tenant_id)

    @property
    def write_pause_manager(self) -> WritePauseManager:
        """
        Get the WritePauseManager for advanced pause operations.

        Use this for accessing advanced features like:
        - Pause metrics history
        - Detailed pause state
        - Force resume all

        Returns:
            The WritePauseManager instance.
        """
        return self._write_pause_manager

    # =========================================================================
    # EventStore Protocol Implementation
    # =========================================================================

    async def append_events(
        self,
        aggregate_id: UUID,
        aggregate_type: str,
        events: list[DomainEvent],
        expected_version: int,
    ) -> AppendResult:
        """
        Append events, routing to appropriate store.

        Routes based on tenant_id from events and migration state.

        Args:
            aggregate_id: ID of the aggregate
            aggregate_type: Type of aggregate (e.g., 'Order')
            events: Events to append
            expected_version: Expected current version (for optimistic locking)

        Returns:
            AppendResult with success status and new version

        Raises:
            ValueError: If events list is empty
            WritePausedError: If writes are paused and timeout exceeded
        """
        if not events:
            raise ValueError("Cannot append empty event list")

        tenant_id = self._extract_tenant_id(events)

        with self._tracer.span(
            "eventsource.router.append_events",
            {
                ATTR_AGGREGATE_ID: str(aggregate_id),
                ATTR_AGGREGATE_TYPE: aggregate_type,
                ATTR_TENANT_ID: str(tenant_id) if tenant_id else "none",
                ATTR_EVENT_COUNT: len(events),
                ATTR_EXPECTED_VERSION: expected_version,
            },
        ):
            # Check for write pause
            await self._wait_if_paused(tenant_id)

            # Get routing
            store = await self._get_write_store(tenant_id)

            return await store.append_events(
                aggregate_id,
                aggregate_type,
                events,
                expected_version,
            )

    async def get_events(
        self,
        aggregate_id: UUID,
        aggregate_type: str | None = None,
        from_version: int = 0,
        from_timestamp: datetime | None = None,
        to_timestamp: datetime | None = None,
    ) -> EventStream:
        """
        Get all events for an aggregate from appropriate store.

        For tenant-aware routing, the aggregate must have at least one
        event in the store for tenant detection. If not found in default
        store, routes based on aggregate lookup.

        Args:
            aggregate_id: ID of the aggregate
            aggregate_type: Type of aggregate (optional)
            from_version: Start from this version (default: 0)
            from_timestamp: Only get events after this timestamp
            to_timestamp: Only get events before this timestamp

        Returns:
            EventStream containing the aggregate's events
        """
        with self._tracer.span(
            "eventsource.router.get_events",
            {
                ATTR_AGGREGATE_ID: str(aggregate_id),
                ATTR_AGGREGATE_TYPE: aggregate_type or "any",
                ATTR_FROM_VERSION: from_version,
            },
        ):
            # For reads without tenant context, use default store
            # The router needs tenant_id to make routing decisions
            store = self._default_store

            return await store.get_events(
                aggregate_id,
                aggregate_type,
                from_version,
                from_timestamp,
                to_timestamp,
            )

    async def get_events_by_type(
        self,
        aggregate_type: str,
        tenant_id: UUID | None = None,
        from_timestamp: datetime | None = None,
    ) -> list[DomainEvent]:
        """
        Get all events for a specific aggregate type.

        Routes to appropriate store based on tenant_id if provided.

        Args:
            aggregate_type: Type of aggregate (e.g., 'Order')
            tenant_id: Filter by tenant ID (optional)
            from_timestamp: Only get events after this timestamp

        Returns:
            List of events in chronological order
        """
        with self._tracer.span(
            "eventsource.router.get_events_by_type",
            {
                ATTR_AGGREGATE_TYPE: aggregate_type,
                ATTR_TENANT_ID: str(tenant_id) if tenant_id else "all",
            },
        ):
            if tenant_id:
                store = await self._get_read_store(tenant_id)
            else:
                store = self._default_store

            return await store.get_events_by_type(
                aggregate_type,
                tenant_id,
                from_timestamp,
            )

    async def event_exists(self, event_id: UUID) -> bool:
        """
        Check if an event exists in any registered store.

        Checks default store first (most common case), then other stores.

        Args:
            event_id: ID of the event to check

        Returns:
            True if event exists in any store
        """
        with self._tracer.span(
            "eventsource.router.event_exists",
            {},
        ):
            # Check default store first (most common)
            if await self._default_store.event_exists(event_id):
                return True

            # Check other stores
            for store_id, store in self._stores.items():
                if store_id == self._default_store_id:
                    continue
                if await store.event_exists(event_id):
                    return True

            return False

    async def get_stream_version(
        self,
        aggregate_id: UUID,
        aggregate_type: str,
    ) -> int:
        """
        Get the current version of an aggregate.

        Args:
            aggregate_id: ID of the aggregate
            aggregate_type: Type of aggregate

        Returns:
            Current version (0 if aggregate doesn't exist)
        """
        return await self._default_store.get_stream_version(
            aggregate_id,
            aggregate_type,
        )

    async def read_stream(
        self,
        stream_id: str,
        options: ReadOptions | None = None,
    ) -> AsyncIterator[StoredEvent]:
        """
        Read events from a specific stream.

        Routes to appropriate store based on tenant_id in options.

        Args:
            stream_id: The stream identifier (format: "aggregate_id:aggregate_type")
            options: Options for reading (direction, limit, etc.)

        Yields:
            StoredEvent instances with position metadata
        """
        options = options or ReadOptions()

        with self._tracer.span(
            "eventsource.router.read_stream",
            {
                "stream_id": stream_id,
                ATTR_POSITION: options.from_position,
            },
        ):
            if options.tenant_id:
                store = await self._get_read_store(options.tenant_id)
            else:
                store = self._default_store

            async for event in store.read_stream(stream_id, options):
                yield event

    async def read_all(
        self,
        options: ReadOptions | None = None,
    ) -> AsyncIterator[StoredEvent]:
        """
        Read all events from appropriate store.

        If options.tenant_id is provided, routes to that tenant's store.
        Otherwise, reads from default store.

        Args:
            options: Options for reading (direction, limit, tenant_id, etc.)

        Yields:
            StoredEvent instances with global position metadata
        """
        options = options or ReadOptions()

        with self._tracer.span(
            "eventsource.router.read_all",
            {
                ATTR_TENANT_ID: str(options.tenant_id) if options.tenant_id else "all",
                ATTR_POSITION: options.from_position,
            },
        ):
            if options.tenant_id:
                store = await self._get_read_store(options.tenant_id)
            else:
                store = self._default_store

            async for event in store.read_all(options):
                yield event

    async def get_global_position(self) -> int:
        """
        Get global position from default store.

        Note: Each store has its own position sequence.

        Returns:
            The maximum global position in the default store
        """
        return await self._default_store.get_global_position()

    # =========================================================================
    # Tenant-Aware Store Resolution
    # =========================================================================

    async def get_store_for_tenant(self, tenant_id: UUID) -> EventStore:
        """
        Get the read store for a specific tenant.

        This is a convenience method for external callers who need
        direct access to a tenant's store.

        Args:
            tenant_id: Tenant UUID

        Returns:
            EventStore for the tenant
        """
        return await self._get_read_store(tenant_id)

    async def get_write_stores_for_tenant(self, tenant_id: UUID) -> list[EventStore]:
        """
        Get all write stores for a tenant during migration.

        During DUAL_WRITE phase, returns both source and target stores.
        Otherwise, returns just the single write store.

        Args:
            tenant_id: Tenant UUID

        Returns:
            List of EventStore instances for writing
        """
        routing = await self._routing_repo.get_routing(tenant_id)

        if routing is None:
            return [self._default_store]

        state = routing.migration_state

        if state == TenantMigrationState.DUAL_WRITE:
            # In dual-write, return both stores
            stores = []
            source_store = self._stores.get(routing.store_id, self._default_store)
            stores.append(source_store)

            if routing.target_store_id:
                target_store = self._stores.get(routing.target_store_id)
                if target_store:
                    stores.append(target_store)

            return stores

        # Otherwise, return single write store
        write_store = await self._get_write_store(tenant_id)
        return [write_store]

    # =========================================================================
    # Helper Methods
    # =========================================================================

    def _extract_tenant_id(self, events: list[DomainEvent]) -> UUID | None:
        """
        Extract tenant_id from events.

        Assumes all events in a batch have the same tenant_id.
        Returns None if no tenant_id is set.

        Args:
            events: List of domain events

        Returns:
            Tenant UUID or None
        """
        if events and events[0].tenant_id:
            return events[0].tenant_id
        return None

    async def _get_write_store(self, tenant_id: UUID | None) -> EventStore:
        """
        Get the store for write operations based on tenant and migration state.

        Args:
            tenant_id: Tenant UUID or None

        Returns:
            EventStore for writing

        Raises:
            WritePausedError: If tenant is in CUTOVER_PAUSED state
        """
        if tenant_id is None:
            return self._default_store

        # Check for dual-write interceptor first
        if tenant_id in self._dual_write_interceptors:
            return self._dual_write_interceptors[tenant_id]

        # Get routing configuration
        routing = await self._routing_repo.get_routing(tenant_id)

        if routing is None:
            return self._default_store

        # Route based on migration state
        state = routing.migration_state

        if state == TenantMigrationState.NORMAL:
            return self._stores.get(routing.store_id, self._default_store)

        elif state == TenantMigrationState.BULK_COPY:
            # During bulk copy, writes still go to source
            return self._stores.get(routing.store_id, self._default_store)

        elif state == TenantMigrationState.DUAL_WRITE:
            # Should have interceptor set; fall back to source if not
            interceptor = self._dual_write_interceptors.get(tenant_id)
            if interceptor:
                return interceptor
            logger.warning(f"Dual-write state but no interceptor for tenant {tenant_id}")
            return self._stores.get(routing.store_id, self._default_store)

        elif state == TenantMigrationState.CUTOVER_PAUSED:
            # Writes should be paused; this shouldn't be reached normally
            # as _wait_if_paused should have blocked
            raise WritePausedError(tenant_id, self._write_pause_timeout)

        elif state == TenantMigrationState.MIGRATED:
            # Route to new store (which is now the store_id after cutover)
            return self._stores.get(routing.store_id, self._default_store)

        return self._default_store

    async def _get_read_store(self, tenant_id: UUID) -> EventStore:
        """
        Get the store for read operations based on tenant and migration state.

        Args:
            tenant_id: Tenant UUID

        Returns:
            EventStore for reading
        """
        routing = await self._routing_repo.get_routing(tenant_id)

        if routing is None:
            return self._default_store

        # During migration phases, reads go to source until cutover completes
        state = routing.migration_state

        if state in (
            TenantMigrationState.NORMAL,
            TenantMigrationState.BULK_COPY,
            TenantMigrationState.DUAL_WRITE,
            TenantMigrationState.CUTOVER_PAUSED,
        ):
            return self._stores.get(routing.store_id, self._default_store)

        elif state == TenantMigrationState.MIGRATED:
            # After migration, store_id has been updated to target
            return self._stores.get(routing.store_id, self._default_store)

        return self._default_store

    async def _wait_if_paused(self, tenant_id: UUID | None) -> None:
        """
        Wait if writes are paused for this tenant.

        Delegates to WritePauseManager for efficient waiting with
        metrics tracking.

        Args:
            tenant_id: Tenant UUID or None

        Raises:
            WritePausedError: If timeout exceeded while waiting
        """
        # WritePauseManager handles None tenant_id gracefully
        await self._write_pause_manager.wait_if_paused(tenant_id)


__all__ = [
    "TenantStoreRouter",
    "WritePausedError",
    "StoreNotFoundError",
    "PauseMetrics",
    "WritePauseManager",
]
