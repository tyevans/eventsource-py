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
    - BULK_COPY: Reads go to source store; writes go through
      DualWriteInterceptor, which is already installed and mirroring to
      target while the copy pass runs
    - DUAL_WRITE: Copy pass is complete; writes keep going through
      DualWriteInterceptor, reads from source
    - CUTOVER_PAUSED: Writes blocked, reads from source
    - MIGRATED: All operations go to target store

Usage:
    >>> from eventsource.application.migration import TenantStoreRouter
    >>>
    >>> router = TenantStoreRouter(default_store, routing_repo)
    >>> router.register_store("dedicated", dedicated_store)
    >>>
    >>> # Operations route based on tenant state
    >>> await router.append(stream, events, ExpectedVersion.exact(0))

See Also:
    - Task: P1-005-tenant-store-router.md
    - FRD: docs/tasks/multi-tenant-live-migration/multi-tenant-live-migration.md
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Sequence
from typing import TYPE_CHECKING
from uuid import UUID

from eventsource.application.migration.write_pause import (
    PauseMetrics,
    WritePausedError,
    WritePauseManager,
)
from eventsource.domain import StreamId
from eventsource.domain.event import DomainEvent
from eventsource.domain.exceptions import EventSourceError
from eventsource.observability import Tracer, create_tracer
from eventsource.observability.attributes import (
    ATTR_AGGREGATE_ID,
    ATTR_AGGREGATE_TYPE,
    ATTR_EVENT_COUNT,
    ATTR_EXPECTED_VERSION,
    ATTR_POSITION,
    ATTR_TENANT_ID,
)
from eventsource.ports import (
    AppendResult,
    CategoryReadOptions,
    EventEnvelope,
    ExpectedVersion,
    FeedReadOptions,
    FullEventStore,
    Position,
    StreamReadOptions,
)
from eventsource.ports.migration.models import TenantMigrationState

if TYPE_CHECKING:
    from eventsource.ports.migration.repositories import TenantRoutingRepository

logger = logging.getLogger(__name__)


class StoreNotFoundError(EventSourceError):
    """
    Raised when a store ID cannot be resolved to a registered store.

    This error indicates that a routing configuration references a store ID
    that has not been registered with the router.

    Attributes:
        store_id: The store ID that could not be found.
    """

    def __init__(self, store_id: str):
        self.store_id = store_id
        super().__init__(f"Store not found: {store_id}")


class TenantStoreRouter:
    """
    `FullEventStore`-shaped wrapper that routes operations by tenant.

    Structural conformance only -- the router satisfies `FullEventStore`
    by having its eight members, not by inheriting from any base class.

    Routes read and write operations to the appropriate store based on:
    - Tenant routing configuration (which store the tenant is on)
    - Migration state (NORMAL, BULK_COPY, DUAL_WRITE, CUTOVER_PAUSED, MIGRATED)

    During migration:
    - NORMAL: Route to configured store
    - BULK_COPY: Route reads to source store; route writes through
      DualWriteInterceptor (installed before the copy pass starts, so
      mirror coverage overlaps the copy with no gap)
    - DUAL_WRITE: Copy pass complete; route writes through
      DualWriteInterceptor
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
        >>> await router.append(stream, events, ExpectedVersion.exact(0))
    """

    def __init__(
        self,
        default_store: FullEventStore,
        routing_repo: TenantRoutingRepository,
        *,
        stores: dict[str, FullEventStore] | None = None,
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
            stores: Dictionary mapping store IDs to FullEventStore instances
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
        self._stores: dict[str, FullEventStore] = stores.copy() if stores else {}
        self._stores[default_store_id] = default_store
        self._write_pause_timeout = write_pause_timeout

        # Write pause coordination using WritePauseManager
        self._write_pause_manager = write_pause_manager or WritePauseManager(
            default_timeout=write_pause_timeout
        )

        # Dual-write interceptors (set during migration)
        self._dual_write_interceptors: dict[UUID, FullEventStore] = {}

    # =========================================================================
    # Store Registry Management
    # =========================================================================

    def register_store(self, store_id: str, store: FullEventStore) -> None:
        """
        Register a store for routing.

        Args:
            store_id: Unique identifier for the store
            store: FullEventStore instance
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

    def get_store(self, store_id: str) -> FullEventStore | None:
        """
        Get a registered store by ID.

        Args:
            store_id: Store identifier

        Returns:
            FullEventStore instance or None if not registered
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
        interceptor: FullEventStore,
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
    # FullEventStore Port Implementation
    # =========================================================================

    async def append(
        self,
        stream: StreamId,
        events: Sequence[DomainEvent],
        expected: ExpectedVersion,
    ) -> AppendResult:
        """
        Append events, routing to the appropriate store.

        Routes based on tenant_id from events and migration state.

        Args:
            stream: Identity of the stream to append to
            events: Events to append
            expected: Optimistic-concurrency expectation

        Returns:
            AppendResult from the routed store

        Raises:
            ValueError: If events list is empty
            OptimisticLockError: If the routed store's version check fails
            WritePausedError: If writes are paused and timeout exceeded
        """
        if not events:
            raise ValueError("Cannot append empty event list")

        tenant_id = self._extract_tenant_id(events)

        with self._tracer.span(
            "eventsource.router.append",
            {
                ATTR_AGGREGATE_ID: str(stream.aggregate_id),
                ATTR_AGGREGATE_TYPE: stream.category,
                ATTR_TENANT_ID: str(tenant_id) if tenant_id else "none",
                ATTR_EVENT_COUNT: len(events),
                ATTR_EXPECTED_VERSION: expected.kind,
            },
        ):
            # Check for write pause
            await self._wait_if_paused(tenant_id)

            # Get routing
            store = await self._get_write_store(tenant_id)

            return await store.append(stream, events, expected)

    async def read_category(
        self,
        category: str,
        options: CategoryReadOptions | None = None,
    ) -> AsyncIterator[EventEnvelope]:
        """
        Read events across all streams in a category.

        Routes to the appropriate store based on `options.tenant_id`
        when one is given, otherwise reads from the default store.

        Args:
            category: The stream category (e.g. 'Order')
            options: Options for reading (tenant, timestamp, limit)

        Yields:
            EventEnvelope instances in the port's category order
            (storage time, position tie-break, `from_timestamp` inclusive)
        """
        opts = options or CategoryReadOptions()

        with self._tracer.span(
            "eventsource.router.read_category",
            {
                ATTR_AGGREGATE_TYPE: category,
                ATTR_TENANT_ID: str(opts.tenant_id) if opts.tenant_id else "all",
            },
        ):
            if opts.tenant_id:
                store = await self._get_read_store(opts.tenant_id)
            else:
                store = self._default_store

            async for envelope in store.read_category(category, opts):
                yield envelope

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

    async def get_stream_version(self, stream: StreamId) -> int:
        """
        Get the current version of a stream in the default store.

        Args:
            stream: Identity of the stream

        Returns:
            Current version (0 if the stream doesn't exist)
        """
        return await self._default_store.get_stream_version(stream)

    async def read_stream(
        self,
        stream: StreamId,
        options: StreamReadOptions | None = None,
    ) -> AsyncIterator[EventEnvelope]:
        """
        Read events from a specific stream in the default store.

        The stream read carries no tenant, so it cannot be routed by
        tenant; it goes to the default store.

        Args:
            stream: Identity of the stream
            options: Options for reading (direction, version range, limit)

        Yields:
            EventEnvelope instances
        """
        with self._tracer.span(
            "eventsource.router.read_stream",
            {"stream_id": stream.render()},
        ):
            async for envelope in self._default_store.read_stream(stream, options):
                yield envelope

    async def read_all(
        self,
        from_position: Position | None = None,
        options: FeedReadOptions | None = None,
    ) -> AsyncIterator[EventEnvelope]:
        """
        Read the global feed from the appropriate store.

        If `options.tenant_id` is provided, routes to that tenant's store.
        Otherwise, reads from the default store. `from_position` must
        belong to whichever store the read resolves to.

        Args:
            from_position: Read strictly after this position; None for the
                start of the feed
            options: Options for reading (tenant, limit)

        Yields:
            EventEnvelope instances in global feed order
        """
        opts = options or FeedReadOptions()

        with self._tracer.span(
            "eventsource.router.read_all",
            {
                ATTR_TENANT_ID: str(opts.tenant_id) if opts.tenant_id else "all",
                ATTR_POSITION: from_position.to_str() if from_position else "start",
            },
        ):
            if opts.tenant_id:
                store = await self._get_read_store(opts.tenant_id)
            else:
                store = self._default_store

            async for envelope in store.read_all(from_position, opts):
                yield envelope

    async def current_position(self) -> Position | None:
        """
        Get the current global-feed position of the DEFAULT store.

        The returned position belongs to the default store and is NOT
        comparable with any other store's positions -- ordering two
        stores' positions raises `PositionForeignError`.

        Returns:
            The default store's latest position, or None if it is empty
        """
        return await self._default_store.current_position()

    # =========================================================================
    # Tenant-Aware Store Resolution
    # =========================================================================

    async def get_store_for_tenant(self, tenant_id: UUID) -> FullEventStore:
        """
        Get the read store for a specific tenant.

        This is a convenience method for external callers who need
        direct access to a tenant's store.

        Args:
            tenant_id: Tenant UUID

        Returns:
            FullEventStore for the tenant
        """
        return await self._get_read_store(tenant_id)

    async def get_write_stores_for_tenant(self, tenant_id: UUID) -> list[FullEventStore]:
        """
        Get all write stores for a tenant during migration.

        During DUAL_WRITE phase, returns both source and target stores.
        Otherwise, returns just the single write store.

        Args:
            tenant_id: Tenant UUID

        Returns:
            List of FullEventStore instances for writing
        """
        routing = await self._routing_repo.get_routing(tenant_id)

        if routing is None:
            return [self._default_store]

        state = routing.migration_state

        if state == TenantMigrationState.DUAL_WRITE:
            # In dual-write, return both stores
            stores: list[FullEventStore] = []
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

    def _extract_tenant_id(self, events: Sequence[DomainEvent]) -> UUID | None:
        """
        Extract tenant_id from events.

        Assumes all events in a batch have the same tenant_id.
        Returns None if no tenant_id is set.

        Args:
            events: The events being appended

        Returns:
            Tenant UUID or None
        """
        if events and events[0].tenant_id:
            return events[0].tenant_id
        return None

    async def _get_write_store(self, tenant_id: UUID | None) -> FullEventStore:
        """
        Get the store for write operations based on tenant and migration state.

        Args:
            tenant_id: Tenant UUID or None

        Returns:
            FullEventStore for writing

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

    async def _get_read_store(self, tenant_id: UUID) -> FullEventStore:
        """
        Get the store for read operations based on tenant and migration state.

        Args:
            tenant_id: Tenant UUID

        Returns:
            FullEventStore for reading
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
