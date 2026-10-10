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

from typing import TYPE_CHECKING
from uuid import UUID

from eventsource.application.migration.router_management import RouterManagementMixin
from eventsource.application.migration.router_resolution import RouterResolutionMixin
from eventsource.application.migration.router_store import RouterStoreMixin
from eventsource.application.migration.router_types import (
    PauseMetrics,
    StoreNotFoundError,
    WritePausedError,
    WritePauseManager,
)
from eventsource.observability import Tracer, create_tracer
from eventsource.ports import FullEventStore

if TYPE_CHECKING:
    from eventsource.ports.migration.repositories import TenantRoutingRepository


class TenantStoreRouter(RouterResolutionMixin, RouterManagementMixin, RouterStoreMixin):
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


__all__ = [
    "PauseMetrics",
    "StoreNotFoundError",
    "TenantStoreRouter",
    "WritePauseManager",
    "WritePausedError",
]
