"""Cutover readiness validation mixin for CutoverManager."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING
from uuid import UUID

from eventsource.observability import ATTR_TENANT_ID, Tracer
from eventsource.ports import Position
from eventsource.ports.locks import migration_lock_key
from eventsource.ports.migration.models import (
    MigrationConfig,
    TenantMigrationState,
)

if TYPE_CHECKING:
    from eventsource.application.migration.sync_lag_tracker import SyncLagTracker
    from eventsource.ports.locks import DistributedLock
    from eventsource.ports.migration.repositories import TenantRoutingRepository

logger = logging.getLogger(__name__)


class CutoverReadinessMixin:
    """Mixin providing cutover readiness pre-checks."""

    _tracer: Tracer
    _lock_manager: DistributedLock
    _routing_repo: TenantRoutingRepository

    async def validate_cutover_readiness(
        self,
        tenant_id: UUID,
        lag_tracker: SyncLagTracker,
        config: MigrationConfig | None = None,
        *,
        since: Position | None = None,
    ) -> tuple[bool, str | None]:
        """
        Validate that conditions are met for cutover to proceed.

        This is a pre-check that can be called before execute_cutover()
        to verify readiness without actually starting the cutover.

        Args:
            tenant_id: Tenant to validate.
            lag_tracker: SyncLagTracker with current lag information.
            config: Optional migration configuration.
            since: The lag tracker's count anchor (see
                `DualWriteInterceptor.safe_lag_anchor`).

        Returns:
            Tuple of (is_ready, error_message).
            If is_ready is True, error_message is None.

        Example:
            >>> ready, error = await cutover.validate_cutover_readiness(
            ...     tenant_id=tenant_id,
            ...     lag_tracker=lag_tracker,
            ... )
            >>> if ready:
            ...     result = await cutover.execute_cutover(...)
            ... else:
            ...     print(f"Not ready: {error}")
        """
        config = config or MigrationConfig()

        with self._tracer.span(
            "eventsource.cutover.validate_readiness",
            {
                ATTR_TENANT_ID: str(tenant_id),
            },
        ):
            # Check 1: Verify sync lag
            await lag_tracker.calculate_lag(since=since)
            lag = lag_tracker.current_lag

            if lag is None:
                return False, "Unable to calculate sync lag"

            if not lag.is_within_threshold(config.cutover_max_lag_events):
                return (
                    False,
                    f"Sync lag too high: {lag.events} events (max {config.cutover_max_lag_events})",
                )

            # Check 2: Verify routing state is DUAL_WRITE
            routing = await self._routing_repo.get_routing(tenant_id)

            if routing is None:
                return False, "No routing configuration found for tenant"

            if routing.migration_state != TenantMigrationState.DUAL_WRITE:
                return (
                    False,
                    f"Invalid migration state: {routing.migration_state.value} (expected DUAL_WRITE)",
                )

            # Check 3: Verify lock is available (non-blocking check)
            lock_key = migration_lock_key(tenant_id, "cutover")
            lock_info = await self._lock_manager.try_acquire(lock_key)

            if lock_info is None:
                return False, "Cutover lock is already held by another process"

            # Release the lock immediately - we just wanted to check availability
            await self._lock_manager.release(lock_key)

            return True, None


__all__ = [
    "CutoverReadinessMixin",
]
