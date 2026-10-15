"""Cutover execution mixin for CutoverManager."""

from __future__ import annotations

import asyncio
import logging
import time
from typing import TYPE_CHECKING
from uuid import UUID

from eventsource.application.migration.exceptions import (
    CutoverError,
    CutoverLagError,
    CutoverTimeoutError,
)
from eventsource.application.migration.metrics import get_migration_metrics
from eventsource.observability import ATTR_TENANT_ID, Tracer
from eventsource.observability.attributes import ATTR_MIGRATION_ID
from eventsource.ports import Position
from eventsource.ports.exceptions import LockAcquisitionError
from eventsource.ports.locks import migration_lock_key
from eventsource.ports.migration.models import CutoverResult, MigrationConfig, TenantMigrationState

if TYPE_CHECKING:
    from eventsource.application.migration.router import TenantStoreRouter
    from eventsource.application.migration.sync_lag_tracker import SyncLagTracker
    from eventsource.ports.locks import DistributedLock
    from eventsource.ports.migration.repositories import TenantRoutingRepository

logger = logging.getLogger(__name__)

ATTR_CUTOVER_MIGRATION_ID = "eventsource.cutover.migration_id"
ATTR_CUTOVER_TIMEOUT_MS = "eventsource.cutover.timeout_ms"
ATTR_TARGET_STORE_ID = "eventsource.cutover.target_store_id"


class CutoverExecutionMixin:
    """Mixin providing cutover execution and locked sequence coordination."""

    _tracer: Tracer
    _enable_tracing: bool
    _lock_manager: DistributedLock
    _router: TenantStoreRouter
    _routing_repo: TenantRoutingRepository
    _lock_acquisition_timeout: float

    async def _rollback(
        self, tenant_id: UUID, migration_id: UUID, source_store_id: str | None
    ) -> bool:
        raise NotImplementedError

    async def execute_cutover(
        self,
        migration_id: UUID,
        tenant_id: UUID,
        lag_tracker: SyncLagTracker,
        target_store_id: str,
        *,
        config: MigrationConfig | None = None,
        timeout_ms: float | None = None,
        since: Position | None = None,
    ) -> CutoverResult:
        """Execute the atomic cutover from source to target store with automatic rollback."""
        config = config or MigrationConfig()
        effective_timeout_ms = timeout_ms if timeout_ms is not None else config.cutover_timeout_ms
        attrs = {
            ATTR_CUTOVER_MIGRATION_ID: str(migration_id),
            ATTR_MIGRATION_ID: str(migration_id),
            ATTR_TENANT_ID: str(tenant_id),
            ATTR_CUTOVER_TIMEOUT_MS: effective_timeout_ms,
            ATTR_TARGET_STORE_ID: target_store_id,
        }
        with self._tracer.span("eventsource.cutover.execute", attrs):
            logger.info(
                "Starting cutover for migration %s, tenant %s, target store %s",
                migration_id,
                tenant_id,
                target_store_id,
            )
            lock_key = migration_lock_key(tenant_id, "cutover")
            try:
                async with self._lock_manager.acquire(
                    lock_key, timeout=self._lock_acquisition_timeout
                ):
                    args = (migration_id, tenant_id, lag_tracker, target_store_id, config)
                    result = await self._execute_cutover_locked(*args, effective_timeout_ms, since)
            except LockAcquisitionError as e:
                logger.warning("Failed to acquire cutover lock for tenant %s: %s", tenant_id, e)
                msg = f"Failed to acquire cutover lock: {e}"
                result = CutoverResult(
                    success=False, duration_ms=0.0, error_message=msg, rolled_back=False
                )
            get_migration_metrics(str(migration_id), str(tenant_id)).record_cutover_duration(
                result.duration_ms, success=result.success
            )
            return result

    async def _execute_cutover_locked(
        self,
        migration_id: UUID,
        tenant_id: UUID,
        lag_tracker: SyncLagTracker,
        target_store_id: str,
        config: MigrationConfig,
        timeout_ms: float,
        since: Position | None,
    ) -> CutoverResult:
        """Execute the cutover sequence while holding the advisory lock."""
        start_time = time.perf_counter()
        events_synced = 0
        source_routing = await self._routing_repo.get_routing(tenant_id)
        source_store_id = source_routing.store_id if source_routing else None

        try:
            await self._router.pause_writes(tenant_id)
            logger.debug("Paused writes for tenant %s", tenant_id)
            await lag_tracker.calculate_lag(since=since)
            lag = lag_tracker.current_lag
            if lag is None:
                raise CutoverError(
                    "Unable to calculate sync lag",
                    migration_id=migration_id,
                    reason="lag_calculation_failed",
                )
            if not lag.is_within_threshold(config.cutover_max_lag_events):
                raise CutoverLagError(
                    migration_id=migration_id,
                    current_lag=lag.events,
                    max_lag=config.cutover_max_lag_events,
                )
            logger.debug(
                "Sync lag within threshold: %d events (max %d)",
                lag.events,
                config.cutover_max_lag_events,
            )
            await self._routing_repo.set_migration_state(
                tenant_id, TenantMigrationState.CUTOVER_PAUSED, migration_id=migration_id
            )
            elapsed_ms = (time.perf_counter() - start_time) * 1000
            if elapsed_ms >= timeout_ms:
                raise CutoverTimeoutError(
                    migration_id=migration_id, elapsed_ms=elapsed_ms, timeout_ms=timeout_ms
                )
            remaining_ms = max(0, timeout_ms - elapsed_ms - 10)
            if remaining_ms > 0:
                await asyncio.sleep(min(remaining_ms / 1000, 0.010))
            await lag_tracker.calculate_lag(since=since)
            final_lag = lag_tracker.current_lag
            if final_lag:
                events_synced = max(0, lag.events - final_lag.events)
            elapsed_ms = (time.perf_counter() - start_time) * 1000
            if elapsed_ms >= timeout_ms:
                raise CutoverTimeoutError(
                    migration_id=migration_id, elapsed_ms=elapsed_ms, timeout_ms=timeout_ms
                )
            try:
                target_store = self._router.get_store(target_store_id)
                if target_store is not None:
                    await target_store.current_position()
                else:
                    logger.warning("Target store %s not found in router registry", target_store_id)
            except Exception as e:
                raise CutoverError(
                    f"Target store health check failed: {e}",
                    migration_id=migration_id,
                    reason="target_health_check_failed",
                ) from e

            await self._routing_repo.switch_routing(
                tenant_id,
                target_store_id,
                state=TenantMigrationState.MIGRATED,
                migration_id=migration_id,
            )
            self._router.clear_dual_write_interceptor(tenant_id)
            final_elapsed_ms = (time.perf_counter() - start_time) * 1000
            logger.info(
                "Cutover completed for tenant %s in %.2fms (synced %d events)",
                tenant_id,
                final_elapsed_ms,
                events_synced,
            )
            return CutoverResult(
                success=True, duration_ms=final_elapsed_ms, events_synced=events_synced
            )
        except Exception as e:
            duration_ms = (
                e.elapsed_ms
                if isinstance(e, CutoverTimeoutError)
                else (time.perf_counter() - start_time) * 1000
            )
            if isinstance(e, CutoverTimeoutError):
                msg_fmt = "Cutover timeout for tenant %s: %.2fms exceeded %.2fms"
                logger.warning(msg_fmt, tenant_id, e.elapsed_ms, e.timeout_ms)
            elif isinstance(e, CutoverLagError):
                msg_fmt = "Cutover failed for tenant %s: lag too high (%d > %d)"
                logger.warning(msg_fmt, tenant_id, e.current_lag, e.max_lag)
            elif isinstance(e, CutoverError):
                logger.error("Cutover error for tenant %s: %s", tenant_id, e)
            else:
                logger.exception("Unexpected error during cutover for tenant %s", tenant_id)
            error_msg = str(e) if isinstance(e, CutoverError) else f"Unexpected error: {e}"
            rolled_back = await self._rollback(tenant_id, migration_id, source_store_id)
            return CutoverResult(
                success=False,
                duration_ms=duration_ms,
                events_synced=events_synced,
                error_message=error_msg,
                rolled_back=rolled_back,
            )
        finally:
            await self._router.resume_writes(tenant_id)
            logger.debug("Resumed writes for tenant %s", tenant_id)


__all__ = ["CutoverExecutionMixin"]
