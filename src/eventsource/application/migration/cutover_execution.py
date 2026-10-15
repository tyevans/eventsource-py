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

    async def _rollback(self, tenant_id: UUID, migration_id: UUID, source_store_id: str | None) -> bool:  # fmt: skip
        raise NotImplementedError

    async def execute_cutover(
        self, migration_id: UUID, tenant_id: UUID, lag_tracker: SyncLagTracker, target_store_id: str,
        *, config: MigrationConfig | None = None, timeout_ms: float | None = None, since: Position | None = None,
    ) -> CutoverResult:  # fmt: skip
        """Execute the atomic cutover from source to target store with automatic rollback."""
        cfg = config or MigrationConfig()
        eff_timeout = timeout_ms if timeout_ms is not None else cfg.cutover_timeout_ms
        attrs = {
            ATTR_CUTOVER_MIGRATION_ID: str(migration_id), ATTR_MIGRATION_ID: str(migration_id),
            ATTR_TENANT_ID: str(tenant_id), ATTR_CUTOVER_TIMEOUT_MS: eff_timeout, ATTR_TARGET_STORE_ID: target_store_id,
        }  # fmt: skip
        with self._tracer.span("eventsource.cutover.execute", attrs):
            logger.info("Starting cutover for migration %s, tenant %s, target store %s", migration_id, tenant_id, target_store_id)  # fmt: skip
            lock_key = migration_lock_key(tenant_id, "cutover")
            try:
                async with self._lock_manager.acquire(lock_key, timeout=self._lock_acquisition_timeout):  # fmt: skip
                    result = await self._execute_cutover_locked(
                        migration_id, tenant_id, lag_tracker, target_store_id, cfg, eff_timeout, since
                    )  # fmt: skip
            except LockAcquisitionError as e:
                logger.warning("Failed to acquire cutover lock for tenant %s: %s", tenant_id, e)
                result = CutoverResult(False, 0.0, error_message=f"Failed to acquire cutover lock: {e}", rolled_back=False)  # fmt: skip
            get_migration_metrics(str(migration_id), str(tenant_id)).record_cutover_duration(
                result.duration_ms, success=result.success
            )
            return result

    async def _execute_cutover_locked(
        self, migration_id: UUID, tenant_id: UUID, lag_tracker: SyncLagTracker, target_store_id: str,
        config: MigrationConfig, timeout_ms: float, since: Position | None,
    ) -> CutoverResult:  # fmt: skip
        start_time, events_synced = time.perf_counter(), 0
        source_routing = await self._routing_repo.get_routing(tenant_id)
        source_store_id = source_routing.store_id if source_routing else None
        try:
            await self._router.pause_writes(tenant_id)
            logger.debug("Paused writes for tenant %s", tenant_id)
            await lag_tracker.calculate_lag(since=since)
            if (lag := lag_tracker.current_lag) is None:
                raise CutoverError("Unable to calculate sync lag", migration_id=migration_id, reason="lag_calculation_failed")  # fmt: skip
            if not lag.is_within_threshold(max_lag := config.cutover_max_lag_events):
                raise CutoverLagError(migration_id, lag.events, max_lag)
            logger.debug("Sync lag within threshold: %d events (max %d)", lag.events, max_lag)
            await self._routing_repo.set_migration_state(tenant_id, TenantMigrationState.CUTOVER_PAUSED, migration_id=migration_id)  # fmt: skip
            if (elapsed_ms := (time.perf_counter() - start_time) * 1000) >= timeout_ms:
                raise CutoverTimeoutError(migration_id, elapsed_ms, timeout_ms)
            if (remaining_ms := max(0.0, timeout_ms - elapsed_ms - 10)) > 0:
                await asyncio.sleep(min(remaining_ms / 1000, 0.010))
            await lag_tracker.calculate_lag(since=since)
            if lag_tracker.current_lag:
                events_synced = max(0, lag.events - lag_tracker.current_lag.events)
            if (elapsed_ms := (time.perf_counter() - start_time) * 1000) >= timeout_ms:
                raise CutoverTimeoutError(migration_id, elapsed_ms, timeout_ms)
            try:
                if (target_store := self._router.get_store(target_store_id)) is not None:
                    await target_store.current_position()
                else:
                    logger.warning("Target store %s not found in router registry", target_store_id)
            except Exception as e:
                raise CutoverError(f"Target store health check failed: {e}", migration_id=migration_id, reason="target_health_check_failed") from e  # fmt: skip

            await self._routing_repo.switch_routing(tenant_id, target_store_id, state=TenantMigrationState.MIGRATED, migration_id=migration_id)  # fmt: skip
            self._router.clear_dual_write_interceptor(tenant_id)
            final_ms = (time.perf_counter() - start_time) * 1000
            logger.info("Cutover completed for tenant %s in %.2fms (synced %d events)", tenant_id, final_ms, events_synced)  # fmt: skip
            return CutoverResult(success=True, duration_ms=final_ms, events_synced=events_synced)
        except Exception as e:
            elapsed = (time.perf_counter() - start_time) * 1000
            duration_ms = e.elapsed_ms if isinstance(e, CutoverTimeoutError) else elapsed
            if isinstance(e, CutoverTimeoutError):
                logger.warning("Cutover timeout for tenant %s: %.2fms exceeded %.2fms", tenant_id, e.elapsed_ms, e.timeout_ms)  # fmt: skip
            elif isinstance(e, CutoverLagError):
                logger.warning("Cutover failed for tenant %s: lag too high (%d > %d)", tenant_id, e.current_lag, e.max_lag)  # fmt: skip
            elif isinstance(e, CutoverError):
                logger.error("Cutover error for tenant %s: %s", tenant_id, e)
            else:
                logger.exception("Unexpected error during cutover for tenant %s", tenant_id)
            error_msg = str(e) if isinstance(e, CutoverError) else f"Unexpected error: {e}"
            rolled_back = await self._rollback(tenant_id, migration_id, source_store_id)
            return CutoverResult(False, duration_ms, events_synced, error_msg, rolled_back)
        finally:
            await self._router.resume_writes(tenant_id)
            logger.debug("Resumed writes for tenant %s", tenant_id)


__all__ = ["CutoverExecutionMixin"]
