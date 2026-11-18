"""Locked cutover sequence (pause, verify lag, switch routing) for CutoverExecutionMixin."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING
from uuid import UUID

from eventsource.application.migration.exceptions import (
    CutoverError,
    CutoverLagError,
    CutoverTimeoutError,
)
from eventsource.observability.error_summary import error_summary
from eventsource.ports import Position
from eventsource.ports.migration.models import CutoverResult, MigrationConfig, TenantMigrationState

if TYPE_CHECKING:
    from eventsource.application.migration.router import TenantStoreRouter
    from eventsource.application.migration.sync_lag_tracker import SyncLagTracker
    from eventsource.ports.migration.repositories import TenantRoutingRepository

# Logger name kept from the module this sequence was split out of: log names are
# part of the telemetry surface, so they do not follow the import path.
logger = logging.getLogger("eventsource.application.migration.cutover_execution")

StillInCutover = Callable[[], Awaitable[bool]]


class _MigrationLeftCutoverError(CutoverError):
    """The migration left CUTOVER (for example, it was aborted) before routing switched."""


class CutoverLockedSequenceMixin:
    """Mixin providing the routing-switch sequence that runs under the cutover lock."""

    _router: TenantStoreRouter
    _routing_repo: TenantRoutingRepository

    async def _rollback(self, tenant_id: UUID, migration_id: UUID, source_store_id: str | None) -> bool:  # fmt: skip
        raise NotImplementedError

    async def _ensure_still_in_cutover(self, still_in_cutover: StillInCutover | None, migration_id: UUID) -> None:  # fmt: skip
        if still_in_cutover is not None and not await still_in_cutover():
            raise _MigrationLeftCutoverError("Migration left CUTOVER before routing switched", migration_id=migration_id, reason="migration_left_cutover")  # fmt: skip

    async def _execute_cutover_locked(
        self, migration_id: UUID, tenant_id: UUID, lag_tracker: SyncLagTracker, target_store_id: str,
        config: MigrationConfig, timeout_ms: float, since: Position | None,
        still_in_cutover: StillInCutover | None = None,
    ) -> CutoverResult:  # fmt: skip
        start_time, events_synced = time.perf_counter(), 0
        source_routing = await self._routing_repo.get_routing(tenant_id)
        source_store_id = source_routing.store_id if source_routing else None
        try:
            await self._router.pause_writes(tenant_id)
            logger.debug("Paused writes for tenant %s", tenant_id)
            await self._ensure_still_in_cutover(still_in_cutover, migration_id)
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
                raise CutoverError(f"Target store health check failed: {error_summary(e)}", migration_id=migration_id, reason="target_health_check_failed") from e  # fmt: skip

            await self._ensure_still_in_cutover(still_in_cutover, migration_id)
            await self._routing_repo.switch_routing(tenant_id, target_store_id, state=TenantMigrationState.MIGRATED, migration_id=migration_id)  # fmt: skip
            self._router.clear_dual_write_interceptor(tenant_id)
            final_ms = (time.perf_counter() - start_time) * 1000
            logger.info("Cutover completed for tenant %s in %.2fms (synced %d events)", tenant_id, final_ms, events_synced)  # fmt: skip
            return CutoverResult(success=True, duration_ms=final_ms, events_synced=events_synced)
        except _MigrationLeftCutoverError as e:
            # The migration was aborted (or otherwise left CUTOVER). Restoring
            # DUAL_WRITE here would resurrect a dead migration, so clear the
            # routing state and interceptor instead of rolling back.
            logger.warning("Cutover stopped for tenant %s: %s", tenant_id, error_summary(e))
            self._router.clear_dual_write_interceptor(tenant_id)
            await self._routing_repo.clear_migration_state(tenant_id)
            elapsed = (time.perf_counter() - start_time) * 1000
            return CutoverResult(False, elapsed, events_synced, str(e), rolled_back=False)
        except Exception as e:
            elapsed = (time.perf_counter() - start_time) * 1000
            duration_ms = e.elapsed_ms if isinstance(e, CutoverTimeoutError) else elapsed
            if isinstance(e, CutoverTimeoutError):
                logger.warning("Cutover timeout for tenant %s: %.2fms exceeded %.2fms", tenant_id, e.elapsed_ms, e.timeout_ms)  # fmt: skip
            elif isinstance(e, CutoverLagError):
                logger.warning("Cutover failed for tenant %s: lag too high (%d > %d)", tenant_id, e.current_lag, e.max_lag)  # fmt: skip
            elif isinstance(e, CutoverError):
                logger.error("Cutover error for tenant %s: %s", tenant_id, error_summary(e))
            else:
                # No exc_info: an unexpected error can come from event reads, and the
                # traceback would repeat its message (which may echo payload values).
                logger.error("Unexpected error during cutover for tenant %s: %s", tenant_id, error_summary(e))  # fmt: skip
            # CutoverError messages are built here from summaries, so str(e) is safe.
            error_msg = str(e) if isinstance(e, CutoverError) else f"Unexpected error: {error_summary(e)}"  # fmt: skip
            rolled_back = await self._rollback(tenant_id, migration_id, source_store_id)
            return CutoverResult(False, duration_ms, events_synced, error_msg, rolled_back)
        finally:
            await self._router.resume_writes(tenant_id)
            logger.debug("Resumed writes for tenant %s", tenant_id)


__all__ = ["CutoverLockedSequenceMixin", "StillInCutover"]
