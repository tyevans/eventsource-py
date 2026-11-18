"""Cutover execution mixin for CutoverManager."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING
from uuid import UUID

from eventsource.application.migration.cutover_locked import (
    CutoverLockedSequenceMixin,
    StillInCutover,
)
from eventsource.observability import ATTR_TENANT_ID, Tracer
from eventsource.observability.attributes import ATTR_MIGRATION_ID
from eventsource.observability.error_summary import error_summary
from eventsource.ports import Position
from eventsource.ports.exceptions import LockAcquisitionError
from eventsource.ports.locks import migration_lock_key
from eventsource.ports.migration.models import CutoverResult, MigrationConfig

if TYPE_CHECKING:
    from eventsource.application.migration.sync_lag_tracker import SyncLagTracker
    from eventsource.ports.locks import DistributedLock

logger = logging.getLogger(__name__)

ATTR_CUTOVER_MIGRATION_ID = "eventsource.cutover.migration_id"
ATTR_CUTOVER_TIMEOUT_MS = "eventsource.cutover.timeout_ms"
ATTR_TARGET_STORE_ID = "eventsource.cutover.target_store_id"


class CutoverExecutionMixin(CutoverLockedSequenceMixin):
    """Mixin providing cutover execution and locked sequence coordination."""

    _tracer: Tracer
    _enable_tracing: bool
    _lock_manager: DistributedLock
    _lock_acquisition_timeout: float

    async def execute_cutover(
        self, migration_id: UUID, tenant_id: UUID, lag_tracker: SyncLagTracker, target_store_id: str,
        *, config: MigrationConfig | None = None, timeout_ms: float | None = None, since: Position | None = None,
        still_in_cutover: StillInCutover | None = None,
    ) -> CutoverResult:  # fmt: skip
        """Execute the atomic cutover from source to target store with automatic rollback.

        `still_in_cutover`, when given, is awaited twice: once after writes
        pause, before the CUTOVER_PAUSED state is written, and again just before
        routing switches. If it returns False, the cutover stops without
        switching routing and clears the pause and interceptor.
        """
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
                        migration_id, tenant_id, lag_tracker, target_store_id, cfg, eff_timeout, since, still_in_cutover
                    )  # fmt: skip
            except LockAcquisitionError as e:
                summary = error_summary(e)
                logger.warning("Failed to acquire cutover lock for tenant %s: %s", tenant_id, summary)  # fmt: skip
                result = CutoverResult(False, 0.0, error_message=f"Failed to acquire cutover lock: {summary}", rolled_back=False)  # fmt: skip
            return result


__all__ = ["CutoverExecutionMixin"]
