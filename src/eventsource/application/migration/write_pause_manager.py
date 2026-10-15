"""WritePauseManager - Coordinates write pausing during migration cutover."""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from eventsource.application.migration.write_pause_types import (
    PauseMetrics,
    PauseState,
    WritePausedError,
)

logger = logging.getLogger(__name__)


class WritePauseManager:
    """Manages asyncio.Lock-protected write pause coordination for tenant migrations."""

    def __init__(self, *, default_timeout: float = 5.0, max_history_size: int = 100) -> None:
        self._default_timeout = default_timeout
        self._max_history_size = max_history_size
        self._paused_tenants: dict[UUID, PauseState] = {}
        self._lock = asyncio.Lock()
        self._metrics_history: list[PauseMetrics] = []
        self._max_waiters: dict[UUID, int] = {}
        self._total_waiters: dict[UUID, int] = {}

    @property
    def default_timeout(self) -> float:
        return self._default_timeout

    async def pause_writes(self, tenant_id: UUID) -> bool:
        """Pause writes for a tenant (idempotent). Returns True if newly paused."""
        async with self._lock:
            if tenant_id in self._paused_tenants:
                logger.debug("Tenant %s already paused (idempotent call)", tenant_id)
                return False
            self._paused_tenants[tenant_id] = PauseState()
            self._max_waiters[tenant_id] = self._total_waiters[tenant_id] = 0
            logger.info("Paused writes for tenant %s", tenant_id)
            return True

    async def resume_writes(self, tenant_id: UUID) -> PauseMetrics | None:
        """Resume writes for a tenant and return PauseMetrics, or None if not paused."""
        async with self._lock:
            if (state := self._paused_tenants.pop(tenant_id, None)) is None:
                logger.debug("Tenant %s not paused (idempotent resume)", tenant_id)
                return None

            duration_ms = (time.perf_counter() - state.started_at) * 1000
            max_waiters = self._max_waiters.pop(tenant_id, 0)
            total_waiters = self._total_waiters.pop(tenant_id, 0)
            metrics = PauseMetrics(
                tenant_id=tenant_id,
                duration_ms=duration_ms,
                started_at=state.started_at_utc,
                ended_at=datetime.now(UTC),
                max_waiters=max_waiters,
                total_waiters=total_waiters,
            )
            state.event.set()
            self._metrics_history.append(metrics)
            if len(self._metrics_history) > self._max_history_size:
                self._metrics_history.pop(0)
            logger.info(
                "Resumed writes for tenant %s (duration=%.2fms, waiters=%d)",
                tenant_id,
                duration_ms,
                total_waiters,
            )
            return metrics

    async def wait_if_paused(
        self, tenant_id: UUID | None, *, timeout: float | None = None
    ) -> float:
        """Wait if writes are paused for a tenant; returns wait time in ms or raises WritePausedError."""
        if tenant_id is None:
            return 0.0
        effective_timeout = timeout if timeout is not None else self._default_timeout
        async with self._lock:
            if (state := self._paused_tenants.get(tenant_id)) is None:
                return 0.0
            state.waiting_count += 1
            self._total_waiters[tenant_id] = self._total_waiters.get(tenant_id, 0) + 1
            if state.waiting_count > self._max_waiters.get(tenant_id, 0):
                self._max_waiters[tenant_id] = state.waiting_count
            event = state.event

        start_wait = time.perf_counter()
        try:
            await asyncio.wait_for(event.wait(), timeout=effective_timeout)
            return (time.perf_counter() - start_wait) * 1000
        except TimeoutError:
            wait_ms = (time.perf_counter() - start_wait) * 1000
            logger.warning(
                "Write pause timeout for tenant %s (waited %.2fms, timeout %.2fs)",
                tenant_id,
                wait_ms,
                effective_timeout,
            )
            raise WritePausedError(tenant_id, effective_timeout, waited_ms=wait_ms) from None
        finally:
            async with self._lock:
                if (state := self._paused_tenants.get(tenant_id)) is not None:
                    state.waiting_count = max(0, state.waiting_count - 1)

    def is_paused(self, tenant_id: UUID) -> bool:
        return tenant_id in self._paused_tenants

    async def get_pause_state(self, tenant_id: UUID) -> dict[str, Any] | None:
        async with self._lock:
            if (state := self._paused_tenants.get(tenant_id)) is None:
                return None
            return {
                "tenant_id": str(tenant_id),
                "started_at": state.started_at_utc.isoformat(),
                "duration_ms": (time.perf_counter() - state.started_at) * 1000,
                "waiting_count": state.waiting_count,
                "total_waiters": self._total_waiters.get(tenant_id, 0),
            }

    async def get_all_paused(self) -> list[UUID]:
        async with self._lock:
            return list(self._paused_tenants.keys())

    def get_metrics_history(self) -> list[PauseMetrics]:
        return list(self._metrics_history)

    async def force_resume_all(self) -> list[PauseMetrics]:
        async with self._lock:
            tenant_ids = list(self._paused_tenants.keys())
        metrics_list = [m for tid in tenant_ids if (m := await self.resume_writes(tid)) is not None]
        if metrics_list:
            logger.warning("Force resumed %d paused tenants", len(metrics_list))
        return metrics_list

    async def wait_for_no_waiters(
        self, tenant_id: UUID, *, timeout: float = 1.0, poll_interval: float = 0.01
    ) -> bool:
        start = time.perf_counter()
        while (time.perf_counter() - start) < timeout:
            async with self._lock:
                state = self._paused_tenants.get(tenant_id)
                if state is None or state.waiting_count == 0:
                    return True
            await asyncio.sleep(poll_interval)
        return False


__all__ = ["WritePauseManager"]
