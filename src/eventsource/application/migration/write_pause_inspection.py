"""Read-only inspection and wait helpers for WritePauseManager."""

from __future__ import annotations

import asyncio
import time
from typing import Any
from uuid import UUID

from eventsource.application.migration.write_pause_types import PauseMetrics, PauseState


class WritePauseInspectionMixin:
    """Mixin providing read-only pause state queries and the no-waiters poll."""

    _lock: asyncio.Lock
    _paused_tenants: dict[UUID, PauseState]
    _total_waiters: dict[UUID, int]
    _metrics_history: list[PauseMetrics]

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


__all__ = ["WritePauseInspectionMixin"]
