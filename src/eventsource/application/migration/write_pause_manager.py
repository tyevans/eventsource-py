"""
WritePauseManager - Coordinates write pausing during migration cutover.
"""

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
    """
    Manages write pause coordination for tenant migrations.

    Provides thread-safe pause/resume operations with timeout support
    and comprehensive metrics tracking.

    The manager uses asyncio.Event for efficient waiting - writers block
    on the event until it is set (resume is called) or the timeout expires.

    Example:
        >>> manager = WritePauseManager(default_timeout=5.0)
        >>>
        >>> # During cutover
        >>> await manager.pause_writes(tenant_id)
        >>>
        >>> # Writer threads will block
        >>> try:
        ...     await manager.wait_if_paused(tenant_id)
        ... except WritePausedError:
        ...     print("Timeout waiting for pause to end")
        >>>
        >>> # Resume after cutover
        >>> metrics = await manager.resume_writes(tenant_id)
        >>> print(f"Paused for {metrics.duration_ms}ms")

    Thread Safety:
        All operations are protected by an asyncio.Lock to ensure
        consistent state updates in concurrent scenarios.

    Attributes:
        _default_timeout: Default timeout for wait operations.
        _paused_tenants: Map of tenant_id to PauseState.
        _lock: Asyncio lock for thread safety.
        _metrics_history: Recent pause metrics for monitoring.
        _max_history_size: Maximum metrics entries to retain.
    """

    def __init__(
        self,
        *,
        default_timeout: float = 5.0,
        max_history_size: int = 100,
    ) -> None:
        """
        Initialize the write pause manager.

        Args:
            default_timeout: Default timeout in seconds for wait operations.
                Individual wait calls can override this.
            max_history_size: Maximum number of pause metrics to retain
                in history for monitoring.
        """
        self._default_timeout = default_timeout
        self._max_history_size = max_history_size
        self._paused_tenants: dict[UUID, PauseState] = {}
        self._lock = asyncio.Lock()
        self._metrics_history: list[PauseMetrics] = []
        # Track max waiters per tenant for metrics
        self._max_waiters: dict[UUID, int] = {}
        self._total_waiters: dict[UUID, int] = {}

    @property
    def default_timeout(self) -> float:
        """Get the default timeout in seconds."""
        return self._default_timeout

    async def pause_writes(self, tenant_id: UUID) -> bool:
        """
        Pause writes for a tenant.

        After calling this, any calls to wait_if_paused() for this tenant
        will block until resume_writes() is called or timeout expires.

        This operation is idempotent - calling it multiple times for the
        same tenant has no additional effect beyond the first call.

        Args:
            tenant_id: The tenant UUID to pause writes for.

        Returns:
            True if a new pause was created, False if already paused.
        """
        async with self._lock:
            if tenant_id in self._paused_tenants:
                logger.debug(
                    "Tenant %s already paused (idempotent call)",
                    tenant_id,
                )
                return False

            self._paused_tenants[tenant_id] = PauseState()
            self._max_waiters[tenant_id] = 0
            self._total_waiters[tenant_id] = 0

            logger.info(
                "Paused writes for tenant %s",
                tenant_id,
            )
            return True

    async def resume_writes(self, tenant_id: UUID) -> PauseMetrics | None:
        """
        Resume writes for a tenant and return pause metrics.

        Signals all waiting writers to proceed and removes the pause state.
        Returns metrics about the pause duration and waiter counts.

        This operation is idempotent - calling it for a non-paused tenant
        returns None without error.

        Args:
            tenant_id: The tenant UUID to resume writes for.

        Returns:
            PauseMetrics if tenant was paused, None if not paused.
        """
        async with self._lock:
            state = self._paused_tenants.pop(tenant_id, None)

            if state is None:
                logger.debug(
                    "Tenant %s not paused (idempotent resume)",
                    tenant_id,
                )
                return None

            # Calculate metrics before signaling
            end_time = time.perf_counter()
            ended_at = datetime.now(UTC)
            duration_ms = (end_time - state.started_at) * 1000

            max_waiters = self._max_waiters.pop(tenant_id, 0)
            total_waiters = self._total_waiters.pop(tenant_id, 0)

            metrics = PauseMetrics(
                tenant_id=tenant_id,
                duration_ms=duration_ms,
                started_at=state.started_at_utc,
                ended_at=ended_at,
                max_waiters=max_waiters,
                total_waiters=total_waiters,
            )

            # Signal all waiting writers
            state.event.set()

            # Store metrics in history
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
        self,
        tenant_id: UUID | None,
        *,
        timeout: float | None = None,
    ) -> float:
        """
        Wait if writes are paused for a tenant.

        If the tenant is not paused, returns immediately. If paused,
        blocks until resume_writes() is called or timeout expires.

        Args:
            tenant_id: The tenant UUID to check. If None, returns immediately.
            timeout: Timeout in seconds. If None, uses default_timeout.

        Returns:
            Time waited in milliseconds (0 if not paused).

        Raises:
            WritePausedError: If timeout expires while waiting.
        """
        if tenant_id is None:
            return 0.0

        effective_timeout = timeout if timeout is not None else self._default_timeout

        # Fast path: check if paused without full lock
        async with self._lock:
            state = self._paused_tenants.get(tenant_id)
            if state is None:
                return 0.0

            # Increment waiter counts
            state.waiting_count += 1
            self._total_waiters[tenant_id] = self._total_waiters.get(tenant_id, 0) + 1
            current_waiters = state.waiting_count
            if current_waiters > self._max_waiters.get(tenant_id, 0):
                self._max_waiters[tenant_id] = current_waiters

            event = state.event

        # Wait outside the lock to avoid blocking other operations
        start_wait = time.perf_counter()

        try:
            await asyncio.wait_for(
                event.wait(),
                timeout=effective_timeout,
            )
            wait_ms = (time.perf_counter() - start_wait) * 1000
            return wait_ms

        except TimeoutError:
            wait_ms = (time.perf_counter() - start_wait) * 1000
            logger.warning(
                "Write pause timeout for tenant %s (waited %.2fms, timeout %.2fs)",
                tenant_id,
                wait_ms,
                effective_timeout,
            )
            raise WritePausedError(
                tenant_id,
                effective_timeout,
                waited_ms=wait_ms,
            ) from None

        finally:
            # Decrement waiter count
            async with self._lock:
                state = self._paused_tenants.get(tenant_id)
                if state is not None:
                    state.waiting_count = max(0, state.waiting_count - 1)

    def is_paused(self, tenant_id: UUID) -> bool:
        """
        Check if writes are paused for a tenant.

        This is a synchronous check - it does not acquire the lock
        for performance, so the result may be slightly stale.

        Args:
            tenant_id: The tenant UUID to check.

        Returns:
            True if writes are currently paused.
        """
        return tenant_id in self._paused_tenants

    async def get_pause_state(self, tenant_id: UUID) -> dict[str, Any] | None:
        """
        Get detailed pause state for a tenant.

        Returns information about the current pause including duration
        so far and waiter count. Returns None if not paused.

        Args:
            tenant_id: The tenant UUID.

        Returns:
            Dictionary with pause state info, or None if not paused.
        """
        async with self._lock:
            state = self._paused_tenants.get(tenant_id)
            if state is None:
                return None

            current_time = time.perf_counter()
            duration_ms = (current_time - state.started_at) * 1000

            return {
                "tenant_id": str(tenant_id),
                "started_at": state.started_at_utc.isoformat(),
                "duration_ms": duration_ms,
                "waiting_count": state.waiting_count,
                "total_waiters": self._total_waiters.get(tenant_id, 0),
            }

    async def get_all_paused(self) -> list[UUID]:
        """
        Get all currently paused tenant IDs.

        Returns:
            List of tenant UUIDs that are currently paused.
        """
        async with self._lock:
            return list(self._paused_tenants.keys())

    def get_metrics_history(self) -> list[PauseMetrics]:
        """
        Get recent pause metrics history.

        Returns a copy of the metrics history for monitoring and analysis.

        Returns:
            List of recent PauseMetrics instances.
        """
        return list(self._metrics_history)

    async def force_resume_all(self) -> list[PauseMetrics]:
        """
        Force resume all paused tenants.

        This is an emergency operation that resumes all paused tenants.
        Should only be used during system shutdown or error recovery.

        Returns:
            List of PauseMetrics for all resumed tenants.
        """
        async with self._lock:
            tenant_ids = list(self._paused_tenants.keys())

        metrics_list = []
        for tenant_id in tenant_ids:
            metrics = await self.resume_writes(tenant_id)
            if metrics:
                metrics_list.append(metrics)

        if metrics_list:
            logger.warning(
                "Force resumed %d paused tenants",
                len(metrics_list),
            )

        return metrics_list

    async def wait_for_no_waiters(
        self,
        tenant_id: UUID,
        *,
        timeout: float = 1.0,
        poll_interval: float = 0.01,
    ) -> bool:
        """
        Wait for all waiters to complete for a paused tenant.

        This is useful during graceful shutdown or when you need to
        ensure all in-flight operations have resolved.

        Args:
            tenant_id: The tenant UUID.
            timeout: Maximum time to wait in seconds.
            poll_interval: How often to check in seconds.

        Returns:
            True if no waiters remain, False if timeout expired.
        """
        start = time.perf_counter()

        while (time.perf_counter() - start) < timeout:
            async with self._lock:
                state = self._paused_tenants.get(tenant_id)
                if state is None or state.waiting_count == 0:
                    return True

            await asyncio.sleep(poll_interval)

        return False


__all__ = ["WritePauseManager"]
