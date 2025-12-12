"""
WritePauseManager - Coordinates write pausing during migration cutover.

The WritePauseManager provides a robust mechanism for temporarily pausing
writes to a tenant's event store during the critical cutover phase of
migration. It ensures that:

1. Writers block efficiently using asyncio primitives
2. Timeouts prevent indefinite blocking
3. All waiting writers are signaled when pause ends
4. Metrics are tracked for observability
5. Edge cases are handled gracefully

This component is used by TenantStoreRouter to implement the write pause
behavior and by CutoverManager to coordinate the cutover sequence.

Design Principles:
    - Thread-safe through asyncio.Lock
    - Efficient waiting via asyncio.Event
    - Configurable timeouts per operation
    - Idempotent pause/resume operations
    - Comprehensive metrics for observability

Usage:
    >>> from eventsource.migration.write_pause import WritePauseManager
    >>>
    >>> manager = WritePauseManager(default_timeout=5.0)
    >>>
    >>> # Pause writes for a tenant
    >>> await manager.pause_writes(tenant_id)
    >>>
    >>> # Writers will block (with timeout)
    >>> await manager.wait_if_paused(tenant_id, timeout=2.0)
    >>>
    >>> # Resume writes
    >>> metrics = await manager.resume_writes(tenant_id)
    >>> print(f"Pause duration: {metrics.duration_ms}ms")

See Also:
    - Task: P2-004-write-pause-mechanism.md
    - TenantStoreRouter: Uses this for write pause coordination
    - CutoverManager: Orchestrates pause during cutover
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import UUID

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class WritePausedError(Exception):
    """
    Raised when a write operation times out waiting for pause to end.

    This error indicates that a write operation was attempted for a tenant
    whose writes are paused, and the configured timeout was exceeded while
    waiting for the pause to be lifted.

    Attributes:
        tenant_id: The tenant UUID for which writes are paused.
        timeout: The timeout duration in seconds that was exceeded.
        waited_ms: How long the operation actually waited in milliseconds.
    """

    def __init__(
        self,
        tenant_id: UUID,
        timeout: float,
        waited_ms: float | None = None,
    ) -> None:
        """
        Initialize the error.

        Args:
            tenant_id: The tenant UUID for which writes are paused.
            timeout: The timeout duration in seconds.
            waited_ms: How long the operation waited (optional).
        """
        self.tenant_id = tenant_id
        self.timeout = timeout
        self.waited_ms = waited_ms

        waited_info = f" (waited {waited_ms:.2f}ms)" if waited_ms is not None else ""
        super().__init__(
            f"Writes paused for tenant {tenant_id}; timeout after {timeout}s{waited_info}"
        )


@dataclass
class PauseState:
    """
    Internal state for a paused tenant.

    Tracks the pause event, start time, and waiting writer count for
    a single tenant's pause state.

    Attributes:
        event: The asyncio.Event used to signal resume.
        started_at: When the pause began (monotonic time for duration).
        started_at_utc: When the pause began (UTC for logging).
        waiting_count: Number of writers currently waiting.
    """

    event: asyncio.Event = field(default_factory=asyncio.Event)
    started_at: float = field(default_factory=time.perf_counter)
    started_at_utc: datetime = field(default_factory=lambda: datetime.now(UTC))
    waiting_count: int = 0


@dataclass(frozen=True)
class PauseMetrics:
    """
    Metrics for a completed pause operation.

    Captures timing and waiter information for observability and
    performance monitoring.

    Attributes:
        tenant_id: The tenant UUID.
        duration_ms: How long the pause lasted in milliseconds.
        started_at: When the pause began (UTC).
        ended_at: When the pause ended (UTC).
        max_waiters: Maximum number of concurrent waiters observed.
        total_waiters: Total number of wait operations during pause.
    """

    tenant_id: UUID
    duration_ms: float
    started_at: datetime
    ended_at: datetime
    max_waiters: int = 0
    total_waiters: int = 0

    @property
    def duration_seconds(self) -> float:
        """Get duration in seconds."""
        return self.duration_ms / 1000.0

    def to_dict(self) -> dict[str, Any]:
        """
        Convert to dictionary for logging/serialization.

        Returns:
            Dictionary representation of the metrics.
        """
        return {
            "tenant_id": str(self.tenant_id),
            "duration_ms": self.duration_ms,
            "duration_seconds": self.duration_seconds,
            "started_at": self.started_at.isoformat(),
            "ended_at": self.ended_at.isoformat(),
            "max_waiters": self.max_waiters,
            "total_waiters": self.total_waiters,
        }


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


__all__ = [
    "WritePauseManager",
    "WritePausedError",
    "PauseMetrics",
    "PauseState",
]
