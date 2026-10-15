"""
Leader election port for multi-instance coordination.

Provides the pluggable boundary (`LeaderElector`) for leader election across
multiple eventsource instances (e.g., `InMemoryLeaderElector` for testing and
single-instance deployments, or distributed lease/lock backends).

Topic constants, message value objects (`ShutdownNotification`,
`HeartbeatMessage`, `WorkAssignment`), and `WorkRedistributionCoordinator` live
in `eventsource.application.subscriptions.coordination`.
"""

from abc import abstractmethod
from collections.abc import Awaitable, Callable
from typing import Protocol, runtime_checkable

# Type alias for leadership change callbacks (True on acquire, False on loss)
LeaderChangeCallback = Callable[[bool], Awaitable[None]]


@runtime_checkable
class LeaderElector(Protocol):
    """
    Protocol for async-safe leader election implementations.

    Allows a single instance among many to become the leader for coordinating
    work that should only happen once (e.g., catch-up coordination, global
    position tracking). Implementations must handle acquisition with timeout,
    graceful release, leadership loss detection, and callback notifications.
    """

    @property
    @abstractmethod
    def identity(self) -> str:
        """Get the unique identifier string for this elector instance."""
        ...

    @property
    @abstractmethod
    def is_leader(self) -> bool:
        """
        Check if this instance currently holds leadership (cached state).

        Note: Leadership may be lost asynchronously; use fencing tokens or
        re-verify leadership for critical operations.
        """
        ...

    @property
    @abstractmethod
    def current_leader(self) -> str | None:
        """Get the identity of the current leader, or None if no leader."""
        ...

    @abstractmethod
    async def try_acquire(self, timeout: float = 10.0) -> bool:
        """
        Attempt to acquire leadership without blocking if another holds it.

        Args:
            timeout: Maximum time to wait for backend response

        Returns:
            True if leadership was acquired, False otherwise

        Raises:
            ConnectionError: If unable to communicate with backend
        """
        ...

    @abstractmethod
    async def release(self) -> None:
        """
        Release leadership if currently held (safe to call if not leader).

        Raises:
            ConnectionError: If unable to communicate with backend
        """
        ...

    @abstractmethod
    async def renew(self) -> bool:
        """
        Renew leadership lease if currently the leader.

        Returns:
            True if renewal succeeded, False if not leader or failed
        """
        ...

    @abstractmethod
    def on_leader_change(self, callback: LeaderChangeCallback) -> None:
        """
        Register an async callback invoked in registration order on leadership changes.

        Args:
            callback: Async function called with `is_leader=True` or `False`
        """
        ...

    @abstractmethod
    def remove_leader_change_callback(self, callback: LeaderChangeCallback) -> bool:
        """Remove a registered leadership change callback; returns True if removed."""
        ...


__all__ = [
    "LeaderElector",
    "LeaderChangeCallback",
]
