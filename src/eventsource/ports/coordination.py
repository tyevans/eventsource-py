"""
Leader election port for multi-instance coordination.

This module provides the pluggable boundary for leader election across
multiple eventsource instances. The LeaderElector protocol allows pluggable
implementations:

- InMemoryLeaderElector (eventsource.adapters.memory.coordination): For
  testing and single-instance deployments
- KubernetesLeaderElector: Using K8s Leases API (future)
- RedisLeaderElector: Using Redis locks (future)
- ConsulLeaderElector: Using Consul sessions (future)

The topic constants, message value objects (ShutdownNotification,
HeartbeatMessage, WorkAssignment), and WorkRedistributionCoordinator live in
``eventsource.application.subscriptions.coordination`` -- they are the
coordinator's wire shapes, not part of this port's payloads.

Example:
    >>> from eventsource.ports.coordination import (
    ...     LeaderElector,
    ...     LeaderChangeCallback,
    ... )
    >>>
    >>> class MyLeaderElector:
    ...     # Implementation satisfying LeaderElector protocol
    ...     pass
    >>>
    >>> async def on_leader_change(is_leader: bool) -> None:
    ...     if is_leader:
    ...         print("Became leader")
    ...     else:
    ...         print("Lost leadership")
    >>>
    >>> elector = MyLeaderElector(identity="instance-1")
    >>> elector.on_leader_change(on_leader_change)
    >>> await elector.try_acquire()
"""

from abc import abstractmethod
from collections.abc import Awaitable, Callable
from typing import Protocol, runtime_checkable

# Type alias for leadership change callbacks
# Called with is_leader=True when acquiring leadership,
# is_leader=False when losing leadership
LeaderChangeCallback = Callable[[bool], Awaitable[None]]


@runtime_checkable
class LeaderElector(Protocol):
    """
    Protocol for leader election implementations.

    Leader election allows a single instance among many to become
    the "leader" for coordinating work that should only happen once
    (e.g., catch-up coordination, global position tracking).

    Implementations must be async-safe and handle:
    - Leadership acquisition with timeout
    - Graceful leadership release
    - Leadership loss detection
    - Callback notification on leadership changes

    Example usage:
        >>> elector = SomeLeaderElector(identity="instance-1")
        >>>
        >>> # Register for leadership changes
        >>> async def on_leader_change(is_leader: bool):
        ...     if is_leader:
        ...         logger.info("Became leader, starting coordination")
        ...     else:
        ...         logger.info("Lost leadership, stopping coordination")
        ...
        >>> elector.on_leader_change(on_leader_change)
        >>>
        >>> # Try to become leader
        >>> if await elector.try_acquire():
        ...     # We are the leader
        ...     await do_leader_work()
        >>>
        >>> # Release on shutdown
        >>> await elector.release()

    Thread Safety:
        All methods must be safe to call from multiple async tasks.
        Leadership state must be consistent across concurrent access.
    """

    @property
    @abstractmethod
    def identity(self) -> str:
        """
        Get the unique identity of this elector.

        This identity is used to identify this instance in the
        leader election process. Should be unique across all
        instances participating in the same election.

        Returns:
            Unique identifier string for this instance
        """
        ...

    @property
    @abstractmethod
    def is_leader(self) -> bool:
        """
        Check if this instance currently holds leadership.

        This is a quick check that returns the cached leadership
        state. It does not verify with the coordination backend.

        Returns:
            True if this instance is the current leader

        Note:
            Leadership may be lost asynchronously. For critical
            operations, use fencing tokens or re-verify leadership.
        """
        ...

    @property
    @abstractmethod
    def current_leader(self) -> str | None:
        """
        Get the identity of the current leader.

        Returns:
            Identity of current leader, or None if no leader
        """
        ...

    @abstractmethod
    async def try_acquire(self, timeout: float = 10.0) -> bool:
        """
        Attempt to acquire leadership.

        Makes a single attempt to become the leader. If another
        instance already holds leadership, returns False immediately
        (does not wait for leadership to become available).

        Args:
            timeout: Maximum time to wait for backend response

        Returns:
            True if leadership was acquired, False otherwise

        Raises:
            ConnectionError: If unable to communicate with backend

        Note:
            This does not block waiting for leadership. Use
            wait_for_leadership() if you need to wait.
        """
        ...

    @abstractmethod
    async def release(self) -> None:
        """
        Release leadership if currently held.

        If this instance is the leader, releases leadership so
        another instance can acquire it. Safe to call even if
        not currently the leader.

        This should be called during graceful shutdown to allow
        faster leadership transfer.

        Raises:
            ConnectionError: If unable to communicate with backend
        """
        ...

    @abstractmethod
    async def renew(self) -> bool:
        """
        Renew leadership lease.

        If this instance is the leader, extends the leadership
        lease. Should be called periodically to maintain leadership.
        If not the leader, returns False.

        Returns:
            True if renewal succeeded, False if not leader or failed

        Note:
            Many backends handle renewal automatically. This method
            is for explicit renewal when needed.
        """
        ...

    @abstractmethod
    def on_leader_change(
        self,
        callback: LeaderChangeCallback,
    ) -> None:
        """
        Register callback for leadership changes.

        The callback is invoked when:
        - This instance becomes the leader (is_leader=True)
        - This instance loses leadership (is_leader=False)

        Multiple callbacks can be registered. They are called in
        registration order.

        Args:
            callback: Async function called with new leadership state

        Example:
            >>> async def handle_change(is_leader: bool):
            ...     if is_leader:
            ...         await start_coordination()
            ...     else:
            ...         await stop_coordination()
            ...
            >>> elector.on_leader_change(handle_change)
        """
        ...

    @abstractmethod
    def remove_leader_change_callback(
        self,
        callback: LeaderChangeCallback,
    ) -> bool:
        """
        Remove a registered leadership change callback.

        Args:
            callback: The callback to remove

        Returns:
            True if callback was found and removed, False otherwise
        """
        ...


__all__ = [
    "LeaderElector",
    "LeaderChangeCallback",
]
