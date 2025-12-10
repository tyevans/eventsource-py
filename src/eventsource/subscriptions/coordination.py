"""
Coordination primitives for multi-instance deployments.

This module provides abstractions for leader election and work
coordination across multiple eventsource instances.

The LeaderElector protocol allows pluggable implementations:
- InMemoryLeaderElector: For testing and single-instance
- KubernetesLeaderElector: Using K8s Leases API (future)
- RedisLeaderElector: Using Redis locks (future)
- ConsulLeaderElector: Using Consul sessions (future)

Work Redistribution Signals:
When an instance is shutting down, it can broadcast a notification
to other instances so they can prepare to take over its work.

- ShutdownNotification: Broadcast when an instance begins shutdown
- HeartbeatMessage: Periodic health broadcasts for peer monitoring
- WorkRedistributionCoordinator: Coordinates shutdown signaling

Example:
    >>> from eventsource.subscriptions.coordination import (
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

Work Redistribution Example:
    >>> from eventsource.subscriptions.coordination import (
    ...     WorkRedistributionCoordinator,
    ...     ShutdownNotification,
    ...     ShutdownIntent,
    ... )
    >>>
    >>> coordinator = WorkRedistributionCoordinator(instance_id="worker-1")
    >>>
    >>> async def on_peer_shutdown(notification: ShutdownNotification) -> None:
    ...     print(f"Peer {notification.instance_id} is shutting down")
    ...     # Prepare to claim orphaned work
    >>>
    >>> coordinator.on_peer_shutdown(on_peer_shutdown)
"""

import asyncio
import logging
from abc import abstractmethod
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any, Protocol, runtime_checkable

logger = logging.getLogger(__name__)


# =============================================================================
# Coordination Topic Constants
# =============================================================================

COORDINATION_TOPIC_PREFIX = "__eventsource_coordination"
"""Prefix for all coordination topics."""

SHUTDOWN_NOTIFICATIONS_TOPIC = f"{COORDINATION_TOPIC_PREFIX}.shutdown"
"""Topic for shutdown notifications between instances."""

HEARTBEAT_TOPIC = f"{COORDINATION_TOPIC_PREFIX}.heartbeat"
"""Topic for heartbeat messages between instances."""

WORK_ASSIGNMENT_TOPIC = f"{COORDINATION_TOPIC_PREFIX}.work_assignment"
"""Topic for work assignment messages from leader."""


# =============================================================================
# Shutdown Intent Enumeration
# =============================================================================


class ShutdownIntent(Enum):
    """
    The intent behind a shutdown notification.

    Different shutdown intents may require different handling strategies:
    - GRACEFUL: Normal shutdown, other instances have time to prepare
    - PREEMPTION: Cloud preemption, limited time to react
    - HEALTH_FAILURE: Unexpected health failure, may need immediate takeover
    - MAINTENANCE: Planned maintenance, can be scheduled
    """

    GRACEFUL = "graceful"
    """Normal graceful shutdown (e.g., rolling update, scale-down)."""

    PREEMPTION = "preemption"
    """Cloud preemption (spot instance termination)."""

    HEALTH_FAILURE = "health_failure"
    """Shutdown due to health check failure."""

    MAINTENANCE = "maintenance"
    """Planned maintenance window."""


# =============================================================================
# Shutdown Notification Message
# =============================================================================


@dataclass(frozen=True)
class ShutdownNotification:
    """
    Notification broadcast when an instance begins shutdown.

    Other instances can use this to:
    - Stop waiting for the shutting-down instance
    - Prepare to claim orphaned work
    - Adjust load expectations

    The notification includes timing information so peers can
    coordinate their response.

    Attributes:
        instance_id: Unique identifier of the shutting-down instance
        intent: The reason for shutdown
        initiated_at: When shutdown was initiated
        expected_completion_at: When shutdown is expected to complete
        subscriptions: List of subscriptions this instance is handling
        in_flight_count: Number of events currently in flight
        metadata: Additional context (e.g., cloud provider details)

    Example:
        >>> from datetime import timedelta
        >>> notification = ShutdownNotification(
        ...     instance_id="worker-3",
        ...     intent=ShutdownIntent.PREEMPTION,
        ...     initiated_at=datetime.now(UTC),
        ...     expected_completion_at=datetime.now(UTC) + timedelta(seconds=30),
        ...     subscriptions=["order-projection", "inventory-sync"],
        ...     in_flight_count=15,
        ... )
        >>> notification.time_remaining_seconds
        29.99...
    """

    instance_id: str
    intent: ShutdownIntent
    initiated_at: datetime
    expected_completion_at: datetime
    subscriptions: tuple[str, ...] = field(default_factory=tuple)
    in_flight_count: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """
        Convert to dictionary for serialization.

        Returns:
            Dictionary representation suitable for JSON serialization.
        """
        return {
            "instance_id": self.instance_id,
            "intent": self.intent.value,
            "initiated_at": self.initiated_at.isoformat(),
            "expected_completion_at": self.expected_completion_at.isoformat(),
            "subscriptions": list(self.subscriptions),
            "in_flight_count": self.in_flight_count,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ShutdownNotification":
        """
        Create from dictionary.

        Args:
            data: Dictionary with notification fields.

        Returns:
            ShutdownNotification instance.
        """
        return cls(
            instance_id=data["instance_id"],
            intent=ShutdownIntent(data["intent"]),
            initiated_at=datetime.fromisoformat(data["initiated_at"]),
            expected_completion_at=datetime.fromisoformat(data["expected_completion_at"]),
            subscriptions=tuple(data.get("subscriptions", [])),
            in_flight_count=data.get("in_flight_count", 0),
            metadata=data.get("metadata", {}),
        )

    @property
    def time_remaining_seconds(self) -> float:
        """
        Get seconds until expected completion.

        Returns:
            Seconds remaining, or 0.0 if already past expected completion.
        """
        remaining = (self.expected_completion_at - datetime.now(UTC)).total_seconds()
        return max(0.0, remaining)

    @property
    def is_expired(self) -> bool:
        """
        Check if the shutdown notification has expired.

        Returns:
            True if past expected completion time.
        """
        return self.time_remaining_seconds <= 0.0


# =============================================================================
# Heartbeat Message
# =============================================================================


@dataclass(frozen=True)
class HeartbeatMessage:
    """
    Heartbeat message for peer health monitoring.

    Instances can broadcast heartbeats to indicate they are alive
    and processing work. Absence of heartbeats indicates a crashed
    or network-partitioned instance.

    Heartbeats complement shutdown notifications by detecting crashes
    where no explicit notification can be sent.

    Attributes:
        instance_id: Unique identifier of the instance
        timestamp: When heartbeat was generated
        subscriptions: Active subscriptions
        in_flight_count: Current in-flight events
        is_leader: Whether this instance is the leader
        load_factor: Current load as fraction (0.0-1.0)

    Example:
        >>> heartbeat = HeartbeatMessage(
        ...     instance_id="worker-1",
        ...     timestamp=datetime.now(UTC),
        ...     subscriptions=("order-projection",),
        ...     in_flight_count=5,
        ...     is_leader=True,
        ...     load_factor=0.75,
        ... )
    """

    instance_id: str
    timestamp: datetime
    subscriptions: tuple[str, ...] = field(default_factory=tuple)
    in_flight_count: int = 0
    is_leader: bool = False
    load_factor: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        """
        Convert to dictionary for serialization.

        Returns:
            Dictionary representation suitable for JSON serialization.
        """
        return {
            "instance_id": self.instance_id,
            "timestamp": self.timestamp.isoformat(),
            "subscriptions": list(self.subscriptions),
            "in_flight_count": self.in_flight_count,
            "is_leader": self.is_leader,
            "load_factor": self.load_factor,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "HeartbeatMessage":
        """
        Create from dictionary.

        Args:
            data: Dictionary with heartbeat fields.

        Returns:
            HeartbeatMessage instance.
        """
        return cls(
            instance_id=data["instance_id"],
            timestamp=datetime.fromisoformat(data["timestamp"]),
            subscriptions=tuple(data.get("subscriptions", [])),
            in_flight_count=data.get("in_flight_count", 0),
            is_leader=data.get("is_leader", False),
            load_factor=data.get("load_factor", 0.0),
        )

    def is_stale(self, max_age_seconds: float = 15.0) -> bool:
        """
        Check if the heartbeat is stale.

        Args:
            max_age_seconds: Maximum age before considered stale.

        Returns:
            True if heartbeat is older than max_age_seconds.
        """
        age = (datetime.now(UTC) - self.timestamp).total_seconds()
        return age > max_age_seconds


# =============================================================================
# Work Assignment Message
# =============================================================================


@dataclass(frozen=True)
class WorkAssignment:
    """
    Work assignment message from leader to followers.

    Used in leader-coordinated redistribution pattern where the
    leader calculates optimal work distribution and assigns
    subscriptions to specific instances.

    Attributes:
        target_instance_id: Instance that should handle this work
        subscriptions: Subscriptions to be handled
        source_instance_id: Instance the work is coming from
        assigned_at: When the assignment was made
        priority: Assignment priority (higher = more urgent)
    """

    target_instance_id: str
    subscriptions: tuple[str, ...]
    source_instance_id: str
    assigned_at: datetime
    priority: int = 0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "target_instance_id": self.target_instance_id,
            "subscriptions": list(self.subscriptions),
            "source_instance_id": self.source_instance_id,
            "assigned_at": self.assigned_at.isoformat(),
            "priority": self.priority,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "WorkAssignment":
        """Create from dictionary."""
        return cls(
            target_instance_id=data["target_instance_id"],
            subscriptions=tuple(data["subscriptions"]),
            source_instance_id=data["source_instance_id"],
            assigned_at=datetime.fromisoformat(data["assigned_at"]),
            priority=data.get("priority", 0),
        )


# =============================================================================
# Callback Types
# =============================================================================

# Type alias for leadership change callbacks
# Called with is_leader=True when acquiring leadership,
# is_leader=False when losing leadership
LeaderChangeCallback = Callable[[bool], Awaitable[None]]

# Type alias for peer shutdown callbacks
# Called when a peer instance broadcasts a shutdown notification
PeerShutdownCallback = Callable[[ShutdownNotification], Awaitable[None]]

# Type alias for heartbeat callbacks
# Called when a heartbeat is received from a peer
HeartbeatCallback = Callable[[HeartbeatMessage], Awaitable[None]]

# Type alias for work assignment callbacks
# Called when work is assigned to this instance
WorkAssignmentCallback = Callable[[WorkAssignment], Awaitable[None]]

# Type alias for peer timeout callbacks
# Called when a peer hasn't sent a heartbeat within the timeout period
PeerTimeoutCallback = Callable[[str], Awaitable[None]]  # instance_id


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


@runtime_checkable
class LeaderElectorWithLease(LeaderElector, Protocol):
    """
    Extended protocol for lease-based leader election.

    Adds lease management capabilities for implementations that
    use time-based leases (e.g., Kubernetes Leases, Redis locks).

    This protocol extends LeaderElector with additional methods
    for managing lease duration and waiting for leadership.

    Example:
        >>> elector = KubernetesLeaderElector(
        ...     identity="pod-1",
        ...     lease_duration=15.0,
        ... )
        >>>
        >>> # Wait for leadership with timeout
        >>> if await elector.wait_for_leadership(timeout=30.0):
        ...     # We are the leader
        ...     remaining = elector.lease_remaining_seconds
        ...     print(f"Leader with {remaining}s remaining on lease")
    """

    @property
    @abstractmethod
    def lease_duration_seconds(self) -> float:
        """
        Get the lease duration in seconds.

        Returns:
            Duration of leadership lease
        """
        ...

    @property
    @abstractmethod
    def lease_remaining_seconds(self) -> float | None:
        """
        Get remaining time on current lease.

        Returns:
            Seconds remaining on lease, or None if not leader
        """
        ...

    @abstractmethod
    async def wait_for_leadership(
        self,
        timeout: float | None = None,
    ) -> bool:
        """
        Wait until this instance becomes leader.

        Blocks until leadership is acquired or timeout expires.

        Args:
            timeout: Maximum time to wait, None for indefinite

        Returns:
            True if became leader, False if timeout expired
        """
        ...


@dataclass
class SharedLeaderState:
    """
    Shared state for simulated multi-instance leader election.

    Create one instance and share it across multiple InMemoryLeaderElector
    instances to simulate multi-instance behavior in tests.

    Example:
        >>> state = SharedLeaderState()
        >>> elector1 = InMemoryLeaderElector("worker-1", shared_state=state)
        >>> elector2 = InMemoryLeaderElector("worker-2", shared_state=state)
        >>> await elector1.try_acquire()
        True
        >>> await elector2.try_acquire()
        False
        >>> state.current_leader
        'worker-1'
    """

    current_leader: str | None = None


@dataclass
class InMemoryLeaderElector:
    """
    In-memory leader election for testing and single-instance deployments.

    This implementation provides two modes:
    1. Single-instance mode (default): Always becomes leader immediately
    2. Simulated multi-instance mode: Shared state for testing scenarios

    For production multi-instance deployments, use KubernetesLeaderElector
    or RedisLeaderElector instead.

    Example (single instance):
        >>> elector = InMemoryLeaderElector(identity="worker-1")
        >>> await elector.try_acquire()  # Always True
        True
        >>> elector.is_leader
        True

    Example (simulated multi-instance):
        >>> # Share state across instances
        >>> elector1 = InMemoryLeaderElector(identity="worker-1", shared_state=state)
        >>> elector2 = InMemoryLeaderElector(identity="worker-2", shared_state=state)
        >>> await elector1.try_acquire()  # First wins
        True
        >>> await elector2.try_acquire()  # Already taken
        False

    Attributes:
        identity: Unique identifier for this instance
        shared_state: Optional shared state for simulated multi-instance
    """

    _identity: str
    shared_state: "SharedLeaderState | None" = None

    # Internal state
    _is_leader: bool = field(default=False, repr=False)
    _callbacks: list[LeaderChangeCallback] = field(default_factory=list, repr=False)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)

    @property
    def identity(self) -> str:
        """Get the unique identity of this elector."""
        return self._identity

    @property
    def is_leader(self) -> bool:
        """Check if this instance is the leader."""
        return self._is_leader

    @property
    def current_leader(self) -> str | None:
        """Get the identity of the current leader."""
        if self.shared_state is not None:
            return self.shared_state.current_leader
        return self._identity if self._is_leader else None

    async def try_acquire(self, timeout: float = 10.0) -> bool:
        """
        Attempt to acquire leadership.

        In single-instance mode, always succeeds.
        In simulated multi-instance mode, succeeds if no other leader.
        """
        async with self._lock:
            if self.shared_state is not None:
                # Simulated multi-instance mode
                if self.shared_state.current_leader is None:
                    self.shared_state.current_leader = self._identity
                    await self._set_leadership(True)
                    return True
                # Return True if already leader, False if another instance is leader
                return self.shared_state.current_leader == self._identity
            else:
                # Single-instance mode - always become leader
                if not self._is_leader:
                    await self._set_leadership(True)
                return True

    async def release(self) -> None:
        """Release leadership if currently held."""
        async with self._lock:
            if not self._is_leader:
                return

            if self.shared_state is not None and self.shared_state.current_leader == self._identity:
                self.shared_state.current_leader = None

            await self._set_leadership(False)

            logger.info(
                "Leadership released",
                extra={"identity": self._identity},
            )

    async def renew(self) -> bool:
        """Renew leadership lease (no-op for in-memory)."""
        return self._is_leader

    def on_leader_change(self, callback: LeaderChangeCallback) -> None:
        """Register callback for leadership changes."""
        self._callbacks.append(callback)

    def remove_leader_change_callback(self, callback: LeaderChangeCallback) -> bool:
        """Remove a registered leadership change callback."""
        try:
            self._callbacks.remove(callback)
            return True
        except ValueError:
            return False

    async def _set_leadership(self, is_leader: bool) -> None:
        """Set leadership state and invoke callbacks."""
        if self._is_leader == is_leader:
            return

        self._is_leader = is_leader

        logger.info(
            "Leadership changed",
            extra={
                "identity": self._identity,
                "is_leader": is_leader,
            },
        )

        # Invoke callbacks
        for callback in self._callbacks:
            try:
                await callback(is_leader)
            except Exception as e:
                logger.error(
                    "Leader change callback failed",
                    extra={
                        "identity": self._identity,
                        "is_leader": is_leader,
                        "error": str(e),
                    },
                    exc_info=True,
                )

    async def force_lose_leadership(self) -> None:
        """
        Force this instance to lose leadership (for testing).

        Simulates leadership being revoked by another instance
        or coordination backend.
        """
        async with self._lock:
            if self._is_leader:
                if self.shared_state is not None:
                    self.shared_state.current_leader = None
                await self._set_leadership(False)


# =============================================================================
# Work Redistribution Coordinator
# =============================================================================


@dataclass
class PeerInfo:
    """
    Information about a peer instance.

    Tracks the last known state of a peer based on heartbeats
    and shutdown notifications.

    Attributes:
        instance_id: Unique identifier of the peer
        last_heartbeat: Most recent heartbeat received
        shutdown_notification: Shutdown notification if peer is draining
        status: Current status of the peer
    """

    instance_id: str
    last_heartbeat: HeartbeatMessage | None = None
    shutdown_notification: ShutdownNotification | None = None

    @property
    def status(self) -> str:
        """Get current status of the peer."""
        if self.shutdown_notification is not None:
            if self.shutdown_notification.is_expired:
                return "terminated"
            return "draining"
        if self.last_heartbeat is None:
            return "unknown"
        if self.last_heartbeat.is_stale():
            return "stale"
        return "healthy"

    @property
    def subscriptions(self) -> tuple[str, ...]:
        """Get subscriptions handled by this peer."""
        if self.shutdown_notification is not None:
            return self.shutdown_notification.subscriptions
        if self.last_heartbeat is not None:
            return self.last_heartbeat.subscriptions
        return ()


@dataclass
class WorkRedistributionCoordinator:
    """
    Coordinates work redistribution during instance shutdown.

    This coordinator manages the signaling protocol for work redistribution:
    - Tracks known peers and their status
    - Creates shutdown notifications for this instance
    - Invokes callbacks when peers shutdown or timeout
    - Optionally integrates with LeaderElector for leadership handoff

    The coordinator does not implement the actual message transport.
    It provides the protocol and callback infrastructure that can be
    integrated with any message bus (Redis, RabbitMQ, Kafka, etc.).

    Example:
        >>> coordinator = WorkRedistributionCoordinator(instance_id="worker-1")
        >>>
        >>> # Register for peer shutdown notifications
        >>> async def on_peer_shutdown(notification: ShutdownNotification) -> None:
        ...     print(f"Peer {notification.instance_id} is shutting down")
        ...     print(f"Subscriptions to claim: {notification.subscriptions}")
        ...
        >>> coordinator.on_peer_shutdown(on_peer_shutdown)
        >>>
        >>> # Create shutdown notification for this instance
        >>> notification = coordinator.create_shutdown_notification(
        ...     intent=ShutdownIntent.GRACEFUL,
        ...     subscriptions=["order-projection", "inventory-sync"],
        ...     in_flight_count=15,
        ...     drain_timeout_seconds=30.0,
        ... )
        >>> # Publish notification to message bus...

    Integration with LeaderElector:
        >>> elector = InMemoryLeaderElector(_identity="worker-1")
        >>> coordinator = WorkRedistributionCoordinator(
        ...     instance_id="worker-1",
        ...     leader_elector=elector,
        ... )
        >>>
        >>> # On shutdown, leadership will be released automatically
        >>> notification = coordinator.create_shutdown_notification(...)

    Attributes:
        instance_id: Unique identifier for this instance
        leader_elector: Optional leader elector for leadership handoff
        heartbeat_timeout_seconds: Seconds before a peer is considered stale
    """

    instance_id: str
    leader_elector: LeaderElector | None = None
    heartbeat_timeout_seconds: float = 15.0

    # Peer tracking
    _peers: dict[str, PeerInfo] = field(default_factory=dict, repr=False)

    # Callbacks
    _peer_shutdown_callbacks: list[PeerShutdownCallback] = field(default_factory=list, repr=False)
    _heartbeat_callbacks: list[HeartbeatCallback] = field(default_factory=list, repr=False)
    _peer_timeout_callbacks: list[PeerTimeoutCallback] = field(default_factory=list, repr=False)
    _work_assignment_callbacks: list[WorkAssignmentCallback] = field(
        default_factory=list, repr=False
    )

    # Lock for thread safety
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)

    # Shutdown state
    _is_shutting_down: bool = field(default=False, repr=False)
    _shutdown_notification: ShutdownNotification | None = field(default=None, repr=False)

    @property
    def is_shutting_down(self) -> bool:
        """Check if this instance is shutting down."""
        return self._is_shutting_down

    @property
    def shutdown_notification(self) -> ShutdownNotification | None:
        """Get the shutdown notification for this instance."""
        return self._shutdown_notification

    @property
    def known_peers(self) -> dict[str, PeerInfo]:
        """Get a copy of known peers."""
        return dict(self._peers)

    @property
    def healthy_peer_count(self) -> int:
        """Get count of healthy peers."""
        return sum(1 for p in self._peers.values() if p.status == "healthy")

    @property
    def draining_peers(self) -> list[PeerInfo]:
        """Get list of peers that are draining."""
        return [p for p in self._peers.values() if p.status == "draining"]

    def create_shutdown_notification(
        self,
        intent: ShutdownIntent,
        subscriptions: list[str] | tuple[str, ...],
        in_flight_count: int = 0,
        drain_timeout_seconds: float = 30.0,
        metadata: dict[str, Any] | None = None,
    ) -> ShutdownNotification:
        """
        Create a shutdown notification for this instance.

        This method creates the notification and marks this coordinator
        as shutting down. The caller is responsible for publishing the
        notification to the coordination topic.

        Args:
            intent: The reason for shutdown
            subscriptions: Subscriptions this instance is handling
            in_flight_count: Number of events currently in flight
            drain_timeout_seconds: Expected time to complete shutdown
            metadata: Additional context

        Returns:
            ShutdownNotification ready for publishing

        Example:
            >>> notification = coordinator.create_shutdown_notification(
            ...     intent=ShutdownIntent.PREEMPTION,
            ...     subscriptions=["order-projection"],
            ...     in_flight_count=10,
            ...     drain_timeout_seconds=30.0,
            ...     metadata={"spot_termination_time": "2025-01-01T12:00:00Z"},
            ... )
        """
        now = datetime.now(UTC)
        from datetime import timedelta

        notification = ShutdownNotification(
            instance_id=self.instance_id,
            intent=intent,
            initiated_at=now,
            expected_completion_at=now + timedelta(seconds=drain_timeout_seconds),
            subscriptions=tuple(subscriptions),
            in_flight_count=in_flight_count,
            metadata=metadata or {},
        )

        self._is_shutting_down = True
        self._shutdown_notification = notification

        logger.info(
            "Created shutdown notification",
            extra={
                "instance_id": self.instance_id,
                "intent": intent.value,
                "subscriptions": list(subscriptions),
                "in_flight_count": in_flight_count,
                "drain_timeout_seconds": drain_timeout_seconds,
            },
        )

        return notification

    def create_heartbeat(
        self,
        subscriptions: list[str] | tuple[str, ...],
        in_flight_count: int = 0,
        load_factor: float = 0.0,
    ) -> HeartbeatMessage:
        """
        Create a heartbeat message for this instance.

        Args:
            subscriptions: Active subscriptions
            in_flight_count: Current in-flight events
            load_factor: Current load as fraction (0.0-1.0)

        Returns:
            HeartbeatMessage ready for publishing
        """
        is_leader = False
        if self.leader_elector is not None:
            is_leader = self.leader_elector.is_leader

        return HeartbeatMessage(
            instance_id=self.instance_id,
            timestamp=datetime.now(UTC),
            subscriptions=tuple(subscriptions),
            in_flight_count=in_flight_count,
            is_leader=is_leader,
            load_factor=load_factor,
        )

    async def handle_peer_shutdown(self, notification: ShutdownNotification) -> None:
        """
        Handle a shutdown notification from a peer.

        Updates peer tracking and invokes registered callbacks.

        Args:
            notification: Shutdown notification from peer
        """
        if notification.instance_id == self.instance_id:
            # Ignore our own notification
            return

        async with self._lock:
            # Update or create peer info
            if notification.instance_id not in self._peers:
                self._peers[notification.instance_id] = PeerInfo(
                    instance_id=notification.instance_id
                )
            self._peers[notification.instance_id].shutdown_notification = notification

        logger.info(
            "Received peer shutdown notification",
            extra={
                "peer_id": notification.instance_id,
                "intent": notification.intent.value,
                "subscriptions": list(notification.subscriptions),
                "time_remaining": notification.time_remaining_seconds,
            },
        )

        # Invoke callbacks
        for callback in self._peer_shutdown_callbacks:
            try:
                await callback(notification)
            except Exception as e:
                logger.error(
                    "Peer shutdown callback failed",
                    extra={
                        "peer_id": notification.instance_id,
                        "error": str(e),
                    },
                    exc_info=True,
                )

    async def handle_heartbeat(self, heartbeat: HeartbeatMessage) -> None:
        """
        Handle a heartbeat from a peer.

        Updates peer tracking and invokes registered callbacks.

        Args:
            heartbeat: Heartbeat message from peer
        """
        if heartbeat.instance_id == self.instance_id:
            # Ignore our own heartbeat
            return

        async with self._lock:
            # Update or create peer info
            if heartbeat.instance_id not in self._peers:
                self._peers[heartbeat.instance_id] = PeerInfo(instance_id=heartbeat.instance_id)
            self._peers[heartbeat.instance_id].last_heartbeat = heartbeat

        # Invoke callbacks
        for callback in self._heartbeat_callbacks:
            try:
                await callback(heartbeat)
            except Exception as e:
                logger.error(
                    "Heartbeat callback failed",
                    extra={
                        "peer_id": heartbeat.instance_id,
                        "error": str(e),
                    },
                    exc_info=True,
                )

    async def handle_work_assignment(self, assignment: WorkAssignment) -> None:
        """
        Handle a work assignment from the leader.

        Invokes registered callbacks if the assignment is for this instance.

        Args:
            assignment: Work assignment message
        """
        if assignment.target_instance_id != self.instance_id:
            # Not for us
            return

        logger.info(
            "Received work assignment",
            extra={
                "source_instance_id": assignment.source_instance_id,
                "subscriptions": list(assignment.subscriptions),
                "priority": assignment.priority,
            },
        )

        # Invoke callbacks
        for callback in self._work_assignment_callbacks:
            try:
                await callback(assignment)
            except Exception as e:
                logger.error(
                    "Work assignment callback failed",
                    extra={
                        "source_instance_id": assignment.source_instance_id,
                        "error": str(e),
                    },
                    exc_info=True,
                )

    async def check_peer_timeouts(self) -> list[str]:
        """
        Check for timed-out peers and invoke callbacks.

        Should be called periodically (e.g., every heartbeat interval).

        Returns:
            List of peer instance IDs that have timed out
        """
        timed_out: list[str] = []

        async with self._lock:
            for peer_id, peer_info in list(self._peers.items()):
                # Skip peers that are already draining/terminated
                if peer_info.shutdown_notification is not None:
                    continue

                # Check if heartbeat is stale
                if peer_info.last_heartbeat is None:
                    continue

                if peer_info.last_heartbeat.is_stale(self.heartbeat_timeout_seconds):
                    timed_out.append(peer_id)

        # Invoke callbacks outside lock
        for peer_id in timed_out:
            logger.warning(
                "Peer heartbeat timeout",
                extra={
                    "peer_id": peer_id,
                    "timeout_seconds": self.heartbeat_timeout_seconds,
                },
            )

            for callback in self._peer_timeout_callbacks:
                try:
                    await callback(peer_id)
                except Exception as e:
                    logger.error(
                        "Peer timeout callback failed",
                        extra={
                            "peer_id": peer_id,
                            "error": str(e),
                        },
                        exc_info=True,
                    )

        return timed_out

    def remove_peer(self, instance_id: str) -> bool:
        """
        Remove a peer from tracking.

        Args:
            instance_id: ID of peer to remove

        Returns:
            True if peer was removed, False if not found
        """
        if instance_id in self._peers:
            del self._peers[instance_id]
            logger.info(
                "Removed peer from tracking",
                extra={"peer_id": instance_id},
            )
            return True
        return False

    def get_orphaned_subscriptions(self) -> dict[str, tuple[str, ...]]:
        """
        Get subscriptions from peers that are draining or terminated.

        Returns:
            Mapping of peer_id to their subscriptions
        """
        orphaned: dict[str, tuple[str, ...]] = {}
        for peer_id, peer_info in self._peers.items():
            if peer_info.status in ("draining", "terminated", "stale") and peer_info.subscriptions:
                orphaned[peer_id] = peer_info.subscriptions
        return orphaned

    # Callback registration methods

    def on_peer_shutdown(self, callback: PeerShutdownCallback) -> None:
        """
        Register callback for peer shutdown notifications.

        The callback is invoked when a peer broadcasts a shutdown
        notification. Use this to prepare for claiming orphaned work.

        Args:
            callback: Async function called with ShutdownNotification
        """
        self._peer_shutdown_callbacks.append(callback)

    def remove_peer_shutdown_callback(self, callback: PeerShutdownCallback) -> bool:
        """
        Remove a peer shutdown callback.

        Args:
            callback: The callback to remove

        Returns:
            True if removed, False if not found
        """
        try:
            self._peer_shutdown_callbacks.remove(callback)
            return True
        except ValueError:
            return False

    def on_heartbeat(self, callback: HeartbeatCallback) -> None:
        """
        Register callback for peer heartbeats.

        Args:
            callback: Async function called with HeartbeatMessage
        """
        self._heartbeat_callbacks.append(callback)

    def remove_heartbeat_callback(self, callback: HeartbeatCallback) -> bool:
        """
        Remove a heartbeat callback.

        Args:
            callback: The callback to remove

        Returns:
            True if removed, False if not found
        """
        try:
            self._heartbeat_callbacks.remove(callback)
            return True
        except ValueError:
            return False

    def on_peer_timeout(self, callback: PeerTimeoutCallback) -> None:
        """
        Register callback for peer timeouts.

        The callback is invoked when a peer hasn't sent a heartbeat
        within the timeout period. Use this for crash detection.

        Args:
            callback: Async function called with peer instance_id
        """
        self._peer_timeout_callbacks.append(callback)

    def remove_peer_timeout_callback(self, callback: PeerTimeoutCallback) -> bool:
        """
        Remove a peer timeout callback.

        Args:
            callback: The callback to remove

        Returns:
            True if removed, False if not found
        """
        try:
            self._peer_timeout_callbacks.remove(callback)
            return True
        except ValueError:
            return False

    def on_work_assignment(self, callback: WorkAssignmentCallback) -> None:
        """
        Register callback for work assignments.

        The callback is invoked when the leader assigns work to
        this instance.

        Args:
            callback: Async function called with WorkAssignment
        """
        self._work_assignment_callbacks.append(callback)

    def remove_work_assignment_callback(self, callback: WorkAssignmentCallback) -> bool:
        """
        Remove a work assignment callback.

        Args:
            callback: The callback to remove

        Returns:
            True if removed, False if not found
        """
        try:
            self._work_assignment_callbacks.remove(callback)
            return True
        except ValueError:
            return False

    async def initiate_leadership_handoff(self) -> bool:
        """
        Initiate leadership handoff if this instance is the leader.

        Should be called before shutdown if using leader election.

        Returns:
            True if leadership was released, False if not leader
        """
        if self.leader_elector is None:
            return False

        if not self.leader_elector.is_leader:
            return False

        logger.info(
            "Initiating leadership handoff",
            extra={"instance_id": self.instance_id},
        )

        await self.leader_elector.release()
        return True


__all__ = [
    # Topic constants
    "COORDINATION_TOPIC_PREFIX",
    "SHUTDOWN_NOTIFICATIONS_TOPIC",
    "HEARTBEAT_TOPIC",
    "WORK_ASSIGNMENT_TOPIC",
    # Enums
    "ShutdownIntent",
    # Message types
    "ShutdownNotification",
    "HeartbeatMessage",
    "WorkAssignment",
    # Callback types
    "LeaderChangeCallback",
    "PeerShutdownCallback",
    "HeartbeatCallback",
    "WorkAssignmentCallback",
    "PeerTimeoutCallback",
    # Leader election
    "LeaderElector",
    "LeaderElectorWithLease",
    "InMemoryLeaderElector",
    "SharedLeaderState",
    # Work redistribution
    "PeerInfo",
    "WorkRedistributionCoordinator",
]
