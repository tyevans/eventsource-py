"""
Coordination primitives for multi-instance deployments.

This module provides transport-agnostic work coordination across multiple
eventsource instances: the coordination topic constants, the shutdown/
heartbeat/work-assignment message value objects, and
WorkRedistributionCoordinator.

The LeaderElector protocol itself (and its lease-based extension) lives in
``eventsource.ports.coordination`` -- it's a pluggable boundary. Pluggable
implementations:
- InMemoryLeaderElector (eventsource.adapters.memory.coordination): For
  testing and single-instance
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
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any

from eventsource.ports.coordination import LeaderChangeCallback, LeaderElector

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

# LeaderChangeCallback is defined in eventsource.ports.coordination and
# re-imported above; it's part of the LeaderElector port's contract.

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
        >>> from eventsource.adapters.memory.coordination import InMemoryLeaderElector
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
    # Leader election (re-exported from eventsource.ports.coordination;
    # InMemoryLeaderElector/SharedLeaderState now live in
    # eventsource.adapters.memory.coordination)
    "LeaderElector",
    # Work redistribution
    "PeerInfo",
    "WorkRedistributionCoordinator",
]
