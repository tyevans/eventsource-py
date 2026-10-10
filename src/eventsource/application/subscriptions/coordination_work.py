"""
Work redistribution coordinator for multi-instance deployments.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from eventsource.application.subscriptions.coordination_callbacks import (
    WorkRedistributionCallbacksMixin,
)
from eventsource.application.subscriptions.coordination_leadership import (
    WorkAssignment,
    WorkAssignmentCallback,
    WorkRedistributionLeadershipMixin,
)
from eventsource.application.subscriptions.coordination_peer_tracker import (
    WorkRedistributionPeerTrackerMixin,
)
from eventsource.application.subscriptions.coordination_shutdown import (
    HeartbeatCallback,
    HeartbeatMessage,
    PeerInfo,
    PeerShutdownCallback,
    PeerTimeoutCallback,
    ShutdownIntent,
    ShutdownNotification,
)
from eventsource.ports.coordination import LeaderElector

logger = logging.getLogger(__name__)


@dataclass
class WorkRedistributionCoordinator(
    WorkRedistributionCallbacksMixin,
    WorkRedistributionLeadershipMixin,
    WorkRedistributionPeerTrackerMixin,
):
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
        >>> notification = coordinator.create_shutdown_notification(
        ...     intent=ShutdownIntent.GRACEFUL,
        ...     subscriptions=["order-projection"],
        ... )
        >>> # Publish notification to message bus...

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
        """
        now = datetime.now(UTC)
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


__all__ = [
    "WorkAssignment",
    "WorkAssignmentCallback",
    "WorkRedistributionCoordinator",
]
