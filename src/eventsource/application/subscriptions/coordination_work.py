"""Work redistribution coordinator for multi-instance deployments."""

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
    """Coordinates work redistribution, peer heartbeats, and shutdown handoff."""

    instance_id: str
    leader_elector: LeaderElector | None = None
    heartbeat_timeout_seconds: float = 15.0

    _peers: dict[str, PeerInfo] = field(default_factory=dict, repr=False)
    _peer_shutdown_callbacks: list[PeerShutdownCallback] = field(default_factory=list, repr=False)
    _heartbeat_callbacks: list[HeartbeatCallback] = field(default_factory=list, repr=False)
    _peer_timeout_callbacks: list[PeerTimeoutCallback] = field(default_factory=list, repr=False)
    _work_assignment_callbacks: list[WorkAssignmentCallback] = field(default_factory=list, repr=False)  # fmt: skip
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)
    _is_shutting_down: bool = field(default=False, repr=False)
    _shutdown_notification: ShutdownNotification | None = field(default=None, repr=False)

    @property
    def is_shutting_down(self) -> bool:
        return self._is_shutting_down

    @property
    def shutdown_notification(self) -> ShutdownNotification | None:
        return self._shutdown_notification

    def create_shutdown_notification(
        self,
        intent: ShutdownIntent,
        subscriptions: list[str] | tuple[str, ...],
        in_flight_count: int = 0,
        drain_timeout_seconds: float = 30.0,
        metadata: dict[str, Any] | None = None,
    ) -> ShutdownNotification:
        """Create a shutdown notification for this instance and mark it as shutting down."""
        now = datetime.now(UTC)
        notification = ShutdownNotification(
            self.instance_id, intent, now, now + timedelta(seconds=drain_timeout_seconds),
            tuple(subscriptions), in_flight_count=in_flight_count, metadata=metadata or {},
        )  # fmt: skip
        self._is_shutting_down, self._shutdown_notification = True, notification
        extra = {
            "instance_id": self.instance_id, "intent": intent.value,
            "subscriptions": list(subscriptions), "in_flight_count": in_flight_count,
            "drain_timeout_seconds": drain_timeout_seconds,
        }  # fmt: skip
        logger.info("Created shutdown notification", extra=extra)
        return notification

    def create_heartbeat(
        self,
        subscriptions: list[str] | tuple[str, ...],
        in_flight_count: int = 0,
        load_factor: float = 0.0,
    ) -> HeartbeatMessage:
        """Create a heartbeat message for this instance."""
        is_leader = self.leader_elector.is_leader if self.leader_elector is not None else False
        return HeartbeatMessage(
            self.instance_id, datetime.now(UTC), tuple(subscriptions),
            in_flight_count=in_flight_count, is_leader=is_leader, load_factor=load_factor,
        )  # fmt: skip

    async def handle_peer_shutdown(self, notification: ShutdownNotification) -> None:
        """Handle a shutdown notification from a peer."""
        if notification.instance_id == self.instance_id:
            return
        async with self._lock:
            peer = self._peers.setdefault(
                notification.instance_id, PeerInfo(instance_id=notification.instance_id)
            )
            peer.shutdown_notification = notification
        extra = {
            "peer_id": notification.instance_id, "intent": notification.intent.value,
            "subscriptions": list(notification.subscriptions),
            "time_remaining": notification.time_remaining_seconds,
        }  # fmt: skip
        logger.info("Received peer shutdown notification", extra=extra)
        for callback in self._peer_shutdown_callbacks:
            try:
                await callback(notification)
            except Exception as e:
                err_extra = {"peer_id": notification.instance_id, "error": str(e)}
                logger.error("Peer shutdown callback failed", extra=err_extra, exc_info=True)

    async def handle_heartbeat(self, heartbeat: HeartbeatMessage) -> None:
        """Handle a heartbeat from a peer."""
        if heartbeat.instance_id == self.instance_id:
            return
        async with self._lock:
            peer = self._peers.setdefault(
                heartbeat.instance_id, PeerInfo(instance_id=heartbeat.instance_id)
            )
            peer.last_heartbeat = heartbeat
        for callback in self._heartbeat_callbacks:
            try:
                await callback(heartbeat)
            except Exception as e:
                err_extra = {"peer_id": heartbeat.instance_id, "error": str(e)}
                logger.error("Heartbeat callback failed", extra=err_extra, exc_info=True)

    async def handle_work_assignment(self, assignment: WorkAssignment) -> None:
        """Handle a work assignment from the leader."""
        if assignment.target_instance_id != self.instance_id:
            return
        extra = {
            "source_instance_id": assignment.source_instance_id,
            "subscriptions": list(assignment.subscriptions), "priority": assignment.priority,
        }  # fmt: skip
        logger.info("Received work assignment", extra=extra)
        for callback in self._work_assignment_callbacks:
            try:
                await callback(assignment)
            except Exception as e:
                err_extra = {"source_instance_id": assignment.source_instance_id, "error": str(e)}
                logger.error("Work assignment callback failed", extra=err_extra, exc_info=True)


__all__ = ["WorkAssignment", "WorkAssignmentCallback", "WorkRedistributionCoordinator"]
