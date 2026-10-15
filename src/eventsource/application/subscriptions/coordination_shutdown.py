"""Shutdown and heartbeat primitives for subscription coordination."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any

COORDINATION_TOPIC_PREFIX = "__eventsource_coordination"
"""Prefix for all coordination topics."""

SHUTDOWN_NOTIFICATIONS_TOPIC = f"{COORDINATION_TOPIC_PREFIX}.shutdown"
"""Topic for shutdown notifications between instances."""

HEARTBEAT_TOPIC = f"{COORDINATION_TOPIC_PREFIX}.heartbeat"
"""Topic for heartbeat messages between instances."""

WORK_ASSIGNMENT_TOPIC = f"{COORDINATION_TOPIC_PREFIX}.work_assignment"
"""Topic for work assignment messages from leader."""


class ShutdownIntent(Enum):
    """The intent behind a shutdown notification."""

    GRACEFUL = "graceful"
    """Normal graceful shutdown (e.g., rolling update, scale-down)."""

    PREEMPTION = "preemption"
    """Cloud preemption (spot instance termination)."""

    HEALTH_FAILURE = "health_failure"
    """Shutdown due to health check failure."""

    MAINTENANCE = "maintenance"
    """Planned maintenance window."""


@dataclass(frozen=True)
class ShutdownNotification:
    """
    Notification broadcast when an instance begins shutdown.

    Attributes:
        instance_id: Unique identifier of the shutting-down instance
        intent: The reason for shutdown
        initiated_at: When shutdown was initiated
        expected_completion_at: When shutdown is expected to complete
        subscriptions: Subscriptions this instance is handling
        in_flight_count: Number of events currently in flight
        metadata: Additional context (e.g., cloud provider details)
    """

    instance_id: str
    intent: ShutdownIntent
    initiated_at: datetime
    expected_completion_at: datetime
    subscriptions: tuple[str, ...] = field(default_factory=tuple)
    in_flight_count: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary suitable for JSON serialization."""
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
    def from_dict(cls, data: dict[str, Any]) -> ShutdownNotification:
        """Create a ShutdownNotification instance from a dictionary."""
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
        """Get seconds until expected completion, or 0.0 if already past."""
        remaining = (self.expected_completion_at - datetime.now(UTC)).total_seconds()
        return max(0.0, remaining)

    @property
    def is_expired(self) -> bool:
        """Check if the shutdown notification has expired."""
        return self.time_remaining_seconds <= 0.0


@dataclass(frozen=True)
class HeartbeatMessage:
    """
    Heartbeat message for peer health monitoring.

    Attributes:
        instance_id: Unique identifier of the instance
        timestamp: When heartbeat was generated
        subscriptions: Active subscriptions
        in_flight_count: Current in-flight events
        is_leader: Whether this instance is the leader
        load_factor: Current load as fraction (0.0-1.0)
    """

    instance_id: str
    timestamp: datetime
    subscriptions: tuple[str, ...] = field(default_factory=tuple)
    in_flight_count: int = 0
    is_leader: bool = False
    load_factor: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary suitable for JSON serialization."""
        return {
            "instance_id": self.instance_id,
            "timestamp": self.timestamp.isoformat(),
            "subscriptions": list(self.subscriptions),
            "in_flight_count": self.in_flight_count,
            "is_leader": self.is_leader,
            "load_factor": self.load_factor,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> HeartbeatMessage:
        """Create a HeartbeatMessage instance from a dictionary."""
        return cls(
            instance_id=data["instance_id"],
            timestamp=datetime.fromisoformat(data["timestamp"]),
            subscriptions=tuple(data.get("subscriptions", [])),
            in_flight_count=data.get("in_flight_count", 0),
            is_leader=data.get("is_leader", False),
            load_factor=data.get("load_factor", 0.0),
        )

    def is_stale(self, max_age_seconds: float = 15.0) -> bool:
        """Check if the heartbeat is older than max_age_seconds."""
        age = (datetime.now(UTC) - self.timestamp).total_seconds()
        return age > max_age_seconds


@dataclass
class PeerInfo:
    """Tracks the last known state of a peer based on heartbeats and shutdown notifications."""

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


# =============================================================================
# Callback Types
# =============================================================================

PeerShutdownCallback = Callable[[ShutdownNotification], Awaitable[None]]
HeartbeatCallback = Callable[[HeartbeatMessage], Awaitable[None]]
PeerTimeoutCallback = Callable[[str], Awaitable[None]]  # instance_id

__all__ = [
    "COORDINATION_TOPIC_PREFIX",
    "HEARTBEAT_TOPIC",
    "HeartbeatCallback",
    "HeartbeatMessage",
    "PeerInfo",
    "PeerShutdownCallback",
    "PeerTimeoutCallback",
    "SHUTDOWN_NOTIFICATIONS_TOPIC",
    "ShutdownIntent",
    "ShutdownNotification",
    "WORK_ASSIGNMENT_TOPIC",
]
