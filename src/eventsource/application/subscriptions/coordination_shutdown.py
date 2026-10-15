"""Shutdown and heartbeat primitives for subscription coordination."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any

COORDINATION_TOPIC_PREFIX = "__eventsource_coordination"
SHUTDOWN_NOTIFICATIONS_TOPIC = f"{COORDINATION_TOPIC_PREFIX}.shutdown"
HEARTBEAT_TOPIC = f"{COORDINATION_TOPIC_PREFIX}.heartbeat"
WORK_ASSIGNMENT_TOPIC = f"{COORDINATION_TOPIC_PREFIX}.work_assignment"


class ShutdownIntent(Enum):
    """The intent behind a shutdown notification."""

    GRACEFUL = "graceful"
    PREEMPTION = "preemption"
    HEALTH_FAILURE = "health_failure"
    MAINTENANCE = "maintenance"


@dataclass(frozen=True)
class ShutdownNotification:
    """Notification broadcast when an instance begins shutdown."""

    instance_id: str
    intent: ShutdownIntent
    initiated_at: datetime
    expected_completion_at: datetime
    subscriptions: tuple[str, ...] = field(default_factory=tuple)
    in_flight_count: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
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
        return max(0.0, (self.expected_completion_at - datetime.now(UTC)).total_seconds())

    @property
    def is_expired(self) -> bool:
        return self.time_remaining_seconds <= 0.0


@dataclass(frozen=True)
class HeartbeatMessage:
    """Heartbeat message for peer health monitoring."""

    instance_id: str
    timestamp: datetime
    subscriptions: tuple[str, ...] = field(default_factory=tuple)
    in_flight_count: int = 0
    is_leader: bool = False
    load_factor: float = 0.0

    def to_dict(self) -> dict[str, Any]:
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
        return cls(
            instance_id=data["instance_id"],
            timestamp=datetime.fromisoformat(data["timestamp"]),
            subscriptions=tuple(data.get("subscriptions", [])),
            in_flight_count=data.get("in_flight_count", 0),
            is_leader=data.get("is_leader", False),
            load_factor=data.get("load_factor", 0.0),
        )

    def is_stale(self, max_age_seconds: float = 15.0) -> bool:
        return (datetime.now(UTC) - self.timestamp).total_seconds() > max_age_seconds


@dataclass
class PeerInfo:
    """Tracks the last known state of a peer based on heartbeats and shutdown notifications."""

    instance_id: str
    last_heartbeat: HeartbeatMessage | None = None
    shutdown_notification: ShutdownNotification | None = None

    @property
    def status(self) -> str:
        if self.shutdown_notification is not None:
            return "terminated" if self.shutdown_notification.is_expired else "draining"
        if self.last_heartbeat is None:
            return "unknown"
        return "stale" if self.last_heartbeat.is_stale() else "healthy"

    @property
    def subscriptions(self) -> tuple[str, ...]:
        if self.shutdown_notification is not None:
            return self.shutdown_notification.subscriptions
        return self.last_heartbeat.subscriptions if self.last_heartbeat is not None else ()


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
