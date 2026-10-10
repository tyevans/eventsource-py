"""
Coordination primitives for multi-instance deployments (facade).

This module re-exports coordination topic constants, shutdown and heartbeat
message value objects, leadership lease verification, and WorkRedistributionCoordinator.
"""

from __future__ import annotations

from eventsource.application.subscriptions.coordination_leadership import (
    WorkAssignment,
    WorkAssignmentCallback,
    verify_leadership_lease,
)
from eventsource.application.subscriptions.coordination_shutdown import (
    COORDINATION_TOPIC_PREFIX,
    HEARTBEAT_TOPIC,
    SHUTDOWN_NOTIFICATIONS_TOPIC,
    WORK_ASSIGNMENT_TOPIC,
    HeartbeatCallback,
    HeartbeatMessage,
    PeerInfo,
    PeerShutdownCallback,
    PeerTimeoutCallback,
    ShutdownIntent,
    ShutdownNotification,
)
from eventsource.application.subscriptions.coordination_work import (
    WorkRedistributionCoordinator,
)
from eventsource.ports.coordination import LeaderChangeCallback, LeaderElector

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
    "verify_leadership_lease",
    # Work redistribution
    "PeerInfo",
    "WorkRedistributionCoordinator",
]
