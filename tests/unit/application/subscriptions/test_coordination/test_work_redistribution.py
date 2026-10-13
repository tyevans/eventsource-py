"""Tests for WorkRedistributionCoordinator lifecycle, heartbeats, and message handling."""

from datetime import UTC, datetime, timedelta

import pytest

from eventsource.adapters.memory.coordination import InMemoryLeaderElector
from eventsource.application.subscriptions.coordination import (
    HeartbeatMessage,
    ShutdownIntent,
    ShutdownNotification,
    WorkAssignment,
    WorkRedistributionCoordinator,
)


@pytest.mark.asyncio
class TestWorkRedistributionCoordinator:
    """Tests for WorkRedistributionCoordinator."""

    async def test_create_coordinator(self) -> None:
        """Test creating a coordinator."""
        coordinator = WorkRedistributionCoordinator(instance_id="worker-1")

        assert coordinator.instance_id == "worker-1"
        assert coordinator.is_shutting_down is False
        assert coordinator.shutdown_notification is None
        assert coordinator.known_peers == {}
        assert coordinator.healthy_peer_count == 0
        assert coordinator.draining_peers == []

    async def test_create_shutdown_notification(self) -> None:
        """Test creating a shutdown notification."""
        coordinator = WorkRedistributionCoordinator(instance_id="worker-1")

        notification = coordinator.create_shutdown_notification(
            intent=ShutdownIntent.GRACEFUL,
            subscriptions=["sub-1", "sub-2"],
            in_flight_count=10,
            drain_timeout_seconds=30.0,
            metadata={"reason": "scale-down"},
        )

        assert notification.instance_id == "worker-1"
        assert notification.intent == ShutdownIntent.GRACEFUL
        assert notification.subscriptions == ("sub-1", "sub-2")
        assert notification.in_flight_count == 10
        assert notification.metadata == {"reason": "scale-down"}
        assert coordinator.is_shutting_down is True
        assert coordinator.shutdown_notification == notification

    async def test_create_heartbeat_without_leader(self) -> None:
        """Test creating heartbeat without leader elector."""
        coordinator = WorkRedistributionCoordinator(instance_id="worker-1")

        heartbeat = coordinator.create_heartbeat(
            subscriptions=["sub-1"],
            in_flight_count=5,
            load_factor=0.5,
        )

        assert heartbeat.instance_id == "worker-1"
        assert heartbeat.subscriptions == ("sub-1",)
        assert heartbeat.in_flight_count == 5
        assert heartbeat.is_leader is False
        assert heartbeat.load_factor == 0.5

    async def test_create_heartbeat_with_leader(self) -> None:
        """Test creating heartbeat with leader elector."""
        elector = InMemoryLeaderElector(_identity="worker-1")
        await elector.try_acquire()

        coordinator = WorkRedistributionCoordinator(
            instance_id="worker-1",
            leader_elector=elector,
        )

        heartbeat = coordinator.create_heartbeat(
            subscriptions=["sub-1"],
            in_flight_count=5,
            load_factor=0.5,
        )

        assert heartbeat.is_leader is True

    async def test_handle_peer_shutdown(self) -> None:
        """Test handling peer shutdown notification."""
        coordinator = WorkRedistributionCoordinator(instance_id="worker-1")
        received_notifications: list[ShutdownNotification] = []

        async def callback(notification: ShutdownNotification) -> None:
            received_notifications.append(notification)

        coordinator.on_peer_shutdown(callback)

        now = datetime.now(UTC)
        notification = ShutdownNotification(
            instance_id="worker-2",
            intent=ShutdownIntent.GRACEFUL,
            initiated_at=now,
            expected_completion_at=now + timedelta(seconds=30),
            subscriptions=("sub-1",),
        )

        await coordinator.handle_peer_shutdown(notification)

        assert len(received_notifications) == 1
        assert received_notifications[0] == notification
        assert "worker-2" in coordinator.known_peers
        assert coordinator.known_peers["worker-2"].status == "draining"

    async def test_handle_peer_shutdown_ignores_self(self) -> None:
        """Test that own shutdown notification is ignored."""
        coordinator = WorkRedistributionCoordinator(instance_id="worker-1")
        received_notifications: list[ShutdownNotification] = []

        async def callback(notification: ShutdownNotification) -> None:
            received_notifications.append(notification)

        coordinator.on_peer_shutdown(callback)

        now = datetime.now(UTC)
        notification = ShutdownNotification(
            instance_id="worker-1",  # Same as coordinator
            intent=ShutdownIntent.GRACEFUL,
            initiated_at=now,
            expected_completion_at=now + timedelta(seconds=30),
            subscriptions=("sub-1",),
        )

        await coordinator.handle_peer_shutdown(notification)

        assert len(received_notifications) == 0
        assert "worker-1" not in coordinator.known_peers

    async def test_handle_heartbeat(self) -> None:
        """Test handling peer heartbeat."""
        coordinator = WorkRedistributionCoordinator(instance_id="worker-1")
        received_heartbeats: list[HeartbeatMessage] = []

        async def callback(heartbeat: HeartbeatMessage) -> None:
            received_heartbeats.append(heartbeat)

        coordinator.on_heartbeat(callback)

        heartbeat = HeartbeatMessage(
            instance_id="worker-2",
            timestamp=datetime.now(UTC),
            subscriptions=("sub-1",),
            in_flight_count=5,
        )

        await coordinator.handle_heartbeat(heartbeat)

        assert len(received_heartbeats) == 1
        assert received_heartbeats[0] == heartbeat
        assert "worker-2" in coordinator.known_peers
        assert coordinator.known_peers["worker-2"].status == "healthy"
        assert coordinator.healthy_peer_count == 1

    async def test_handle_heartbeat_ignores_self(self) -> None:
        """Test that own heartbeat is ignored."""
        coordinator = WorkRedistributionCoordinator(instance_id="worker-1")
        received_heartbeats: list[HeartbeatMessage] = []

        async def callback(heartbeat: HeartbeatMessage) -> None:
            received_heartbeats.append(heartbeat)

        coordinator.on_heartbeat(callback)

        heartbeat = HeartbeatMessage(
            instance_id="worker-1",  # Same as coordinator
            timestamp=datetime.now(UTC),
        )

        await coordinator.handle_heartbeat(heartbeat)

        assert len(received_heartbeats) == 0
        assert "worker-1" not in coordinator.known_peers

    async def test_handle_work_assignment(self) -> None:
        """Test handling work assignment."""
        coordinator = WorkRedistributionCoordinator(instance_id="worker-2")
        received_assignments: list[WorkAssignment] = []

        async def callback(assignment: WorkAssignment) -> None:
            received_assignments.append(assignment)

        coordinator.on_work_assignment(callback)

        assignment = WorkAssignment(
            target_instance_id="worker-2",
            subscriptions=("sub-1",),
            source_instance_id="worker-1",
            assigned_at=datetime.now(UTC),
        )

        await coordinator.handle_work_assignment(assignment)

        assert len(received_assignments) == 1
        assert received_assignments[0] == assignment

    async def test_handle_work_assignment_ignores_other_target(self) -> None:
        """Test that work assignment for other instance is ignored."""
        coordinator = WorkRedistributionCoordinator(instance_id="worker-1")
        received_assignments: list[WorkAssignment] = []

        async def callback(assignment: WorkAssignment) -> None:
            received_assignments.append(assignment)

        coordinator.on_work_assignment(callback)

        assignment = WorkAssignment(
            target_instance_id="worker-2",  # Not this instance
            subscriptions=("sub-1",),
            source_instance_id="worker-3",
            assigned_at=datetime.now(UTC),
        )

        await coordinator.handle_work_assignment(assignment)

        assert len(received_assignments) == 0

    async def test_callback_error_handling(self) -> None:
        """Test that callback errors don't break processing."""
        coordinator = WorkRedistributionCoordinator(instance_id="worker-1")
        successful_callbacks: list[str] = []

        async def failing_callback(notification: ShutdownNotification) -> None:
            raise RuntimeError("Callback failed")

        async def success_callback(notification: ShutdownNotification) -> None:
            successful_callbacks.append(notification.instance_id)

        coordinator.on_peer_shutdown(failing_callback)
        coordinator.on_peer_shutdown(success_callback)

        now = datetime.now(UTC)
        notification = ShutdownNotification(
            instance_id="worker-2",
            intent=ShutdownIntent.GRACEFUL,
            initiated_at=now,
            expected_completion_at=now + timedelta(seconds=30),
            subscriptions=(),
        )

        # Should not raise
        await coordinator.handle_peer_shutdown(notification)

        assert successful_callbacks == ["worker-2"]

    async def test_remove_callbacks(self) -> None:
        """Test removing callbacks."""
        coordinator = WorkRedistributionCoordinator(instance_id="worker-1")

        async def shutdown_cb(n: ShutdownNotification) -> None:
            pass

        async def heartbeat_cb(h: HeartbeatMessage) -> None:
            pass

        async def timeout_cb(p: str) -> None:
            pass

        async def assignment_cb(a: WorkAssignment) -> None:
            pass

        coordinator.on_peer_shutdown(shutdown_cb)
        coordinator.on_heartbeat(heartbeat_cb)
        coordinator.on_peer_timeout(timeout_cb)
        coordinator.on_work_assignment(assignment_cb)

        assert coordinator.remove_peer_shutdown_callback(shutdown_cb) is True
        assert coordinator.remove_peer_shutdown_callback(shutdown_cb) is False

        assert coordinator.remove_heartbeat_callback(heartbeat_cb) is True
        assert coordinator.remove_heartbeat_callback(heartbeat_cb) is False

        assert coordinator.remove_peer_timeout_callback(timeout_cb) is True
        assert coordinator.remove_peer_timeout_callback(timeout_cb) is False

        assert coordinator.remove_work_assignment_callback(assignment_cb) is True
        assert coordinator.remove_work_assignment_callback(assignment_cb) is False
