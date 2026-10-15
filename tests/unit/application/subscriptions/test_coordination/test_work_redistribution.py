"""Tests for WorkRedistributionCoordinator lifecycle, heartbeats, and message handling."""

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from eventsource.adapters.memory.coordination import InMemoryLeaderElector
from eventsource.application.subscriptions.coordination import (
    HeartbeatMessage,
    ShutdownIntent,
    ShutdownNotification,
    WorkAssignment,
    WorkRedistributionCoordinator,
)


def _make_shutdown(
    instance_id: str = "worker-2", subscriptions: tuple[str, ...] = ("sub-1",)
) -> ShutdownNotification:
    now = datetime.now(UTC)
    return ShutdownNotification(
        instance_id=instance_id,
        intent=ShutdownIntent.GRACEFUL,
        initiated_at=now,
        expected_completion_at=now + timedelta(seconds=30),
        subscriptions=subscriptions,
    )


def _collector() -> tuple[list[Any], Any]:
    items: list[Any] = []

    async def cb(item: Any) -> None:
        items.append(item)

    return items, cb


@pytest.mark.asyncio
class TestWorkRedistributionCoordinator:
    async def test_create_coordinator(self) -> None:
        coordinator = WorkRedistributionCoordinator(instance_id="worker-1")
        assert coordinator.instance_id == "worker-1"
        assert coordinator.is_shutting_down is False and coordinator.shutdown_notification is None
        assert coordinator.known_peers == {} and coordinator.healthy_peer_count == 0
        assert coordinator.draining_peers == []

    async def test_create_shutdown_notification(self) -> None:
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
        coordinator = WorkRedistributionCoordinator(instance_id="worker-1")
        heartbeat = coordinator.create_heartbeat(
            subscriptions=["sub-1"], in_flight_count=5, load_factor=0.5
        )
        assert heartbeat.instance_id == "worker-1" and heartbeat.subscriptions == ("sub-1",)
        assert heartbeat.in_flight_count == 5 and heartbeat.is_leader is False
        assert heartbeat.load_factor == 0.5

    async def test_create_heartbeat_with_leader(self) -> None:
        elector = InMemoryLeaderElector(_identity="worker-1")
        await elector.try_acquire()
        coordinator = WorkRedistributionCoordinator(instance_id="worker-1", leader_elector=elector)
        heartbeat = coordinator.create_heartbeat(
            subscriptions=["sub-1"], in_flight_count=5, load_factor=0.5
        )
        assert heartbeat.is_leader is True

    async def test_handle_peer_shutdown(self) -> None:
        coordinator = WorkRedistributionCoordinator(instance_id="worker-1")
        received_notifications, callback = _collector()
        coordinator.on_peer_shutdown(callback)
        notification = _make_shutdown("worker-2")
        await coordinator.handle_peer_shutdown(notification)
        assert received_notifications == [notification]
        assert coordinator.known_peers["worker-2"].status == "draining"

    async def test_handle_peer_shutdown_ignores_self(self) -> None:
        coordinator = WorkRedistributionCoordinator(instance_id="worker-1")
        received_notifications, callback = _collector()
        coordinator.on_peer_shutdown(callback)
        await coordinator.handle_peer_shutdown(_make_shutdown("worker-1"))
        assert len(received_notifications) == 0 and "worker-1" not in coordinator.known_peers

    async def test_handle_heartbeat(self) -> None:
        coordinator = WorkRedistributionCoordinator(instance_id="worker-1")
        received_heartbeats, callback = _collector()
        coordinator.on_heartbeat(callback)
        heartbeat = HeartbeatMessage(
            instance_id="worker-2",
            timestamp=datetime.now(UTC),
            subscriptions=("sub-1",),
            in_flight_count=5,
        )
        await coordinator.handle_heartbeat(heartbeat)
        assert received_heartbeats == [heartbeat]
        assert coordinator.known_peers["worker-2"].status == "healthy"
        assert coordinator.healthy_peer_count == 1

    async def test_handle_heartbeat_ignores_self(self) -> None:
        coordinator = WorkRedistributionCoordinator(instance_id="worker-1")
        received_heartbeats, callback = _collector()
        coordinator.on_heartbeat(callback)
        await coordinator.handle_heartbeat(
            HeartbeatMessage(instance_id="worker-1", timestamp=datetime.now(UTC))
        )
        assert len(received_heartbeats) == 0 and "worker-1" not in coordinator.known_peers

    async def test_handle_work_assignment(self) -> None:
        coordinator = WorkRedistributionCoordinator(instance_id="worker-2")
        received_assignments, callback = _collector()
        coordinator.on_work_assignment(callback)
        assignment = WorkAssignment(
            target_instance_id="worker-2",
            subscriptions=("sub-1",),
            source_instance_id="worker-1",
            assigned_at=datetime.now(UTC),
        )
        await coordinator.handle_work_assignment(assignment)
        assert received_assignments == [assignment]

    async def test_handle_work_assignment_ignores_other_target(self) -> None:
        coordinator = WorkRedistributionCoordinator(instance_id="worker-1")
        received_assignments, callback = _collector()
        coordinator.on_work_assignment(callback)
        await coordinator.handle_work_assignment(
            WorkAssignment(
                target_instance_id="worker-2",
                subscriptions=("sub-1",),
                source_instance_id="worker-3",
                assigned_at=datetime.now(UTC),
            )
        )
        assert len(received_assignments) == 0

    async def test_callback_error_handling(self) -> None:
        coordinator = WorkRedistributionCoordinator(instance_id="worker-1")
        successful_callbacks: list[str] = []

        async def failing_callback(notification: ShutdownNotification) -> None:
            raise RuntimeError("Callback failed")

        async def success_callback(notification: ShutdownNotification) -> None:
            successful_callbacks.append(notification.instance_id)

        coordinator.on_peer_shutdown(failing_callback)
        coordinator.on_peer_shutdown(success_callback)
        await coordinator.handle_peer_shutdown(_make_shutdown("worker-2", ()))
        assert successful_callbacks == ["worker-2"]

    async def test_remove_callbacks(self) -> None:
        coordinator = WorkRedistributionCoordinator(instance_id="worker-1")

        async def cb(*_args: object) -> None: ...

        coordinator.on_peer_shutdown(cb)
        coordinator.on_heartbeat(cb)
        coordinator.on_peer_timeout(cb)
        coordinator.on_work_assignment(cb)

        assert coordinator.remove_peer_shutdown_callback(cb) is True
        assert coordinator.remove_peer_shutdown_callback(cb) is False
        assert coordinator.remove_heartbeat_callback(cb) is True
        assert coordinator.remove_heartbeat_callback(cb) is False
        assert coordinator.remove_peer_timeout_callback(cb) is True
        assert coordinator.remove_peer_timeout_callback(cb) is False
        assert coordinator.remove_work_assignment_callback(cb) is True
        assert coordinator.remove_work_assignment_callback(cb) is False
