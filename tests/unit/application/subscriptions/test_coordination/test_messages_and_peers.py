"""Tests for heartbeat messages, work assignments, and peer info tracking."""

from datetime import UTC, datetime, timedelta

from eventsource.application.subscriptions.coordination import (
    HeartbeatMessage,
    PeerInfo,
    ShutdownIntent,
    ShutdownNotification,
    WorkAssignment,
)


class TestHeartbeatMessage:
    """Tests for HeartbeatMessage."""

    def test_create_heartbeat(self) -> None:
        now = datetime.now(UTC)
        hb = HeartbeatMessage(
            instance_id="worker-1",
            timestamp=now,
            subscriptions=("sub-1",),
            in_flight_count=5,
            is_leader=True,
            load_factor=0.75,
        )
        assert hb.instance_id == "worker-1" and hb.timestamp == now
        assert hb.subscriptions == ("sub-1",) and hb.in_flight_count == 5
        assert hb.is_leader is True and hb.load_factor == 0.75

    def test_to_dict(self) -> None:
        now = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
        hb = HeartbeatMessage(
            instance_id="worker-1",
            timestamp=now,
            subscriptions=("sub-1", "sub-2"),
            in_flight_count=10,
            is_leader=True,
            load_factor=0.5,
        )
        res = hb.to_dict()
        assert res["instance_id"] == "worker-1"
        assert res["timestamp"] == "2025-01-01T12:00:00+00:00"
        assert res["subscriptions"] == ["sub-1", "sub-2"] and res["in_flight_count"] == 10
        assert res["is_leader"] is True and res["load_factor"] == 0.5

    def test_from_dict(self) -> None:
        data = {
            "instance_id": "worker-1",
            "timestamp": "2025-01-01T12:00:00+00:00",
            "subscriptions": ["sub-1"],
            "in_flight_count": 5,
            "is_leader": False,
            "load_factor": 0.25,
        }
        hb = HeartbeatMessage.from_dict(data)
        assert hb.instance_id == "worker-1" and hb.subscriptions == ("sub-1",)
        assert hb.in_flight_count == 5 and hb.is_leader is False and hb.load_factor == 0.25

    def test_is_stale_fresh(self) -> None:
        hb = HeartbeatMessage(instance_id="worker-1", timestamp=datetime.now(UTC))
        assert hb.is_stale(max_age_seconds=15.0) is False

    def test_is_stale_old(self) -> None:
        hb = HeartbeatMessage(
            instance_id="worker-1", timestamp=datetime.now(UTC) - timedelta(seconds=20)
        )
        assert hb.is_stale(max_age_seconds=15.0) is True

    def test_round_trip(self) -> None:
        original = HeartbeatMessage(
            instance_id="worker-1",
            timestamp=datetime.now(UTC),
            subscriptions=("sub-1",),
            in_flight_count=5,
            is_leader=True,
            load_factor=0.75,
        )
        restored = HeartbeatMessage.from_dict(original.to_dict())
        assert restored.instance_id == original.instance_id
        assert restored.is_leader == original.is_leader
        assert restored.load_factor == original.load_factor


class TestWorkAssignment:
    """Tests for WorkAssignment."""

    def test_create_assignment(self) -> None:
        now = datetime.now(UTC)
        assignment = WorkAssignment(
            target_instance_id="worker-2",
            subscriptions=("sub-1", "sub-2"),
            source_instance_id="worker-1",
            assigned_at=now,
            priority=1,
        )
        assert assignment.target_instance_id == "worker-2"
        assert assignment.subscriptions == ("sub-1", "sub-2")
        assert assignment.source_instance_id == "worker-1"
        assert assignment.assigned_at == now and assignment.priority == 1

    def test_to_dict(self) -> None:
        now = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
        assignment = WorkAssignment(
            target_instance_id="worker-2",
            subscriptions=("sub-1",),
            source_instance_id="worker-1",
            assigned_at=now,
            priority=2,
        )
        res = assignment.to_dict()
        assert res["target_instance_id"] == "worker-2" and res["subscriptions"] == ["sub-1"]
        assert res["source_instance_id"] == "worker-1" and res["priority"] == 2
        assert res["assigned_at"] == "2025-01-01T12:00:00+00:00"

    def test_from_dict(self) -> None:
        data = {
            "target_instance_id": "worker-2",
            "subscriptions": ["sub-1", "sub-2"],
            "source_instance_id": "worker-1",
            "assigned_at": "2025-01-01T12:00:00+00:00",
            "priority": 3,
        }
        assignment = WorkAssignment.from_dict(data)
        assert assignment.target_instance_id == "worker-2"
        assert assignment.subscriptions == ("sub-1", "sub-2")
        assert assignment.source_instance_id == "worker-1" and assignment.priority == 3

    def test_round_trip(self) -> None:
        original = WorkAssignment(
            target_instance_id="worker-2",
            subscriptions=("sub-1", "sub-2"),
            source_instance_id="worker-1",
            assigned_at=datetime.now(UTC),
            priority=1,
        )
        restored = WorkAssignment.from_dict(original.to_dict())
        assert restored.target_instance_id == original.target_instance_id
        assert restored.subscriptions == original.subscriptions
        assert restored.source_instance_id == original.source_instance_id
        assert restored.priority == original.priority


class TestPeerInfo:
    """Tests for PeerInfo."""

    def test_initial_state(self) -> None:
        peer = PeerInfo(instance_id="worker-1")
        assert peer.instance_id == "worker-1" and peer.last_heartbeat is None
        assert peer.shutdown_notification is None and peer.status == "unknown"
        assert peer.subscriptions == ()

    def test_status_healthy(self) -> None:
        peer = PeerInfo(
            instance_id="worker-1",
            last_heartbeat=HeartbeatMessage(
                instance_id="worker-1", timestamp=datetime.now(UTC), subscriptions=("sub-1",)
            ),
        )
        assert peer.status == "healthy" and peer.subscriptions == ("sub-1",)

    def test_status_stale(self) -> None:
        peer = PeerInfo(
            instance_id="worker-1",
            last_heartbeat=HeartbeatMessage(
                instance_id="worker-1",
                timestamp=datetime.now(UTC) - timedelta(seconds=20),
                subscriptions=("sub-1",),
            ),
        )
        assert peer.status == "stale"

    def test_status_draining(self) -> None:
        now = datetime.now(UTC)
        peer = PeerInfo(
            instance_id="worker-1",
            shutdown_notification=ShutdownNotification(
                instance_id="worker-1",
                intent=ShutdownIntent.GRACEFUL,
                initiated_at=now,
                expected_completion_at=now + timedelta(seconds=30),
                subscriptions=("sub-1",),
            ),
        )
        assert peer.status == "draining" and peer.subscriptions == ("sub-1",)

    def test_status_terminated(self) -> None:
        now = datetime.now(UTC)
        peer = PeerInfo(
            instance_id="worker-1",
            shutdown_notification=ShutdownNotification(
                instance_id="worker-1",
                intent=ShutdownIntent.GRACEFUL,
                initiated_at=now - timedelta(seconds=60),
                expected_completion_at=now - timedelta(seconds=30),
                subscriptions=("sub-1",),
            ),
        )
        assert peer.status == "terminated"
