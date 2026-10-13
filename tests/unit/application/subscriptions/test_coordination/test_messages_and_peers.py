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
        """Test creating a heartbeat message."""
        now = datetime.now(UTC)

        heartbeat = HeartbeatMessage(
            instance_id="worker-1",
            timestamp=now,
            subscriptions=("sub-1",),
            in_flight_count=5,
            is_leader=True,
            load_factor=0.75,
        )

        assert heartbeat.instance_id == "worker-1"
        assert heartbeat.timestamp == now
        assert heartbeat.subscriptions == ("sub-1",)
        assert heartbeat.in_flight_count == 5
        assert heartbeat.is_leader is True
        assert heartbeat.load_factor == 0.75

    def test_to_dict(self) -> None:
        """Test serialization to dictionary."""
        now = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)

        heartbeat = HeartbeatMessage(
            instance_id="worker-1",
            timestamp=now,
            subscriptions=("sub-1", "sub-2"),
            in_flight_count=10,
            is_leader=True,
            load_factor=0.5,
        )

        result = heartbeat.to_dict()

        assert result["instance_id"] == "worker-1"
        assert result["timestamp"] == "2025-01-01T12:00:00+00:00"
        assert result["subscriptions"] == ["sub-1", "sub-2"]
        assert result["in_flight_count"] == 10
        assert result["is_leader"] is True
        assert result["load_factor"] == 0.5

    def test_from_dict(self) -> None:
        """Test deserialization from dictionary."""
        data = {
            "instance_id": "worker-1",
            "timestamp": "2025-01-01T12:00:00+00:00",
            "subscriptions": ["sub-1"],
            "in_flight_count": 5,
            "is_leader": False,
            "load_factor": 0.25,
        }

        heartbeat = HeartbeatMessage.from_dict(data)

        assert heartbeat.instance_id == "worker-1"
        assert heartbeat.subscriptions == ("sub-1",)
        assert heartbeat.in_flight_count == 5
        assert heartbeat.is_leader is False
        assert heartbeat.load_factor == 0.25

    def test_is_stale_fresh(self) -> None:
        """Test is_stale returns False for fresh heartbeat."""
        heartbeat = HeartbeatMessage(
            instance_id="worker-1",
            timestamp=datetime.now(UTC),
        )

        assert heartbeat.is_stale(max_age_seconds=15.0) is False

    def test_is_stale_old(self) -> None:
        """Test is_stale returns True for old heartbeat."""
        heartbeat = HeartbeatMessage(
            instance_id="worker-1",
            timestamp=datetime.now(UTC) - timedelta(seconds=20),
        )

        assert heartbeat.is_stale(max_age_seconds=15.0) is True

    def test_round_trip(self) -> None:
        """Test serialize/deserialize round trip."""
        original = HeartbeatMessage(
            instance_id="worker-1",
            timestamp=datetime.now(UTC),
            subscriptions=("sub-1",),
            in_flight_count=5,
            is_leader=True,
            load_factor=0.75,
        )

        data = original.to_dict()
        restored = HeartbeatMessage.from_dict(data)

        assert restored.instance_id == original.instance_id
        assert restored.is_leader == original.is_leader
        assert restored.load_factor == original.load_factor


class TestWorkAssignment:
    """Tests for WorkAssignment."""

    def test_create_assignment(self) -> None:
        """Test creating a work assignment."""
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
        assert assignment.assigned_at == now
        assert assignment.priority == 1

    def test_to_dict(self) -> None:
        """Test serialization to dictionary."""
        now = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)

        assignment = WorkAssignment(
            target_instance_id="worker-2",
            subscriptions=("sub-1",),
            source_instance_id="worker-1",
            assigned_at=now,
            priority=2,
        )

        result = assignment.to_dict()

        assert result["target_instance_id"] == "worker-2"
        assert result["subscriptions"] == ["sub-1"]
        assert result["source_instance_id"] == "worker-1"
        assert result["assigned_at"] == "2025-01-01T12:00:00+00:00"
        assert result["priority"] == 2

    def test_from_dict(self) -> None:
        """Test deserialization from dictionary."""
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
        assert assignment.source_instance_id == "worker-1"
        assert assignment.priority == 3

    def test_round_trip(self) -> None:
        """Test serialize/deserialize round trip."""
        original = WorkAssignment(
            target_instance_id="worker-2",
            subscriptions=("sub-1", "sub-2"),
            source_instance_id="worker-1",
            assigned_at=datetime.now(UTC),
            priority=1,
        )

        data = original.to_dict()
        restored = WorkAssignment.from_dict(data)

        assert restored.target_instance_id == original.target_instance_id
        assert restored.subscriptions == original.subscriptions
        assert restored.source_instance_id == original.source_instance_id
        assert restored.priority == original.priority


class TestPeerInfo:
    """Tests for PeerInfo."""

    def test_initial_state(self) -> None:
        """Test initial state of peer info."""
        peer = PeerInfo(instance_id="worker-1")

        assert peer.instance_id == "worker-1"
        assert peer.last_heartbeat is None
        assert peer.shutdown_notification is None
        assert peer.status == "unknown"
        assert peer.subscriptions == ()

    def test_status_healthy(self) -> None:
        """Test status is healthy with recent heartbeat."""
        peer = PeerInfo(
            instance_id="worker-1",
            last_heartbeat=HeartbeatMessage(
                instance_id="worker-1",
                timestamp=datetime.now(UTC),
                subscriptions=("sub-1",),
            ),
        )

        assert peer.status == "healthy"
        assert peer.subscriptions == ("sub-1",)

    def test_status_stale(self) -> None:
        """Test status is stale with old heartbeat."""
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
        """Test status is draining with active shutdown notification."""
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

        assert peer.status == "draining"
        assert peer.subscriptions == ("sub-1",)

    def test_status_terminated(self) -> None:
        """Test status is terminated with expired shutdown notification."""
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
