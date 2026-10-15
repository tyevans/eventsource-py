"""Tests for coordination topic constants, shutdown intent, and shutdown notifications."""

from datetime import UTC, datetime, timedelta

from eventsource.application.subscriptions.coordination import (
    COORDINATION_TOPIC_PREFIX,
    HEARTBEAT_TOPIC,
    SHUTDOWN_NOTIFICATIONS_TOPIC,
    WORK_ASSIGNMENT_TOPIC,
    ShutdownIntent,
    ShutdownNotification,
)


class TestTopicConstants:
    """Tests for coordination topic constants."""

    def test_topic_prefix(self) -> None:
        assert COORDINATION_TOPIC_PREFIX == "__eventsource_coordination"

    def test_shutdown_topic(self) -> None:
        assert SHUTDOWN_NOTIFICATIONS_TOPIC == "__eventsource_coordination.shutdown"

    def test_heartbeat_topic(self) -> None:
        assert HEARTBEAT_TOPIC == "__eventsource_coordination.heartbeat"

    def test_work_assignment_topic(self) -> None:
        assert WORK_ASSIGNMENT_TOPIC == "__eventsource_coordination.work_assignment"


class TestShutdownIntent:
    """Tests for ShutdownIntent enumeration."""

    def test_all_intents_defined(self) -> None:
        assert ShutdownIntent.GRACEFUL.value == "graceful"
        assert ShutdownIntent.PREEMPTION.value == "preemption"
        assert ShutdownIntent.HEALTH_FAILURE.value == "health_failure"
        assert ShutdownIntent.MAINTENANCE.value == "maintenance"

    def test_intent_from_value(self) -> None:
        assert ShutdownIntent("graceful") == ShutdownIntent.GRACEFUL
        assert ShutdownIntent("preemption") == ShutdownIntent.PREEMPTION


class TestShutdownNotification:
    """Tests for ShutdownNotification."""

    def test_create_notification(self) -> None:
        now = datetime.now(UTC)
        later = now + timedelta(seconds=30)
        notification = ShutdownNotification(
            instance_id="worker-1",
            intent=ShutdownIntent.GRACEFUL,
            initiated_at=now,
            expected_completion_at=later,
            subscriptions=("sub-1", "sub-2"),
            in_flight_count=10,
            metadata={"key": "value"},
        )
        assert notification.instance_id == "worker-1"
        assert notification.intent == ShutdownIntent.GRACEFUL
        assert notification.initiated_at == now
        assert notification.expected_completion_at == later
        assert notification.subscriptions == ("sub-1", "sub-2")
        assert notification.in_flight_count == 10
        assert notification.metadata == {"key": "value"}

    def test_to_dict(self) -> None:
        now = datetime(2025, 1, 1, 0, 0, 0, tzinfo=UTC)
        later = datetime(2025, 1, 1, 0, 0, 30, tzinfo=UTC)
        notification = ShutdownNotification(
            instance_id="worker-1",
            intent=ShutdownIntent.GRACEFUL,
            initiated_at=now,
            expected_completion_at=later,
            subscriptions=("sub-1", "sub-2"),
            in_flight_count=10,
        )
        result = notification.to_dict()
        assert result["instance_id"] == "worker-1"
        assert result["intent"] == "graceful"
        assert result["initiated_at"] == "2025-01-01T00:00:00+00:00"
        assert result["expected_completion_at"] == "2025-01-01T00:00:30+00:00"
        assert result["subscriptions"] == ["sub-1", "sub-2"]
        assert result["in_flight_count"] == 10

    def test_from_dict(self) -> None:
        data = {
            "instance_id": "worker-1",
            "intent": "preemption",
            "initiated_at": "2025-01-01T00:00:00+00:00",
            "expected_completion_at": "2025-01-01T00:00:30+00:00",
            "subscriptions": ["sub-1"],
            "in_flight_count": 5,
            "metadata": {"spot": True},
        }
        notification = ShutdownNotification.from_dict(data)
        assert notification.instance_id == "worker-1"
        assert notification.intent == ShutdownIntent.PREEMPTION
        assert notification.subscriptions == ("sub-1",)
        assert notification.in_flight_count == 5
        assert notification.metadata == {"spot": True}

    def test_time_remaining_seconds(self) -> None:
        now = datetime.now(UTC)
        notification = ShutdownNotification(
            instance_id="worker-1",
            intent=ShutdownIntent.GRACEFUL,
            initiated_at=now,
            expected_completion_at=now + timedelta(seconds=30),
            subscriptions=(),
        )
        assert 29 < notification.time_remaining_seconds <= 30

    def test_time_remaining_expired(self) -> None:
        now = datetime.now(UTC)
        notification = ShutdownNotification(
            instance_id="worker-1",
            intent=ShutdownIntent.GRACEFUL,
            initiated_at=now - timedelta(seconds=60),
            expected_completion_at=now - timedelta(seconds=30),
            subscriptions=(),
        )
        assert notification.time_remaining_seconds == 0.0

    def test_is_expired(self) -> None:
        now = datetime.now(UTC)
        notification = ShutdownNotification(
            instance_id="worker-1",
            intent=ShutdownIntent.GRACEFUL,
            initiated_at=now,
            expected_completion_at=now + timedelta(seconds=30),
            subscriptions=(),
        )
        assert notification.is_expired is False

        expired_notification = ShutdownNotification(
            instance_id="worker-1",
            intent=ShutdownIntent.GRACEFUL,
            initiated_at=now - timedelta(seconds=60),
            expected_completion_at=now - timedelta(seconds=30),
            subscriptions=(),
        )
        assert expired_notification.is_expired is True

    def test_round_trip(self) -> None:
        now = datetime.now(UTC)
        original = ShutdownNotification(
            instance_id="worker-1",
            intent=ShutdownIntent.MAINTENANCE,
            initiated_at=now,
            expected_completion_at=now + timedelta(seconds=60),
            subscriptions=("sub-1", "sub-2"),
            in_flight_count=15,
            metadata={"reason": "upgrade"},
        )
        restored = ShutdownNotification.from_dict(original.to_dict())
        assert restored.instance_id == original.instance_id
        assert restored.intent == original.intent
        assert restored.subscriptions == original.subscriptions
        assert restored.in_flight_count == original.in_flight_count
        assert restored.metadata == original.metadata


class TestModuleExportsP3003:
    """Tests for P3-003 module exports."""

    def test_all_exports_defined(self) -> None:
        from eventsource.application.subscriptions import coordination

        expected_new = {
            "COORDINATION_TOPIC_PREFIX",
            "SHUTDOWN_NOTIFICATIONS_TOPIC",
            "HEARTBEAT_TOPIC",
            "WORK_ASSIGNMENT_TOPIC",
            "ShutdownIntent",
            "ShutdownNotification",
            "HeartbeatMessage",
            "WorkAssignment",
            "PeerShutdownCallback",
            "HeartbeatCallback",
            "WorkAssignmentCallback",
            "PeerTimeoutCallback",
            "PeerInfo",
            "WorkRedistributionCoordinator",
        }
        for symbol in expected_new:
            assert symbol in coordination.__all__, f"Missing export: {symbol}"

    def test_exports_from_subscriptions_package(self) -> None:
        from eventsource.application.subscriptions import (
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
            WorkAssignment,
            WorkAssignmentCallback,
            WorkRedistributionCoordinator,
        )

        assert COORDINATION_TOPIC_PREFIX is not None
        assert SHUTDOWN_NOTIFICATIONS_TOPIC is not None
        assert HEARTBEAT_TOPIC is not None
        assert WORK_ASSIGNMENT_TOPIC is not None
        assert ShutdownIntent is not None
        assert ShutdownNotification is not None
        assert HeartbeatMessage is not None
        assert WorkAssignment is not None
        assert HeartbeatCallback is not None
        assert PeerShutdownCallback is not None
        assert WorkAssignmentCallback is not None
        assert PeerTimeoutCallback is not None
        assert PeerInfo is not None
        assert WorkRedistributionCoordinator is not None
