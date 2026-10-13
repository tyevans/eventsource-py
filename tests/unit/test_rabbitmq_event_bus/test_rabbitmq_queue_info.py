"""Unit tests extracted for test_rabbitmq_queue_info.py."""

from __future__ import annotations


class TestQueueInfo:
    """Tests for QueueInfo dataclass."""

    def test_default_values(self) -> None:
        """Test that QueueInfo has correct default values."""
        from eventsource.adapters.rabbitmq import QueueInfo

        info = QueueInfo(
            name="test-queue",
            message_count=10,
            consumer_count=2,
        )

        assert info.name == "test-queue"
        assert info.message_count == 10
        assert info.consumer_count == 2
        assert info.state == "running"
        assert info.error is None

    def test_with_error_state(self) -> None:
        """Test QueueInfo with error state."""
        from eventsource.adapters.rabbitmq import QueueInfo

        info = QueueInfo(
            name="test-queue",
            message_count=0,
            consumer_count=0,
            state="error",
            error="Not connected to RabbitMQ",
        )

        assert info.state == "error"
        assert info.error == "Not connected to RabbitMQ"

    def test_idle_state(self) -> None:
        """Test QueueInfo with idle state."""
        from eventsource.adapters.rabbitmq import QueueInfo

        info = QueueInfo(
            name="test-queue",
            message_count=5,
            consumer_count=0,
            state="idle",
        )

        assert info.state == "idle"
        assert info.consumer_count == 0

    def test_is_dataclass(self) -> None:
        """Test that QueueInfo is a proper dataclass."""
        from dataclasses import is_dataclass

        from eventsource.adapters.rabbitmq import QueueInfo

        assert is_dataclass(QueueInfo)

    def test_equality(self) -> None:
        """Test QueueInfo equality comparison."""
        from eventsource.adapters.rabbitmq import QueueInfo

        info1 = QueueInfo(name="q1", message_count=10, consumer_count=2)
        info2 = QueueInfo(name="q1", message_count=10, consumer_count=2)
        info3 = QueueInfo(name="q2", message_count=10, consumer_count=2)

        assert info1 == info2
        assert info1 != info3


class TestHealthCheckResult:
    """Tests for HealthCheckResult dataclass."""

    def test_healthy_result(self) -> None:
        """Test HealthCheckResult for healthy state."""
        from eventsource.adapters.rabbitmq import HealthCheckResult

        result = HealthCheckResult(
            healthy=True,
            connection_status="connected",
            channel_status="open",
            queue_status="accessible",
        )

        assert result.healthy is True
        assert result.connection_status == "connected"
        assert result.channel_status == "open"
        assert result.queue_status == "accessible"
        assert result.error is None

    def test_unhealthy_result(self) -> None:
        """Test HealthCheckResult for unhealthy state."""
        from eventsource.adapters.rabbitmq import HealthCheckResult

        result = HealthCheckResult(
            healthy=False,
            connection_status="disconnected",
            channel_status="not_initialized",
            queue_status="not_initialized",
            error="Not connected to RabbitMQ",
        )

        assert result.healthy is False
        assert result.error == "Not connected to RabbitMQ"

    def test_with_dlq_status(self) -> None:
        """Test HealthCheckResult with DLQ status."""
        from eventsource.adapters.rabbitmq import HealthCheckResult

        result = HealthCheckResult(
            healthy=True,
            connection_status="connected",
            channel_status="open",
            queue_status="accessible",
            dlq_status="accessible",
        )

        assert result.dlq_status == "accessible"

    def test_with_dlq_disabled(self) -> None:
        """Test HealthCheckResult with DLQ disabled."""
        from eventsource.adapters.rabbitmq import HealthCheckResult

        result = HealthCheckResult(
            healthy=True,
            connection_status="connected",
            channel_status="open",
            queue_status="accessible",
            dlq_status="disabled",
        )

        assert result.dlq_status == "disabled"

    def test_with_details(self) -> None:
        """Test HealthCheckResult with details dictionary."""
        from eventsource.adapters.rabbitmq import HealthCheckResult

        details = {
            "exchange": "test-exchange",
            "queue": "test-queue",
            "consumer_group": "test-group",
            "consuming": True,
        }

        result = HealthCheckResult(
            healthy=True,
            connection_status="connected",
            channel_status="open",
            queue_status="accessible",
            details=details,
        )

        assert result.details == details
        assert result.details["exchange"] == "test-exchange"

    def test_is_dataclass(self) -> None:
        """Test that HealthCheckResult is a proper dataclass."""
        from dataclasses import is_dataclass

        from eventsource.adapters.rabbitmq import HealthCheckResult

        assert is_dataclass(HealthCheckResult)
