"""Configuration and stats data model tests for RedisEventBus."""

from eventsource.adapters.redis.bus import (
    RedisEventBusConfig,
    RedisEventBusStats,
)

__all__ = [
    "TestRedisEventBusConfig",
    "TestRedisEventBusStats",
]


class TestRedisEventBusConfig:
    """Tests for RedisEventBusConfig."""

    def test_default_config(self) -> None:
        """Test default configuration values."""
        config = RedisEventBusConfig()

        assert config.redis_url == "redis://localhost:6379"
        assert config.stream_prefix == "events"
        assert config.consumer_group == "default"
        assert config.consumer_name is not None  # Auto-generated
        assert config.stream_read_count == 100
        assert config.block_ms == 5000
        assert config.max_retries == 3
        assert config.pending_idle_ms == 60000
        assert config.enable_dlq is True
        assert config.dlq_stream_suffix == "_dlq"
        assert config.enable_tracing is True

    def test_custom_config(self) -> None:
        """Test custom configuration values."""
        config = RedisEventBusConfig(
            redis_url="redis://custom:6380",
            stream_prefix="myapp",
            consumer_group="workers",
            consumer_name="worker-1",
            stream_read_count=50,
            max_retries=5,
        )

        assert config.redis_url == "redis://custom:6380"
        assert config.stream_prefix == "myapp"
        assert config.consumer_group == "workers"
        assert config.consumer_name == "worker-1"
        assert config.stream_read_count == 50
        assert config.max_retries == 5

    def test_stream_name_property(self) -> None:
        """Test stream name generation."""
        config = RedisEventBusConfig(stream_prefix="myapp")
        assert config.stream_name == "myapp:stream"

    def test_dlq_stream_name_property(self) -> None:
        """Test DLQ stream name generation."""
        config = RedisEventBusConfig(stream_prefix="myapp")
        assert config.dlq_stream_name == "myapp:stream_dlq"

    def test_get_retry_key(self) -> None:
        """Test retry key generation."""
        config = RedisEventBusConfig(stream_prefix="myapp")
        key = config.get_retry_key("msg-123")
        assert key == "myapp:retry:msg-123"

    def test_auto_generated_consumer_name(self) -> None:
        """Test that consumer name is auto-generated if not provided."""
        config = RedisEventBusConfig(consumer_name=None)
        assert config.consumer_name is not None
        assert "-" in config.consumer_name  # Format: hostname-uuid


class TestRedisEventBusStats:
    """Tests for RedisEventBusStats."""

    def test_default_stats(self) -> None:
        """Test default statistics values."""
        stats = RedisEventBusStats()

        assert stats.events_published == 0
        assert stats.events_consumed == 0
        assert stats.events_processed_success == 0
        assert stats.events_processed_failed == 0
        assert stats.messages_recovered == 0
        assert stats.messages_sent_to_dlq == 0
        assert stats.handler_errors == 0
        assert stats.reconnections == 0

    def test_stats_modification(self) -> None:
        """Test that stats can be modified."""
        stats = RedisEventBusStats()
        stats.events_published += 10
        stats.handler_errors += 2

        assert stats.events_published == 10
        assert stats.handler_errors == 2
