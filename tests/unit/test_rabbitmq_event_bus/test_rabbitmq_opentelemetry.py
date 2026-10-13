"""Unit tests extracted for test_rabbitmq_opentelemetry.py."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from eventsource.adapters.rabbitmq import (
    RabbitMQEventBus,
    RabbitMQEventBusConfig,
)
from eventsource.domain.event import DomainEvent


class TestOpenTelemetryTracing:
    """Tests for OpenTelemetry tracing integration (P3-001)."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration with tracing enabled."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            consumer_group="test-group",
            enable_tracing=True,
        )

    @pytest.fixture
    def config_no_tracing(self) -> RabbitMQEventBusConfig:
        """Create test configuration with tracing disabled."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            consumer_group="test-group",
            enable_tracing=False,
        )

    def test_otel_available_exported(self) -> None:
        """Test that OTEL_AVAILABLE is properly exported."""
        from eventsource.adapters.rabbitmq import OTEL_AVAILABLE

        # OTEL_AVAILABLE should be a boolean
        assert isinstance(OTEL_AVAILABLE, bool)

    def test_tracer_is_none_when_tracing_disabled(
        self, config_no_tracing: RabbitMQEventBusConfig
    ) -> None:
        """Test that _tracer is disabled when tracing is disabled."""
        bus = RabbitMQEventBus(config=config_no_tracing)

        # With composition-based tracing, _tracer is always set but disabled
        assert bus._tracer is not None
        assert bus._tracer.enabled is False
        assert bus._enable_tracing is False

    def test_tracer_set_when_enabled_and_available(self, config: RabbitMQEventBusConfig) -> None:
        """Test that _tracer is set when enabled and available."""
        from eventsource.adapters.rabbitmq import OTEL_AVAILABLE

        bus = RabbitMQEventBus(config=config)

        # With composition-based tracing, _tracer is always set
        assert bus._tracer is not None
        if OTEL_AVAILABLE:
            assert bus._enable_tracing is True
            assert bus._tracer.enabled is True
        else:
            assert bus._tracer.enabled is False

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_creates_span_when_tracing_enabled(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that publish creates a span when tracing is enabled."""
        from eventsource.adapters.rabbitmq import OTEL_AVAILABLE, RabbitMQEventBus

        if not OTEL_AVAILABLE:
            pytest.skip("OpenTelemetry not installed")

        # Set up mocks
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_exchange = AsyncMock()
        mock_queue = AsyncMock()
        mock_queue.declaration_result.message_count = 0
        mock_queue.declaration_result.consumer_count = 0

        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)
        mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)
        mock_channel.declare_queue = AsyncMock(return_value=mock_queue)
        mock_exchange.publish = AsyncMock()

        # Create test event
        class TestEvent(DomainEvent):
            aggregate_type: str = "TestAggregate"

        event = TestEvent(aggregate_id=uuid4())

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        # Mock the tracer via TracingMixin's _tracer attribute
        mock_tracer = MagicMock()
        mock_span = MagicMock()
        mock_tracer.start_span.return_value = mock_span
        bus._tracer = mock_tracer

        await bus.publish([event])

        # Verify span was created and ended
        mock_tracer.start_span.assert_called()
        mock_span.end.assert_called()

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_span_has_correct_attributes(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that publish span includes correct attributes."""
        from eventsource.adapters.rabbitmq import OTEL_AVAILABLE, RabbitMQEventBus
        from eventsource.observability.attributes import (
            ATTR_EVENT_TYPE,
            ATTR_MESSAGING_DESTINATION,
            ATTR_MESSAGING_SYSTEM,
        )

        if not OTEL_AVAILABLE:
            pytest.skip("OpenTelemetry not installed")

        # Set up mocks
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_exchange = AsyncMock()
        mock_queue = AsyncMock()
        mock_queue.declaration_result.message_count = 0
        mock_queue.declaration_result.consumer_count = 0

        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)
        mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)
        mock_channel.declare_queue = AsyncMock(return_value=mock_queue)
        mock_exchange.publish = AsyncMock()

        # Create test event
        class TestEvent(DomainEvent):
            aggregate_type: str = "TestAggregate"

        event = TestEvent(aggregate_id=uuid4())

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        # Mock the tracer via TracingMixin's _tracer attribute
        mock_tracer = MagicMock()
        mock_span = MagicMock()
        mock_tracer.start_span.return_value = mock_span
        bus._tracer = mock_tracer

        await bus.publish([event])

        # Verify span was created with correct name pattern (now uses eventsource.event_bus.publish)
        call_args = mock_tracer.start_span.call_args
        span_name = call_args[0][0]
        assert "eventsource.event_bus.publish" in span_name

        # Verify attributes were set using standard constants
        attributes = call_args[1]["attributes"]
        assert attributes[ATTR_MESSAGING_SYSTEM] == "rabbitmq"
        assert attributes[ATTR_MESSAGING_DESTINATION] == config.exchange_name
        assert attributes[ATTR_EVENT_TYPE] == "TestEvent"
        assert attributes["aggregate.type"] == "TestAggregate"

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_no_span_when_tracing_disabled(
        self, mock_aio_pika: MagicMock, config_no_tracing: RabbitMQEventBusConfig
    ) -> None:
        """Test that publish does not create span when tracing is disabled."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        # Set up mocks
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_exchange = AsyncMock()
        mock_queue = AsyncMock()
        mock_queue.declaration_result.message_count = 0
        mock_queue.declaration_result.consumer_count = 0

        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)
        mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)
        mock_channel.declare_queue = AsyncMock(return_value=mock_queue)
        mock_exchange.publish = AsyncMock()

        # Create test event
        class TestEvent(DomainEvent):
            aggregate_type: str = "TestAggregate"

        event = TestEvent(aggregate_id=uuid4())

        bus = RabbitMQEventBus(config=config_no_tracing)
        await bus.connect()

        # With composition-based tracing, _tracer is always set but disabled
        assert bus._tracer is not None
        assert bus._tracer.enabled is False
        assert bus._enable_tracing is False

        # Publish should still work without tracing
        await bus.publish([event])
        mock_exchange.publish.assert_called_once()

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_span_records_exception_on_error(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that publish span records exception when publish fails."""
        from eventsource.adapters.rabbitmq import OTEL_AVAILABLE, RabbitMQEventBus

        if not OTEL_AVAILABLE:
            pytest.skip("OpenTelemetry not installed")

        # Set up mocks
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_exchange = AsyncMock()
        mock_queue = AsyncMock()
        mock_queue.declaration_result.message_count = 0
        mock_queue.declaration_result.consumer_count = 0

        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)
        mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)
        mock_channel.declare_queue = AsyncMock(return_value=mock_queue)

        # Make publish fail
        publish_error = Exception("Publish failed")
        mock_exchange.publish = AsyncMock(side_effect=publish_error)

        # Create test event
        class TestEvent(DomainEvent):
            aggregate_type: str = "TestAggregate"

        event = TestEvent(aggregate_id=uuid4())

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        # Mock the tracer via TracingMixin's _tracer attribute
        mock_tracer = MagicMock()
        mock_span = MagicMock()
        mock_tracer.start_span.return_value = mock_span
        bus._tracer = mock_tracer

        with pytest.raises(Exception, match="Publish failed"):
            await bus.publish([event])

        # Verify exception was recorded on span
        mock_span.record_exception.assert_called_once()
        mock_span.end.assert_called()

    def test_create_message_with_tracing_injects_context(
        self, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that _create_message_with_tracing injects trace context."""
        from eventsource.adapters.rabbitmq import OTEL_AVAILABLE, RabbitMQEventBus

        if not OTEL_AVAILABLE:
            pytest.skip("OpenTelemetry not installed")

        # Create test event
        class TestEvent(DomainEvent):
            aggregate_type: str = "TestAggregate"

        event = TestEvent(aggregate_id=uuid4())

        bus = RabbitMQEventBus(config=config)

        # Mock inject to verify it gets called
        with patch("eventsource.adapters.rabbitmq.serialization.inject") as mock_inject:
            mock_span = MagicMock()
            message = bus._create_message_with_tracing(event, mock_span)

            # Verify inject was called
            mock_inject.assert_called_once()
            # Message should be created
            assert message is not None

    def test_create_message_with_tracing_without_span(self, config: RabbitMQEventBusConfig) -> None:
        """Test that _create_message_with_tracing works without span."""

        # Create test event
        class TestEvent(DomainEvent):
            aggregate_type: str = "TestAggregate"

        event = TestEvent(aggregate_id=uuid4())

        bus = RabbitMQEventBus(config=config)

        # Without span, inject should not be called
        with patch("eventsource.adapters.rabbitmq.serialization.inject") as mock_inject:
            message = bus._create_message_with_tracing(event, None)

            # Verify inject was not called
            mock_inject.assert_not_called()
            # Message should still be created
            assert message is not None
