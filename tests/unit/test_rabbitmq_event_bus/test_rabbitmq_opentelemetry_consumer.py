"""Unit tests extracted for test_rabbitmq_opentelemetry_consumer.py."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from eventsource.adapters.rabbitmq import (
    RabbitMQEventBus,
    RabbitMQEventBusConfig,
)
from eventsource.domain.event import DomainEvent
from eventsource.domain.event_registry import EventRegistry


class TestOpenTelemetryConsumerTracing:
    """Tests for OpenTelemetry consumer tracing (P3-001)."""

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
    def registry(self) -> EventRegistry:
        """Create an event registry with test events."""
        registry = EventRegistry()

        class TestEvent(DomainEvent):
            aggregate_type: str = "TestAggregate"

        registry.register(TestEvent)
        return registry

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_process_message_creates_consumer_span(
        self,
        mock_aio_pika: MagicMock,
        config: RabbitMQEventBusConfig,
        registry: EventRegistry,
    ) -> None:
        """Test that _process_message creates a consumer span."""
        from eventsource.adapters.rabbitmq import OTEL_AVAILABLE, RabbitMQEventBus

        if not OTEL_AVAILABLE:
            pytest.skip("OpenTelemetry not installed")

        # Create mock message
        mock_message = MagicMock()
        mock_message.headers = {
            "event_type": "TestEvent",
            "aggregate_type": "TestAggregate",
            "x-retry-count": 0,
        }
        mock_message.routing_key = "TestAggregate.TestEvent"
        mock_message.message_id = "test-message-id"
        mock_message.body = b'{"event_type": "TestEvent", "aggregate_type": "TestAggregate", "aggregate_id": "12345678-1234-1234-1234-123456789012", "aggregate_version": 1}'
        mock_message.ack = AsyncMock()

        bus = RabbitMQEventBus(config=config, event_registry=registry)

        # Register a handler
        handler_called = []

        async def test_handler(event: DomainEvent) -> None:
            handler_called.append(event)

        bus.subscribe(registry.get("TestEvent"), test_handler)

        # Mock the tracer via TracingMixin's _tracer attribute
        mock_tracer = MagicMock()
        mock_span = MagicMock()
        mock_tracer.start_span.return_value = mock_span
        bus._tracer = mock_tracer

        # Also mock extract for context propagation
        with patch("eventsource.adapters.rabbitmq.consumer.extract") as mock_extract:
            mock_extract.return_value = None

            await bus._process_message(mock_message)

            # Verify span was created
            mock_tracer.start_span.assert_called()
            # Verify extract was called for context propagation
            mock_extract.assert_called()
            # Verify span was ended
            mock_span.end.assert_called()

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_dispatch_event_creates_handler_spans(
        self,
        mock_aio_pika: MagicMock,
        config: RabbitMQEventBusConfig,
        registry: EventRegistry,
    ) -> None:
        """Test that _dispatch_event creates handler spans."""
        from eventsource.adapters.rabbitmq import OTEL_AVAILABLE, RabbitMQEventBus

        if not OTEL_AVAILABLE:
            pytest.skip("OpenTelemetry not installed")

        # Create test event
        class TestEvent(DomainEvent):
            aggregate_type: str = "TestAggregate"

        event = TestEvent(aggregate_id=uuid4())

        # Create mock message
        mock_message = MagicMock()
        mock_message.message_id = "test-message-id"

        bus = RabbitMQEventBus(config=config, event_registry=registry)

        # Register a handler
        async def test_handler(e: DomainEvent) -> None:
            pass

        bus.subscribe(TestEvent, test_handler)

        # Mock the tracer via TracingMixin's _tracer attribute
        mock_tracer = MagicMock()
        mock_parent_span = MagicMock()
        mock_handler_span = MagicMock()
        mock_tracer.start_span.return_value = mock_handler_span
        bus._tracer = mock_tracer

        await bus._dispatch_event(event, mock_message, mock_parent_span)

        # Verify handler span was created
        mock_tracer.start_span.assert_called()
        call_args = mock_tracer.start_span.call_args
        span_name = call_args[0][0]
        # Updated to use new span naming convention
        assert "eventsource.event_bus.handle" in span_name

        # Verify handler span was ended
        mock_handler_span.end.assert_called()

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_dispatch_event_no_handler_spans_without_parent(
        self,
        mock_aio_pika: MagicMock,
        config: RabbitMQEventBusConfig,
        registry: EventRegistry,
    ) -> None:
        """Test that _dispatch_event does not create handler spans without parent span."""
        from eventsource.adapters.rabbitmq import OTEL_AVAILABLE, RabbitMQEventBus

        if not OTEL_AVAILABLE:
            pytest.skip("OpenTelemetry not installed")

        # Create test event
        class TestEvent(DomainEvent):
            aggregate_type: str = "TestAggregate"

        event = TestEvent(aggregate_id=uuid4())

        # Create mock message
        mock_message = MagicMock()
        mock_message.message_id = "test-message-id"

        bus = RabbitMQEventBus(config=config, event_registry=registry)

        # Register a handler
        handler_called = []

        async def test_handler(e: DomainEvent) -> None:
            handler_called.append(e)

        bus.subscribe(TestEvent, test_handler)

        # Mock the tracer via TracingMixin's _tracer attribute
        mock_tracer = MagicMock()
        bus._tracer = mock_tracer

        # Without parent_span, handler spans should not be created
        await bus._dispatch_event(event, mock_message, parent_span=None)

        # Handler should still be called
        assert len(handler_called) == 1
        # But no spans should be created (because parent_span is None)
        mock_tracer.start_span.assert_not_called()

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_handler_span_records_exception_on_error(
        self,
        mock_aio_pika: MagicMock,
        config: RabbitMQEventBusConfig,
        registry: EventRegistry,
    ) -> None:
        """Test that handler span records exception when handler fails."""
        from eventsource.adapters.rabbitmq import OTEL_AVAILABLE, RabbitMQEventBus

        if not OTEL_AVAILABLE:
            pytest.skip("OpenTelemetry not installed")

        # Create test event
        class TestEvent(DomainEvent):
            aggregate_type: str = "TestAggregate"

        event = TestEvent(aggregate_id=uuid4())

        # Create mock message
        mock_message = MagicMock()
        mock_message.message_id = "test-message-id"

        bus = RabbitMQEventBus(config=config, event_registry=registry)

        # Register a failing handler
        async def failing_handler(e: DomainEvent) -> None:
            raise ValueError("Handler failed")

        bus.subscribe(TestEvent, failing_handler)

        # Mock the tracer via TracingMixin's _tracer attribute
        mock_tracer = MagicMock()
        mock_parent_span = MagicMock()
        mock_handler_span = MagicMock()
        mock_tracer.start_span.return_value = mock_handler_span
        bus._tracer = mock_tracer

        from eventsource.domain.exceptions import HandlerDispatchError

        with pytest.raises(HandlerDispatchError):
            await bus._dispatch_event(event, mock_message, mock_parent_span)

        # Verify exception was recorded on handler span
        mock_handler_span.record_exception.assert_called_once()
        mock_handler_span.end.assert_called()


class TestOpenTelemetryGracefulDegradation:
    """Tests for graceful degradation when OpenTelemetry is not available."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration with tracing enabled."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            consumer_group="test-group",
            enable_tracing=True,
        )

    def test_tracer_none_when_otel_unavailable(self, config: RabbitMQEventBusConfig) -> None:
        """Test that _tracer is disabled gracefully when OTEL unavailable."""
        # With composition-based tracing, _tracer is always set but may be disabled
        bus = RabbitMQEventBus(config=config)

        # _tracer is always set with composition-based tracing
        assert bus._tracer is not None

        from eventsource.adapters.rabbitmq import OTEL_AVAILABLE

        if not OTEL_AVAILABLE:
            # When OTEL is not available, _tracer should be a NullTracer (disabled)
            assert bus._tracer.enabled is False
        else:
            # When OTEL is available, tracer should be enabled
            assert bus._tracer.enabled is True

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_works_without_otel(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that publish works correctly when OTEL is not available."""
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

        # Make OTEL unavailable
        with patch("eventsource.adapters.rabbitmq.bus.OTEL_AVAILABLE", False):
            # Should not raise any exceptions
            await bus.publish([event])

            # Verify publish was called
            mock_exchange.publish.assert_called_once()

    def test_create_message_with_tracing_works_without_otel(
        self, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that _create_message_with_tracing works when OTEL is unavailable."""

        # Create test event
        class TestEvent(DomainEvent):
            aggregate_type: str = "TestAggregate"

        event = TestEvent(aggregate_id=uuid4())

        bus = RabbitMQEventBus(config=config)

        # Make OTEL unavailable
        with patch("eventsource.adapters.rabbitmq.bus.OTEL_AVAILABLE", False):
            mock_span = MagicMock()
            # Should not raise, even with a span provided
            message = bus._create_message_with_tracing(event, mock_span)
            assert message is not None
