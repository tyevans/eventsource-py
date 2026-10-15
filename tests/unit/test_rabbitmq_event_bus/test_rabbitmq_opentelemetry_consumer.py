"""Unit tests extracted for test_rabbitmq_opentelemetry_consumer.py."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from eventsource.adapters.rabbitmq import OTEL_AVAILABLE, RabbitMQEventBus, RabbitMQEventBusConfig
from eventsource.domain.event import DomainEvent
from eventsource.domain.event_registry import EventRegistry
from eventsource.domain.exceptions import HandlerDispatchError


class _TestEvent(DomainEvent):
    aggregate_type: str = "TestAggregate"


@pytest.fixture
def config() -> RabbitMQEventBusConfig:
    return RabbitMQEventBusConfig(
        rabbitmq_url="amqp://test:test@localhost/",
        exchange_name="test-events",
        consumer_group="test-group",
        enable_tracing=True,
    )


class TestOpenTelemetryConsumerTracing:
    """Tests for OpenTelemetry consumer tracing (P3-001)."""

    @pytest.fixture
    def registry(self) -> EventRegistry:
        registry = EventRegistry()

        class TestEvent(DomainEvent):
            aggregate_type: str = "TestAggregate"

        registry.register(TestEvent)
        return registry

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_process_message_creates_consumer_span(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig, registry: EventRegistry
    ) -> None:
        if not OTEL_AVAILABLE:
            pytest.skip("OpenTelemetry not installed")

        mock_message = MagicMock(
            headers={
                "event_type": "TestEvent",
                "aggregate_type": "TestAggregate",
                "x-retry-count": 0,
            },
            routing_key="TestAggregate.TestEvent",
            message_id="test-message-id",
            body=(
                b'{"event_type": "TestEvent", "aggregate_type": "TestAggregate", '
                b'"aggregate_id": "12345678-1234-1234-1234-123456789012", "aggregate_version": 1}'
            ),
            ack=AsyncMock(),
        )
        bus = RabbitMQEventBus(config=config, event_registry=registry)
        handler_called: list[DomainEvent] = []

        async def test_handler(event: DomainEvent) -> None:
            handler_called.append(event)

        bus.subscribe(registry.get("TestEvent"), test_handler)
        mock_tracer, mock_span = MagicMock(), MagicMock()
        mock_tracer.start_span.return_value = mock_span
        bus._consumer._tracer = mock_tracer

        with patch("eventsource.adapters.rabbitmq.consumer.extract", return_value=None) as mock_ext:
            await bus._process_message(mock_message)
            mock_tracer.start_span.assert_called()
            mock_ext.assert_called()
            mock_span.end.assert_called()

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_dispatch_event_creates_handler_spans(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig, registry: EventRegistry
    ) -> None:
        if not OTEL_AVAILABLE:
            pytest.skip("OpenTelemetry not installed")

        bus = RabbitMQEventBus(config=config, event_registry=registry)

        async def test_handler(e: DomainEvent) -> None: ...

        bus.subscribe(_TestEvent, test_handler)
        mock_tracer, mock_parent_span, mock_handler_span = MagicMock(), MagicMock(), MagicMock()
        mock_tracer.start_span.return_value = mock_handler_span
        bus._consumer._tracer = mock_tracer

        await bus._dispatch_event(
            _TestEvent(aggregate_id=uuid4()), MagicMock(message_id="test-id"), mock_parent_span
        )
        mock_tracer.start_span.assert_called()
        assert "eventsource.event_bus.handle" in mock_tracer.start_span.call_args[0][0]
        mock_handler_span.end.assert_called()

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_dispatch_event_no_handler_spans_without_parent(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig, registry: EventRegistry
    ) -> None:
        if not OTEL_AVAILABLE:
            pytest.skip("OpenTelemetry not installed")

        bus = RabbitMQEventBus(config=config, event_registry=registry)
        handler_called: list[DomainEvent] = []

        async def test_handler(e: DomainEvent) -> None:
            handler_called.append(e)

        bus.subscribe(_TestEvent, test_handler)
        mock_tracer = MagicMock()
        bus._consumer._tracer = mock_tracer

        await bus._dispatch_event(
            _TestEvent(aggregate_id=uuid4()), MagicMock(message_id="test-id"), parent_span=None
        )
        assert len(handler_called) == 1
        mock_tracer.start_span.assert_not_called()

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_handler_span_records_exception_on_error(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig, registry: EventRegistry
    ) -> None:
        if not OTEL_AVAILABLE:
            pytest.skip("OpenTelemetry not installed")

        bus = RabbitMQEventBus(config=config, event_registry=registry)

        async def failing_handler(e: DomainEvent) -> None:
            raise ValueError("Handler failed")

        bus.subscribe(_TestEvent, failing_handler)
        mock_tracer, mock_parent_span, mock_handler_span = MagicMock(), MagicMock(), MagicMock()
        mock_tracer.start_span.return_value = mock_handler_span
        bus._consumer._tracer = mock_tracer

        with pytest.raises(HandlerDispatchError):
            await bus._dispatch_event(
                _TestEvent(aggregate_id=uuid4()), MagicMock(message_id="test-id"), mock_parent_span
            )
        mock_handler_span.record_exception.assert_called_once()
        mock_handler_span.end.assert_called()


class TestOpenTelemetryGracefulDegradation:
    """Tests for graceful degradation when OpenTelemetry is not available."""

    def test_tracer_none_when_otel_unavailable(self, config: RabbitMQEventBusConfig) -> None:
        bus = RabbitMQEventBus(config=config)
        assert bus._consumer._tracer is not None
        assert bus._consumer._tracer.enabled is bool(OTEL_AVAILABLE)

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_works_without_otel(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        mock_conn, mock_chan = AsyncMock(is_closed=False), AsyncMock(is_closed=False)
        mock_ex, mock_q = AsyncMock(), AsyncMock()
        mock_q.declaration_result.message_count = 0
        mock_q.declaration_result.consumer_count = 0
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_conn)
        mock_conn.channel = AsyncMock(return_value=mock_chan)
        mock_chan.declare_exchange = AsyncMock(return_value=mock_ex)
        mock_chan.declare_queue = AsyncMock(return_value=mock_q)
        mock_ex.publish = AsyncMock()

        bus = RabbitMQEventBus(config=config)
        await bus.connect()
        with patch("eventsource.adapters.rabbitmq.bus.OTEL_AVAILABLE", False):
            await bus.publish([_TestEvent(aggregate_id=uuid4())])
            mock_ex.publish.assert_called_once()

    def test_create_message_with_tracing_works_without_otel(
        self, config: RabbitMQEventBusConfig
    ) -> None:
        bus = RabbitMQEventBus(config=config)
        with patch("eventsource.adapters.rabbitmq.bus.OTEL_AVAILABLE", False):
            msg = bus._create_message_with_tracing(_TestEvent(aggregate_id=uuid4()), MagicMock())
            assert msg is not None
