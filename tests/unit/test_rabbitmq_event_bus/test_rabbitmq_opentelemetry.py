"""Unit tests extracted for test_rabbitmq_opentelemetry.py."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from eventsource.adapters.rabbitmq import (
    OTEL_AVAILABLE,
    RabbitMQEventBus,
    RabbitMQEventBusConfig,
)
from eventsource.domain.event import DomainEvent


def _make_test_event() -> DomainEvent:
    class TestEvent(DomainEvent):
        aggregate_type: str = "TestAggregate"

    return TestEvent(aggregate_id=uuid4())


def _setup_aio_pika_mocks(mock_aio_pika: MagicMock) -> AsyncMock:
    mock_connection = AsyncMock(is_closed=False)
    mock_channel = AsyncMock(is_closed=False)
    mock_exchange = AsyncMock()
    mock_queue = AsyncMock()
    mock_queue.declaration_result.message_count = 0
    mock_queue.declaration_result.consumer_count = 0

    mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
    mock_connection.channel = AsyncMock(return_value=mock_channel)
    mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)
    mock_channel.declare_queue = AsyncMock(return_value=mock_queue)
    mock_exchange.publish = AsyncMock()
    return mock_exchange


class TestOpenTelemetryTracing:
    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            consumer_group="test-group",
            enable_tracing=True,
        )

    @pytest.fixture
    def config_no_tracing(self) -> RabbitMQEventBusConfig:
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            consumer_group="test-group",
            enable_tracing=False,
        )

    def test_otel_available_exported(self) -> None:
        assert isinstance(OTEL_AVAILABLE, bool)

    def test_tracer_is_none_when_tracing_disabled(
        self, config_no_tracing: RabbitMQEventBusConfig
    ) -> None:
        bus = RabbitMQEventBus(config=config_no_tracing)
        assert bus._publisher._tracer is not None
        assert bus._publisher._tracer.enabled is False
        assert bus._publisher._enable_tracing is False

    def test_tracer_set_when_enabled_and_available(self, config: RabbitMQEventBusConfig) -> None:
        bus = RabbitMQEventBus(config=config)
        assert bus._publisher._tracer is not None
        if OTEL_AVAILABLE:
            assert bus._publisher._enable_tracing is True
            assert bus._publisher._tracer.enabled is True
        else:
            assert bus._publisher._tracer.enabled is False

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_creates_span_when_tracing_enabled(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        if not OTEL_AVAILABLE:
            pytest.skip("OpenTelemetry not installed")

        _setup_aio_pika_mocks(mock_aio_pika)
        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        mock_tracer, mock_span = MagicMock(), MagicMock()
        mock_tracer.start_span.return_value = mock_span
        bus._publisher._tracer = mock_tracer

        await bus.publish([_make_test_event()])
        mock_tracer.start_span.assert_called()
        mock_span.end.assert_called()

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_span_has_correct_attributes(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        from eventsource.observability.attributes import (
            ATTR_EVENT_TYPE,
            ATTR_MESSAGING_DESTINATION,
            ATTR_MESSAGING_SYSTEM,
        )

        if not OTEL_AVAILABLE:
            pytest.skip("OpenTelemetry not installed")

        _setup_aio_pika_mocks(mock_aio_pika)
        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        mock_tracer, mock_span = MagicMock(), MagicMock()
        mock_tracer.start_span.return_value = mock_span
        bus._publisher._tracer = mock_tracer

        await bus.publish([_make_test_event()])
        call_args = mock_tracer.start_span.call_args
        assert "eventsource.event_bus.publish" in call_args[0][0]
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
        mock_exchange = _setup_aio_pika_mocks(mock_aio_pika)
        bus = RabbitMQEventBus(config=config_no_tracing)
        await bus.connect()

        assert bus._publisher._tracer is not None
        assert bus._publisher._tracer.enabled is False
        assert bus._publisher._enable_tracing is False

        await bus.publish([_make_test_event()])
        mock_exchange.publish.assert_called_once()

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_span_records_exception_on_error(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        if not OTEL_AVAILABLE:
            pytest.skip("OpenTelemetry not installed")

        mock_exchange = _setup_aio_pika_mocks(mock_aio_pika)
        mock_exchange.publish = AsyncMock(side_effect=Exception("Publish failed"))
        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        mock_tracer, mock_span = MagicMock(), MagicMock()
        mock_tracer.start_span.return_value = mock_span
        bus._publisher._tracer = mock_tracer

        with pytest.raises(Exception, match="Publish failed"):
            await bus.publish([_make_test_event()])

        mock_span.record_exception.assert_called_once()
        mock_span.end.assert_called()

    def test_create_message_with_tracing_injects_context(
        self, config: RabbitMQEventBusConfig
    ) -> None:
        if not OTEL_AVAILABLE:
            pytest.skip("OpenTelemetry not installed")

        event = _make_test_event()
        bus = RabbitMQEventBus(config=config)
        with patch("eventsource.adapters.rabbitmq.serialization.inject") as mock_inject:
            message = bus._create_message_with_tracing(event, MagicMock())
            mock_inject.assert_called_once()
            assert message is not None

    def test_create_message_with_tracing_without_span(self, config: RabbitMQEventBusConfig) -> None:
        event = _make_test_event()
        bus = RabbitMQEventBus(config=config)
        with patch("eventsource.adapters.rabbitmq.serialization.inject") as mock_inject:
            message = bus._create_message_with_tracing(event, None)
            mock_inject.assert_not_called()
            assert message is not None
