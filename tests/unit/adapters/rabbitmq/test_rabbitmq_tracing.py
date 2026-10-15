"""Tests for RabbitMQEventBus tracing integration."""

import importlib.util
import inspect
from unittest.mock import MagicMock

import pytest

from eventsource.observability import NullTracer

AIO_PIKA_AVAILABLE = importlib.util.find_spec("aio_pika") is not None

pytestmark = pytest.mark.skipif(not AIO_PIKA_AVAILABLE, reason="aio-pika is not installed")


class TestRabbitMQEventBusTracingConfig:
    """Tests for RabbitMQEventBus tracing configuration."""

    def test_tracing_enabled_by_default(self) -> None:
        from eventsource.adapters.rabbitmq import RabbitMQEventBusConfig

        config = RabbitMQEventBusConfig()
        assert config.enable_tracing is True

    def test_tracing_disabled_in_config(self) -> None:
        from eventsource.adapters.rabbitmq import RabbitMQEventBusConfig

        config = RabbitMQEventBusConfig(enable_tracing=False)
        assert config.enable_tracing is False


class TestRabbitMQEventBusTracingComposition:
    """Tests for RabbitMQEventBus Tracer composition integration."""

    @pytest.fixture
    def mock_registry(self) -> MagicMock:
        return MagicMock()

    def test_uses_tracer_composition(self) -> None:
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        sig = inspect.signature(RabbitMQEventBus.__init__)
        assert "tracer" in sig.parameters

    def test_init_tracing_called_on_construction(self, mock_registry: MagicMock) -> None:
        from eventsource.adapters.rabbitmq import RabbitMQEventBus, RabbitMQEventBusConfig

        config = RabbitMQEventBusConfig(enable_tracing=True)
        bus = RabbitMQEventBus(config=config, event_registry=mock_registry)

        assert hasattr(bus._publisher, "_enable_tracing")
        assert hasattr(bus._publisher, "_tracer")
        assert bus._publisher._tracer is not None
        assert bus._consumer._tracer is not None

    def test_tracing_disabled_uses_null_tracer(self, mock_registry: MagicMock) -> None:
        from eventsource.adapters.rabbitmq import RabbitMQEventBus, RabbitMQEventBusConfig

        config = RabbitMQEventBusConfig(enable_tracing=False)
        bus = RabbitMQEventBus(config=config, event_registry=mock_registry)

        assert bus._publisher._enable_tracing is False
        assert bus._consumer._enable_tracing is False
        assert isinstance(bus._publisher._tracer, NullTracer)
        assert isinstance(bus._consumer._tracer, NullTracer)

    def test_custom_tracer_can_be_injected(self, mock_registry: MagicMock) -> None:
        from eventsource.adapters.rabbitmq import RabbitMQEventBus, RabbitMQEventBusConfig

        custom_tracer = NullTracer()
        config = RabbitMQEventBusConfig()
        bus = RabbitMQEventBus(config=config, event_registry=mock_registry, tracer=custom_tracer)

        assert bus._publisher._tracer is custom_tracer
        assert bus._consumer._tracer is custom_tracer

    def test_tracer_has_span_method(self, mock_registry: MagicMock) -> None:
        from eventsource.adapters.rabbitmq import RabbitMQEventBus, RabbitMQEventBusConfig

        config = RabbitMQEventBusConfig()
        bus = RabbitMQEventBus(config=config, event_registry=mock_registry)

        assert hasattr(bus._publisher._tracer, "span")
        assert callable(bus._publisher._tracer.span)


class TestRabbitMQEventBusNoOTELAvailable:
    """Tests for RabbitMQEventBus when OpenTelemetry is not available."""

    @pytest.fixture
    def mock_registry(self) -> MagicMock:
        return MagicMock()

    def test_bus_works_without_otel(self, mock_registry: MagicMock) -> None:
        from eventsource.adapters.rabbitmq import RabbitMQEventBus, RabbitMQEventBusConfig

        config = RabbitMQEventBusConfig(enable_tracing=True)
        bus = RabbitMQEventBus(config=config, event_registry=mock_registry)

        assert bus is not None
        assert bus.config == config


class TestRabbitMQEventBusAttributeConstants:
    """Tests for standard attribute constant usage."""

    def test_uses_standard_attributes(self) -> None:
        from eventsource.adapters.rabbitmq import bus as rabbitmq_module
        from eventsource.adapters.rabbitmq import consumer as consumer_module
        from eventsource.adapters.rabbitmq import publisher as publisher_module

        source_code = (
            inspect.getsource(rabbitmq_module)
            + inspect.getsource(publisher_module)
            + inspect.getsource(consumer_module)
        )

        assert "ATTR_MESSAGING_SYSTEM" in source_code
        assert "ATTR_MESSAGING_DESTINATION" in source_code
        assert "ATTR_EVENT_TYPE" in source_code
        assert "ATTR_EVENT_ID" in source_code
        assert "ATTR_HANDLER_NAME" in source_code
        assert "ATTR_HANDLER_SUCCESS" in source_code
        assert "ATTR_AGGREGATE_ID" in source_code

    def test_no_duplicate_otel_available(self) -> None:
        from eventsource.adapters.rabbitmq import bus as rabbitmq_module

        source_code = inspect.getsource(rabbitmq_module)
        assert "from eventsource.observability import OTEL_AVAILABLE" in source_code

        local_definitions = [
            line
            for line in source_code.split("\n")
            if "OTEL_AVAILABLE = True" in line or "OTEL_AVAILABLE = False" in line
        ]
        assert len(local_definitions) == 0


class TestRabbitMQContextPropagation:
    """Tests for distributed trace context propagation."""

    def test_propagation_available_defined(self) -> None:
        from eventsource.adapters.rabbitmq import publisher as publisher_module

        assert hasattr(publisher_module, "PROPAGATION_AVAILABLE")

    def test_inject_imported_for_propagation(self) -> None:
        from eventsource.adapters.rabbitmq import serialization as serialization_module

        source_code = inspect.getsource(serialization_module)
        assert "from opentelemetry.propagate import inject" in source_code

    def test_extract_imported_for_propagation(self) -> None:
        from eventsource.adapters.rabbitmq import consumer as consumer_module

        source_code = inspect.getsource(consumer_module)
        assert "from opentelemetry.propagate import extract" in source_code


class TestRabbitMQSpanNaming:
    """Tests for consistent span naming convention."""

    def test_span_names_follow_convention(self) -> None:
        from eventsource.adapters.rabbitmq import bus as rabbitmq_module
        from eventsource.adapters.rabbitmq import consumer as consumer_module
        from eventsource.adapters.rabbitmq import publisher as publisher_module

        source_code = (
            inspect.getsource(rabbitmq_module)
            + inspect.getsource(publisher_module)
            + inspect.getsource(consumer_module)
        )

        assert "eventsource.event_bus.publish" in source_code
        assert "eventsource.event_bus.consume" in source_code
        assert "eventsource.event_bus.handle" in source_code
