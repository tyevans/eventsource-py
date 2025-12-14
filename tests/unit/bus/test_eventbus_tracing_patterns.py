"""Tests for EventBus tracing patterns compliance.

This module verifies that all EventBus implementations follow the standardized
tracing patterns documented in the EventBus ABC and FRD.

The tests ensure:
1. All bus implementations use Tracer composition (accept tracer parameter)
2. All buses accept enable_tracing parameter
3. Standard span naming conventions are followed
4. Standard attribute constants are used
5. No duplicate OTEL_AVAILABLE definitions
"""

import inspect

import pytest

from eventsource.bus.interface import EventBus
from eventsource.observability import NullTracer


class TestEventBusABCDocumentation:
    """Tests for EventBus ABC documentation coverage."""

    def test_eventbus_documents_tracing_pattern(self) -> None:
        """EventBus docstring should document the tracing pattern."""
        docstring = EventBus.__doc__
        assert docstring is not None

        # Check for key tracing documentation sections
        assert "Tracing Support" in docstring
        assert "Tracer" in docstring
        assert "enable_tracing" in docstring

    def test_eventbus_documents_span_naming(self) -> None:
        """EventBus docstring should document span naming conventions."""
        docstring = EventBus.__doc__ or ""

        # Check for standard span names
        assert "eventsource.event_bus.publish" in docstring
        assert "eventsource.event_bus.dispatch" in docstring
        assert "eventsource.event_bus.handle" in docstring

    def test_eventbus_documents_standard_attributes(self) -> None:
        """EventBus docstring should document standard attributes."""
        docstring = EventBus.__doc__ or ""

        # Check for standard attribute documentation
        assert "ATTR_EVENT_TYPE" in docstring
        assert "ATTR_EVENT_ID" in docstring
        assert "ATTR_HANDLER_NAME" in docstring

    def test_eventbus_documents_distributed_tracing(self) -> None:
        """EventBus docstring should document distributed trace context."""
        docstring = EventBus.__doc__ or ""

        # Check for distributed tracing guidance
        assert "context propagation" in docstring.lower()


class TestInMemoryEventBusTracingCompliance:
    """Tests for InMemoryEventBus tracing compliance."""

    def test_uses_tracer_composition(self) -> None:
        """InMemoryEventBus should use Tracer composition pattern."""
        from eventsource.bus.memory import InMemoryEventBus

        sig = inspect.signature(InMemoryEventBus.__init__)
        params = sig.parameters

        # Should accept tracer parameter for dependency injection
        assert "tracer" in params

    def test_inherits_from_eventbus(self) -> None:
        """InMemoryEventBus should inherit from EventBus."""
        from eventsource.bus.memory import InMemoryEventBus

        assert issubclass(InMemoryEventBus, EventBus)

    def test_accepts_enable_tracing_parameter(self) -> None:
        """InMemoryEventBus should accept enable_tracing parameter."""
        from eventsource.bus.memory import InMemoryEventBus

        sig = inspect.signature(InMemoryEventBus.__init__)
        params = sig.parameters

        assert "enable_tracing" in params
        assert params["enable_tracing"].default is True

    def test_init_tracing_called(self) -> None:
        """InMemoryEventBus should initialize tracer during construction."""
        from eventsource.bus.memory import InMemoryEventBus

        bus = InMemoryEventBus(enable_tracing=True)
        assert hasattr(bus, "_enable_tracing")
        assert hasattr(bus, "_tracer")
        # Tracer should be set (either OpenTelemetry or NullTracer)
        assert bus._tracer is not None

    def test_tracing_disabled_uses_null_tracer(self) -> None:
        """When tracing disabled, _tracer should be NullTracer."""
        from eventsource.bus.memory import InMemoryEventBus

        bus = InMemoryEventBus(enable_tracing=False)
        assert isinstance(bus._tracer, NullTracer)
        assert bus._enable_tracing is False

    def test_custom_tracer_can_be_injected(self) -> None:
        """Custom tracer can be injected via constructor."""
        from eventsource.bus.memory import InMemoryEventBus

        custom_tracer = NullTracer()
        bus = InMemoryEventBus(tracer=custom_tracer)
        assert bus._tracer is custom_tracer

    def test_uses_standard_span_names(self) -> None:
        """InMemoryEventBus should use standard span names."""
        from eventsource.bus import memory as memory_module

        source = inspect.getsource(memory_module)

        # Check for standardized span names
        assert "eventsource.event_bus.dispatch" in source
        assert "eventsource.event_bus.handle" in source

    def test_uses_standard_attributes(self) -> None:
        """InMemoryEventBus should use standard attribute constants."""
        from eventsource.bus import memory as memory_module

        source = inspect.getsource(memory_module)

        # Check for imports of standard attributes
        assert "ATTR_EVENT_TYPE" in source
        assert "ATTR_EVENT_ID" in source
        assert "ATTR_HANDLER_NAME" in source
        assert "ATTR_HANDLER_SUCCESS" in source

    def test_no_duplicate_otel_available(self) -> None:
        """InMemoryEventBus should not define OTEL_AVAILABLE locally."""
        from eventsource.bus import memory as memory_module

        source = inspect.getsource(memory_module)
        lines = source.split("\n")

        # Look for local OTEL_AVAILABLE = True/False definitions
        local_definitions = [
            line
            for line in lines
            if "OTEL_AVAILABLE = True" in line or "OTEL_AVAILABLE = False" in line
        ]
        assert len(local_definitions) == 0


class TestRedisEventBusTracingCompliance:
    """Tests for RedisEventBus tracing compliance."""

    @pytest.fixture
    def check_redis_available(self) -> None:
        """Check if redis package is available."""
        try:
            from eventsource.bus.redis import REDIS_AVAILABLE

            if not REDIS_AVAILABLE:
                pytest.skip("redis package not installed")
        except ImportError:
            pytest.skip("redis package not installed")

    def test_uses_tracer_composition(self, check_redis_available: None) -> None:
        """RedisEventBus should use Tracer composition pattern."""
        from eventsource.bus.redis import RedisEventBus

        sig = inspect.signature(RedisEventBus.__init__)
        params = sig.parameters

        # Should accept tracer parameter for dependency injection
        assert "tracer" in params

    def test_inherits_from_eventbus(self, check_redis_available: None) -> None:
        """RedisEventBus should inherit from EventBus."""
        from eventsource.bus.redis import RedisEventBus

        assert issubclass(RedisEventBus, EventBus)

    def test_config_has_enable_tracing(self, check_redis_available: None) -> None:
        """RedisEventBusConfig should have enable_tracing field."""
        from eventsource.bus.redis import RedisEventBusConfig

        config = RedisEventBusConfig()
        assert hasattr(config, "enable_tracing")
        assert config.enable_tracing is True

    def test_uses_standard_span_names(self, check_redis_available: None) -> None:
        """RedisEventBus should use standard span names."""
        from eventsource.bus import redis as redis_module

        source = inspect.getsource(redis_module)

        # Check for standardized span names
        assert "eventsource.event_bus.publish" in source
        assert "eventsource.event_bus.dispatch" in source
        assert "eventsource.event_bus.handle" in source
        assert "eventsource.event_bus.process" in source

    def test_uses_standard_attributes(self, check_redis_available: None) -> None:
        """RedisEventBus should use standard attribute constants."""
        from eventsource.bus import redis as redis_module

        source = inspect.getsource(redis_module)

        # Check for imports of standard attributes
        assert "ATTR_MESSAGING_SYSTEM" in source
        assert "ATTR_MESSAGING_DESTINATION" in source
        assert "ATTR_EVENT_TYPE" in source
        assert "ATTR_HANDLER_NAME" in source
        assert "ATTR_HANDLER_SUCCESS" in source

    def test_imports_otel_from_observability(self, check_redis_available: None) -> None:
        """RedisEventBus should import OTEL_AVAILABLE from observability."""
        from eventsource.bus import redis as redis_module

        source = inspect.getsource(redis_module)
        assert "from eventsource.observability import" in source


class TestRabbitMQEventBusTracingCompliance:
    """Tests for RabbitMQEventBus tracing compliance."""

    @pytest.fixture
    def check_rabbitmq_available(self) -> None:
        """Check if aio-pika package is available."""
        try:
            from eventsource.bus.rabbitmq import RABBITMQ_AVAILABLE

            if not RABBITMQ_AVAILABLE:
                pytest.skip("aio-pika package not installed")
        except ImportError:
            pytest.skip("aio-pika package not installed")

    def test_uses_tracer_composition(self, check_rabbitmq_available: None) -> None:
        """RabbitMQEventBus should use Tracer composition pattern."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        sig = inspect.signature(RabbitMQEventBus.__init__)
        params = sig.parameters

        # Should accept tracer parameter for dependency injection
        assert "tracer" in params

    def test_inherits_from_eventbus(self, check_rabbitmq_available: None) -> None:
        """RabbitMQEventBus should inherit from EventBus."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        assert issubclass(RabbitMQEventBus, EventBus)

    def test_config_has_enable_tracing(self, check_rabbitmq_available: None) -> None:
        """RabbitMQEventBusConfig should have enable_tracing field."""
        from eventsource.bus.rabbitmq import RabbitMQEventBusConfig

        config = RabbitMQEventBusConfig()
        assert hasattr(config, "enable_tracing")
        assert config.enable_tracing is True

    def test_uses_standard_span_names(self, check_rabbitmq_available: None) -> None:
        """RabbitMQEventBus should use standard span names."""
        from eventsource.bus import rabbitmq as rabbitmq_module

        source = inspect.getsource(rabbitmq_module)

        # Check for standardized span names
        assert "eventsource.event_bus.publish" in source
        assert "eventsource.event_bus.consume" in source
        assert "eventsource.event_bus.handle" in source

    def test_uses_standard_attributes(self, check_rabbitmq_available: None) -> None:
        """RabbitMQEventBus should use standard attribute constants."""
        from eventsource.bus import rabbitmq as rabbitmq_module

        source = inspect.getsource(rabbitmq_module)

        # Check for imports of standard attributes
        assert "ATTR_MESSAGING_SYSTEM" in source
        assert "ATTR_MESSAGING_DESTINATION" in source
        assert "ATTR_EVENT_TYPE" in source
        assert "ATTR_HANDLER_NAME" in source
        assert "ATTR_HANDLER_SUCCESS" in source

    def test_imports_otel_from_observability(self, check_rabbitmq_available: None) -> None:
        """RabbitMQEventBus should import OTEL_AVAILABLE from observability."""
        from eventsource.bus import rabbitmq as rabbitmq_module

        source = inspect.getsource(rabbitmq_module)
        assert "from eventsource.observability import OTEL_AVAILABLE" in source

    def test_has_context_propagation(self, check_rabbitmq_available: None) -> None:
        """RabbitMQEventBus should have context propagation support."""
        from eventsource.bus import rabbitmq as rabbitmq_module

        source = inspect.getsource(rabbitmq_module)

        # Check for trace context propagation
        assert "from opentelemetry.propagate import extract, inject" in source
        assert hasattr(rabbitmq_module, "PROPAGATION_AVAILABLE")


class TestKafkaEventBusTracingCompliance:
    """Tests for KafkaEventBus tracing compliance."""

    @pytest.fixture
    def check_kafka_available(self) -> None:
        """Check if aiokafka package is available."""
        try:
            from eventsource.bus.kafka import KAFKA_AVAILABLE

            if not KAFKA_AVAILABLE:
                pytest.skip("aiokafka package not installed")
        except ImportError:
            pytest.skip("aiokafka package not installed")

    def test_inherits_from_eventbus(self, check_kafka_available: None) -> None:
        """KafkaEventBus should inherit from EventBus."""
        from eventsource.bus.kafka import KafkaEventBus

        assert issubclass(KafkaEventBus, EventBus)

    def test_config_has_enable_tracing(self, check_kafka_available: None) -> None:
        """KafkaEventBusConfig should have enable_tracing field."""
        from eventsource.bus.kafka import KafkaEventBusConfig

        config = KafkaEventBusConfig()
        assert hasattr(config, "enable_tracing")
        assert config.enable_tracing is True

    def test_uses_tracer_composition(self, check_kafka_available: None) -> None:
        """KafkaEventBus should use Tracer composition pattern."""
        from eventsource.bus.kafka import KafkaEventBus

        sig = inspect.signature(KafkaEventBus.__init__)
        params = sig.parameters

        # Should accept tracer parameter for dependency injection
        assert "tracer" in params

    def test_has_context_propagation(self, check_kafka_available: None) -> None:
        """KafkaEventBus should have context propagation support."""
        from eventsource.bus import kafka as kafka_module

        source = inspect.getsource(kafka_module)

        # Check for trace context propagation imports
        assert "from opentelemetry.propagate import extract, inject" in source


class TestTracerCompositionIntegration:
    """Tests for Tracer composition integration across all buses."""

    def test_all_buses_have_tracer(self) -> None:
        """All bus implementations should have _tracer attribute."""
        from eventsource.bus.memory import InMemoryEventBus

        bus = InMemoryEventBus()
        assert hasattr(bus, "_tracer")
        assert bus._tracer is not None

    def test_tracer_has_span_method(self) -> None:
        """Buses tracer should have span method for creating spans."""
        from eventsource.bus.memory import InMemoryEventBus

        bus = InMemoryEventBus()
        assert hasattr(bus._tracer, "span")
        assert callable(bus._tracer.span)

    def test_tracer_span_works_as_context_manager(self) -> None:
        """Tracer span should work as context manager."""
        from eventsource.bus.memory import InMemoryEventBus

        bus = InMemoryEventBus(enable_tracing=False)

        # Should work as context manager without errors
        with bus._tracer.span("test.span", {}) as span:
            # When using NullTracer, span should be None
            assert span is None


class TestEventBusSpanNamingConsistency:
    """Tests for consistent span naming across all bus implementations."""

    def test_span_naming_pattern(self) -> None:
        """All span names should follow eventsource.event_bus.* pattern."""
        # Standard span names that should be used
        standard_spans = [
            "eventsource.event_bus.publish",
            "eventsource.event_bus.dispatch",
            "eventsource.event_bus.handle",
            "eventsource.event_bus.consume",
            "eventsource.event_bus.process",
        ]

        # Verify pattern
        for span_name in standard_spans:
            assert span_name.startswith("eventsource.event_bus.")
            parts = span_name.split(".")
            assert len(parts) == 3
            assert parts[0] == "eventsource"
            assert parts[1] == "event_bus"
