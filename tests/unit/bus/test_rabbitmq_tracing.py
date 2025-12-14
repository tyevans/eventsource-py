"""Tests for RabbitMQEventBus tracing integration.

This module tests the OpenTelemetry tracing functionality of RabbitMQEventBus,
ensuring proper use of Tracer composition pattern and context propagation.
"""

import importlib.util
import inspect
from unittest.mock import MagicMock

import pytest

from eventsource.observability import NullTracer

# Check if aio-pika is available for these tests
AIO_PIKA_AVAILABLE = importlib.util.find_spec("aio_pika") is not None

# Skip all tests if aio-pika not available
pytestmark = pytest.mark.skipif(
    not AIO_PIKA_AVAILABLE,
    reason="aio-pika is not installed",
)


class TestRabbitMQEventBusTracingConfig:
    """Tests for RabbitMQEventBus tracing configuration."""

    def test_tracing_enabled_by_default(self) -> None:
        """Tracing should be enabled by default in configuration."""
        from eventsource.bus.rabbitmq import RabbitMQEventBusConfig

        config = RabbitMQEventBusConfig()
        assert config.enable_tracing is True

    def test_tracing_disabled_in_config(self) -> None:
        """When tracing is disabled in config, it should be reflected."""
        from eventsource.bus.rabbitmq import RabbitMQEventBusConfig

        config = RabbitMQEventBusConfig(enable_tracing=False)
        assert config.enable_tracing is False


class TestRabbitMQEventBusTracingComposition:
    """Tests for RabbitMQEventBus Tracer composition integration."""

    @pytest.fixture
    def mock_registry(self) -> MagicMock:
        """Create a mock event registry."""
        return MagicMock()

    def test_uses_tracer_composition(self) -> None:
        """RabbitMQEventBus should use Tracer composition pattern."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        sig = inspect.signature(RabbitMQEventBus.__init__)
        params = sig.parameters

        # Should accept tracer parameter for dependency injection
        assert "tracer" in params

    def test_init_tracing_called_on_construction(self, mock_registry: MagicMock) -> None:
        """Tracer should be initialized during construction."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus, RabbitMQEventBusConfig

        config = RabbitMQEventBusConfig(enable_tracing=True)
        bus = RabbitMQEventBus(config=config, event_registry=mock_registry)

        # Tracer should be set
        assert hasattr(bus, "_enable_tracing")
        assert hasattr(bus, "_tracer")
        assert bus._tracer is not None

    def test_tracing_disabled_uses_null_tracer(self, mock_registry: MagicMock) -> None:
        """When tracing is disabled, _tracer should be NullTracer."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus, RabbitMQEventBusConfig

        config = RabbitMQEventBusConfig(enable_tracing=False)
        bus = RabbitMQEventBus(config=config, event_registry=mock_registry)

        assert bus._enable_tracing is False
        assert isinstance(bus._tracer, NullTracer)

    def test_custom_tracer_can_be_injected(self, mock_registry: MagicMock) -> None:
        """Custom tracer can be injected via constructor."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus, RabbitMQEventBusConfig

        custom_tracer = NullTracer()
        config = RabbitMQEventBusConfig()
        bus = RabbitMQEventBus(config=config, event_registry=mock_registry, tracer=custom_tracer)

        assert bus._tracer is custom_tracer

    def test_tracer_has_span_method(self, mock_registry: MagicMock) -> None:
        """The tracer should have span method for creating spans."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus, RabbitMQEventBusConfig

        config = RabbitMQEventBusConfig()
        bus = RabbitMQEventBus(config=config, event_registry=mock_registry)

        assert hasattr(bus._tracer, "span")
        assert callable(bus._tracer.span)


class TestRabbitMQEventBusNoOTELAvailable:
    """Tests for RabbitMQEventBus when OpenTelemetry is not available."""

    @pytest.fixture
    def mock_registry(self) -> MagicMock:
        """Create a mock event registry."""
        return MagicMock()

    def test_bus_works_without_otel(self, mock_registry: MagicMock) -> None:
        """RabbitMQEventBus should work even if OTEL is not available."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus, RabbitMQEventBusConfig

        # This should not raise an exception even if OTEL is not installed
        config = RabbitMQEventBusConfig(enable_tracing=True)
        bus = RabbitMQEventBus(config=config, event_registry=mock_registry)

        # Bus should be created successfully
        assert bus is not None
        assert bus.config == config


class TestRabbitMQEventBusAttributeConstants:
    """Tests for standard attribute constant usage."""

    def test_uses_standard_attributes(self) -> None:
        """Verify that the code uses standard ATTR_* constants."""
        import inspect

        from eventsource.bus import rabbitmq as rabbitmq_module

        source_code = inspect.getsource(rabbitmq_module)

        # Check that standard attribute constants are imported and used
        assert "ATTR_MESSAGING_SYSTEM" in source_code
        assert "ATTR_MESSAGING_DESTINATION" in source_code
        assert "ATTR_EVENT_TYPE" in source_code
        assert "ATTR_EVENT_ID" in source_code
        assert "ATTR_HANDLER_NAME" in source_code
        assert "ATTR_HANDLER_SUCCESS" in source_code
        assert "ATTR_AGGREGATE_ID" in source_code

    def test_no_duplicate_otel_available(self) -> None:
        """Verify that OTEL_AVAILABLE is not duplicated in rabbitmq.py."""
        import inspect

        from eventsource.bus import rabbitmq as rabbitmq_module

        source_code = inspect.getsource(rabbitmq_module)

        # Should import from observability module, not define locally
        assert "from eventsource.observability import OTEL_AVAILABLE" in source_code

        # Should NOT have "OTEL_AVAILABLE = True" (local definition)
        lines = source_code.split("\n")
        local_definitions = [
            line
            for line in lines
            if "OTEL_AVAILABLE = True" in line or "OTEL_AVAILABLE = False" in line
        ]
        # The only assignments should be in the import statement check,
        # not as a local try/except definition
        assert len(local_definitions) == 0


class TestRabbitMQContextPropagation:
    """Tests for distributed trace context propagation."""

    def test_propagation_available_defined(self) -> None:
        """PROPAGATION_AVAILABLE should be defined for context propagation."""
        from eventsource.bus import rabbitmq as rabbitmq_module

        assert hasattr(rabbitmq_module, "PROPAGATION_AVAILABLE")

    def test_inject_imported_for_propagation(self) -> None:
        """inject function should be available for trace context injection."""
        import inspect

        from eventsource.bus import rabbitmq as rabbitmq_module

        source_code = inspect.getsource(rabbitmq_module)

        # Should import inject from opentelemetry.propagate
        assert "from opentelemetry.propagate import extract, inject" in source_code

    def test_extract_imported_for_propagation(self) -> None:
        """extract function should be available for trace context extraction."""
        import inspect

        from eventsource.bus import rabbitmq as rabbitmq_module

        source_code = inspect.getsource(rabbitmq_module)

        # Should import extract from opentelemetry.propagate
        assert "from opentelemetry.propagate import extract, inject" in source_code


class TestRabbitMQSpanNaming:
    """Tests for consistent span naming convention."""

    def test_span_names_follow_convention(self) -> None:
        """Span names should follow 'eventsource.event_bus.*' convention."""
        import inspect

        from eventsource.bus import rabbitmq as rabbitmq_module

        source_code = inspect.getsource(rabbitmq_module)

        # Check for standardized span names in the publish method
        assert "eventsource.event_bus.publish" in source_code

        # Check for standardized span names in the consume method
        assert "eventsource.event_bus.consume" in source_code

        # Check for standardized span names in the handle method
        assert "eventsource.event_bus.handle" in source_code
