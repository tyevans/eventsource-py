"""Tests for EventBus ABC documentation and InMemoryEventBus tracing compliance."""

import inspect

from eventsource.observability import NullTracer
from eventsource.ports.bus import EventBus


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
        from eventsource.adapters.memory.bus import InMemoryEventBus

        sig = inspect.signature(InMemoryEventBus.__init__)
        params = sig.parameters

        # Should accept tracer parameter for dependency injection
        assert "tracer" in params

    def test_inherits_from_eventbus(self) -> None:
        """InMemoryEventBus should inherit from EventBus."""
        from eventsource.adapters.memory.bus import InMemoryEventBus

        assert issubclass(InMemoryEventBus, EventBus)

    def test_accepts_enable_tracing_parameter(self) -> None:
        """InMemoryEventBus should accept enable_tracing parameter."""
        from eventsource.adapters.memory.bus import InMemoryEventBus

        sig = inspect.signature(InMemoryEventBus.__init__)
        params = sig.parameters

        assert "enable_tracing" in params
        assert params["enable_tracing"].default is True

    def test_init_tracing_called(self) -> None:
        """InMemoryEventBus should initialize tracer during construction."""
        from eventsource.adapters.memory.bus import InMemoryEventBus

        bus = InMemoryEventBus(enable_tracing=True)
        assert hasattr(bus, "_enable_tracing")
        assert hasattr(bus, "_tracer")
        # Tracer should be set (either OpenTelemetry or NullTracer)
        assert bus._tracer is not None

    def test_tracing_disabled_uses_null_tracer(self) -> None:
        """When tracing disabled, _tracer should be NullTracer."""
        from eventsource.adapters.memory.bus import InMemoryEventBus

        bus = InMemoryEventBus(enable_tracing=False)
        assert isinstance(bus._tracer, NullTracer)
        assert bus._enable_tracing is False

    def test_custom_tracer_can_be_injected(self) -> None:
        """Custom tracer can be injected via constructor."""
        from eventsource.adapters.memory.bus import InMemoryEventBus

        custom_tracer = NullTracer()
        bus = InMemoryEventBus(tracer=custom_tracer)
        assert bus._tracer is custom_tracer

    def test_uses_standard_span_names(self) -> None:
        """InMemoryEventBus should use standard span names."""
        from eventsource.adapters.memory import bus as memory_module

        source = inspect.getsource(memory_module)

        # Check for standardized span names
        assert "eventsource.event_bus.dispatch" in source
        assert "eventsource.event_bus.handle" in source

    def test_uses_standard_attributes(self) -> None:
        """InMemoryEventBus should use standard attribute constants."""
        from eventsource.adapters.memory import bus as memory_module

        source = inspect.getsource(memory_module)

        # Check for imports of standard attributes
        assert "ATTR_EVENT_TYPE" in source
        assert "ATTR_EVENT_ID" in source
        assert "ATTR_HANDLER_NAME" in source
        assert "ATTR_HANDLER_SUCCESS" in source

    def test_no_duplicate_otel_available(self) -> None:
        """InMemoryEventBus should not define OTEL_AVAILABLE locally."""
        from eventsource.adapters.memory import bus as memory_module

        source = inspect.getsource(memory_module)
        lines = source.split("\n")

        # Look for local OTEL_AVAILABLE = True/False definitions
        local_definitions = [
            line
            for line in lines
            if "OTEL_AVAILABLE = True" in line or "OTEL_AVAILABLE = False" in line
        ]
        assert len(local_definitions) == 0
