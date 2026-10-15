"""Unit tests for create_tracer() factory, usage patterns, and imports."""

from __future__ import annotations

from typing import Any

import pytest

from eventsource.observability import (
    OTEL_AVAILABLE,
    MockTracer,
    NullTracer,
    OpenTelemetryTracer,
    Tracer,
    create_tracer,
)


class TestCreateTracer:
    """Tests for create_tracer() factory function."""

    def test_returns_null_when_disabled(self):
        """Returns NullTracer when tracing is disabled."""
        tracer = create_tracer(__name__, enable_tracing=False)
        assert isinstance(tracer, NullTracer)

    @pytest.mark.skipif(not OTEL_AVAILABLE, reason="OTEL not installed")
    def test_returns_otel_when_enabled_and_available(self):
        """Returns OpenTelemetryTracer when enabled and OTEL available."""
        tracer = create_tracer(__name__, enable_tracing=True)
        assert isinstance(tracer, OpenTelemetryTracer)

    @pytest.mark.skipif(OTEL_AVAILABLE, reason="OTEL is installed")
    def test_returns_null_when_otel_not_available(self):
        """Returns NullTracer when OTEL not available."""
        tracer = create_tracer(__name__, enable_tracing=True)
        assert isinstance(tracer, NullTracer)

    def test_default_enable_tracing_is_true(self):
        """Default enable_tracing parameter is True."""
        tracer = create_tracer(__name__)
        # If OTEL is available, should return OpenTelemetryTracer
        # If OTEL is not available, should return NullTracer
        assert isinstance(tracer, NullTracer | OpenTelemetryTracer)

    def test_returns_tracer_protocol(self):
        """Return value implements Tracer protocol."""
        tracer = create_tracer(__name__, enable_tracing=True)
        assert isinstance(tracer, Tracer)

        tracer = create_tracer(__name__, enable_tracing=False)
        assert isinstance(tracer, Tracer)


class TestTracerUsagePatterns:
    """Tests demonstrating common tracer usage patterns."""

    def test_component_with_injected_tracer(self):
        """Component can receive tracer via dependency injection."""

        class MyComponent:
            def __init__(self, tracer: Tracer | None = None):
                self._tracer = tracer or NullTracer()

            def operation(self, item_id: str) -> str:
                with self._tracer.span("component.operation", {"item.id": item_id}):
                    return f"processed: {item_id}"

        # With NullTracer (no tracing)
        component = MyComponent()
        result = component.operation("123")
        assert result == "processed: 123"

        # With MockTracer (for testing)
        mock_tracer = MockTracer()
        component = MyComponent(tracer=mock_tracer)
        result = component.operation("456")
        assert result == "processed: 456"
        assert mock_tracer.spans == [("component.operation", {"item.id": "456"})]

    @pytest.mark.asyncio
    async def test_async_component_with_tracer(self):
        """Async component can use tracer."""

        class AsyncComponent:
            def __init__(self, tracer: Tracer | None = None):
                self._tracer = tracer or NullTracer()

            async def operation(self, item_id: str) -> str:
                with self._tracer.span("async.operation", {"item.id": item_id}):
                    return f"async: {item_id}"

        mock_tracer = MockTracer()
        component = AsyncComponent(tracer=mock_tracer)
        result = await component.operation("789")

        assert result == "async: 789"
        assert mock_tracer.span_names == ["async.operation"]

    def test_conditional_attribute_computation(self):
        """enabled property allows conditional expensive computation."""

        expensive_called = False

        def expensive_computation() -> dict[str, Any]:
            nonlocal expensive_called
            expensive_called = True
            return {"expensive": "value"}

        # With NullTracer, skip expensive computation
        tracer = NullTracer()
        with tracer.span("operation"):
            if tracer.enabled:
                _attrs = expensive_computation()
        assert expensive_called is False

        # With MockTracer, do expensive computation
        mock_tracer = MockTracer()
        with mock_tracer.span("operation"):
            if mock_tracer.enabled:
                _attrs = expensive_computation()
        assert expensive_called is True

    def test_create_tracer_with_enable_tracing_pattern(self):
        """create_tracer works with enable_tracing constructor pattern."""

        class MyStore:
            def __init__(self, enable_tracing: bool = True):
                self._tracer = create_tracer(__name__, enable_tracing)

            def save(self, item_id: str) -> str:
                with self._tracer.span("store.save", {"item.id": item_id}):
                    return f"saved: {item_id}"

        # Disabled tracing
        store = MyStore(enable_tracing=False)
        assert isinstance(store._tracer, NullTracer)
        result = store.save("123")
        assert result == "saved: 123"

    def test_tracer_with_explicit_override(self):
        """Explicit tracer overrides enable_tracing."""

        class MyStore:
            def __init__(
                self,
                tracer: Tracer | None = None,
                enable_tracing: bool = True,
            ):
                self._tracer = tracer or create_tracer(__name__, enable_tracing)

        # Explicit tracer takes precedence
        mock_tracer = MockTracer()
        store = MyStore(tracer=mock_tracer, enable_tracing=False)
        assert store._tracer is mock_tracer


class TestImports:
    """Test module imports work correctly."""

    def test_import_from_observability(self):
        """All tracer exports are importable from observability."""
        from eventsource.observability import (
            MockTracer,
            NullTracer,
            OpenTelemetryTracer,
            Tracer,
            create_tracer,
        )

        assert Tracer is not None
        assert NullTracer is not None
        assert OpenTelemetryTracer is not None
        assert MockTracer is not None
        assert create_tracer is not None

    def test_import_from_tracer_module(self):
        """All exports are importable from tracer module."""
        from eventsource.observability.tracer import (
            MockTracer,
            NullTracer,
            OpenTelemetryTracer,
            Tracer,
            create_tracer,
        )

        assert Tracer is not None
        assert NullTracer is not None
        assert OpenTelemetryTracer is not None
        assert MockTracer is not None
        assert create_tracer is not None

    def test_otel_available_still_exported(self):
        """OTEL_AVAILABLE is still exported from observability."""
        from eventsource.observability import OTEL_AVAILABLE

        assert isinstance(OTEL_AVAILABLE, bool)
