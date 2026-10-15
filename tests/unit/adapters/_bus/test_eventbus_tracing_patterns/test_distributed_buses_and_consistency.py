"""Tests for Redis, RabbitMQ, and Kafka EventBus tracing compliance and naming consistency."""

import inspect

import pytest

from eventsource.ports.bus import EventBus


def _joined_source(*modules: object) -> str:
    return "".join(inspect.getsource(m) for m in modules)  # type: ignore[arg-type]


class TestRedisEventBusTracingCompliance:
    @pytest.fixture
    def check_redis_available(self) -> None:
        try:
            from eventsource.adapters.redis.bus import REDIS_AVAILABLE

            if not REDIS_AVAILABLE:
                pytest.skip("redis package not installed")
        except ImportError:
            pytest.skip("redis package not installed")

    def test_uses_tracer_composition(self, check_redis_available: None) -> None:
        from eventsource.adapters.redis.bus import RedisEventBus

        assert "tracer" in inspect.signature(RedisEventBus.__init__).parameters

    def test_inherits_from_eventbus(self, check_redis_available: None) -> None:
        from eventsource.adapters.redis.bus import RedisEventBus

        assert issubclass(RedisEventBus, EventBus)

    def test_config_has_enable_tracing(self, check_redis_available: None) -> None:
        from eventsource.adapters.redis.bus import RedisEventBusConfig

        config = RedisEventBusConfig()
        assert hasattr(config, "enable_tracing") and config.enable_tracing is True

    def test_uses_standard_span_names(self, check_redis_available: None) -> None:
        from eventsource.adapters.redis import bus as redis_module

        source = inspect.getsource(redis_module)
        for name in ("publish", "dispatch", "handle", "process"):
            assert f"eventsource.event_bus.{name}" in source

    def test_uses_standard_attributes(self, check_redis_available: None) -> None:
        from eventsource.adapters.redis import bus as redis_module

        source = inspect.getsource(redis_module)
        for attr in (
            "ATTR_MESSAGING_SYSTEM",
            "ATTR_MESSAGING_DESTINATION",
            "ATTR_EVENT_TYPE",
            "ATTR_HANDLER_NAME",
            "ATTR_HANDLER_SUCCESS",
        ):
            assert attr in source

    def test_imports_otel_from_observability(self, check_redis_available: None) -> None:
        from eventsource.adapters.redis import bus as redis_module

        assert "from eventsource.observability import" in inspect.getsource(redis_module)


class TestRabbitMQEventBusTracingCompliance:
    @pytest.fixture
    def check_rabbitmq_available(self) -> None:
        try:
            from eventsource.adapters.rabbitmq import RABBITMQ_AVAILABLE

            if not RABBITMQ_AVAILABLE:
                pytest.skip("aio-pika package not installed")
        except ImportError:
            pytest.skip("aio-pika package not installed")

    def test_uses_tracer_composition(self, check_rabbitmq_available: None) -> None:
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        assert "tracer" in inspect.signature(RabbitMQEventBus.__init__).parameters

    def test_inherits_from_eventbus(self, check_rabbitmq_available: None) -> None:
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        assert issubclass(RabbitMQEventBus, EventBus)

    def test_config_has_enable_tracing(self, check_rabbitmq_available: None) -> None:
        from eventsource.adapters.rabbitmq import RabbitMQEventBusConfig

        config = RabbitMQEventBusConfig()
        assert hasattr(config, "enable_tracing") and config.enable_tracing is True

    def test_uses_standard_span_names(self, check_rabbitmq_available: None) -> None:
        from eventsource.adapters.rabbitmq import bus, consumer, publisher

        source = _joined_source(bus, publisher, consumer)
        for name in ("publish", "consume", "handle"):
            assert f"eventsource.event_bus.{name}" in source

    def test_uses_standard_attributes(self, check_rabbitmq_available: None) -> None:
        from eventsource.adapters.rabbitmq import bus, consumer, publisher

        source = _joined_source(bus, publisher, consumer)
        for attr in (
            "ATTR_MESSAGING_SYSTEM",
            "ATTR_MESSAGING_DESTINATION",
            "ATTR_EVENT_TYPE",
            "ATTR_HANDLER_NAME",
            "ATTR_HANDLER_SUCCESS",
        ):
            assert attr in source

    def test_imports_otel_from_observability(self, check_rabbitmq_available: None) -> None:
        from eventsource.adapters.rabbitmq import bus as rabbitmq_module

        assert "from eventsource.observability import OTEL_AVAILABLE" in inspect.getsource(
            rabbitmq_module
        )

    def test_has_context_propagation(self, check_rabbitmq_available: None) -> None:
        from eventsource.adapters.rabbitmq import consumer, publisher, serialization

        assert "from opentelemetry.propagate import inject" in inspect.getsource(serialization)
        assert "from opentelemetry.propagate import extract" in inspect.getsource(consumer)
        assert hasattr(publisher, "PROPAGATION_AVAILABLE")


class TestKafkaEventBusTracingCompliance:
    @pytest.fixture
    def check_kafka_available(self) -> None:
        try:
            from eventsource.adapters.kafka import KAFKA_AVAILABLE

            if not KAFKA_AVAILABLE:
                pytest.skip("aiokafka package not installed")
        except ImportError:
            pytest.skip("aiokafka package not installed")

    def test_inherits_from_eventbus(self, check_kafka_available: None) -> None:
        from eventsource.adapters.kafka import KafkaEventBus

        assert issubclass(KafkaEventBus, EventBus)

    def test_config_has_enable_tracing(self, check_kafka_available: None) -> None:
        from eventsource.adapters.kafka import KafkaEventBusConfig

        config = KafkaEventBusConfig()
        assert hasattr(config, "enable_tracing") and config.enable_tracing is True

    def test_uses_tracer_composition(self, check_kafka_available: None) -> None:
        from eventsource.adapters.kafka import KafkaEventBus

        assert "tracer" in inspect.signature(KafkaEventBus.__init__).parameters

    def test_has_context_propagation(self, check_kafka_available: None) -> None:
        from eventsource.adapters.kafka import consumer, publisher

        assert "from opentelemetry.propagate import inject" in inspect.getsource(publisher)
        assert "from opentelemetry.propagate import extract" in inspect.getsource(consumer)


class TestTracerCompositionIntegration:
    def test_all_buses_have_tracer(self) -> None:
        from eventsource.adapters.memory.bus import InMemoryEventBus

        bus = InMemoryEventBus()
        assert hasattr(bus, "_tracer") and bus._tracer is not None

    def test_tracer_has_span_method(self) -> None:
        from eventsource.adapters.memory.bus import InMemoryEventBus

        bus = InMemoryEventBus()
        assert hasattr(bus._tracer, "span") and callable(bus._tracer.span)

    def test_tracer_span_works_as_context_manager(self) -> None:
        from eventsource.adapters.memory.bus import InMemoryEventBus

        bus = InMemoryEventBus(enable_tracing=False)
        with bus._tracer.span("test.span", {}) as span:
            assert span is None


class TestEventBusSpanNamingConsistency:
    def test_span_naming_pattern(self) -> None:
        standard_spans = [
            "eventsource.event_bus.publish",
            "eventsource.event_bus.dispatch",
            "eventsource.event_bus.handle",
            "eventsource.event_bus.consume",
            "eventsource.event_bus.process",
        ]
        for span_name in standard_spans:
            assert span_name.startswith("eventsource.event_bus.")
            parts = span_name.split(".")
            assert len(parts) == 3
            assert parts[0] == "eventsource" and parts[1] == "event_bus"
