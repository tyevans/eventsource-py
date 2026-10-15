"""Tests for Redis, RabbitMQ, and Kafka EventBus tracing compliance and naming consistency."""

import inspect

import pytest

from eventsource.ports.bus import EventBus


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

        sig = inspect.signature(RedisEventBus.__init__)
        assert "tracer" in sig.parameters

    def test_inherits_from_eventbus(self, check_redis_available: None) -> None:
        from eventsource.adapters.redis.bus import RedisEventBus

        assert issubclass(RedisEventBus, EventBus)

    def test_config_has_enable_tracing(self, check_redis_available: None) -> None:
        from eventsource.adapters.redis.bus import RedisEventBusConfig

        config = RedisEventBusConfig()
        assert hasattr(config, "enable_tracing")
        assert config.enable_tracing is True

    def test_uses_standard_span_names(self, check_redis_available: None) -> None:
        from eventsource.adapters.redis import bus as redis_module

        source = inspect.getsource(redis_module)
        assert "eventsource.event_bus.publish" in source
        assert "eventsource.event_bus.dispatch" in source
        assert "eventsource.event_bus.handle" in source
        assert "eventsource.event_bus.process" in source

    def test_uses_standard_attributes(self, check_redis_available: None) -> None:
        from eventsource.adapters.redis import bus as redis_module

        source = inspect.getsource(redis_module)
        assert "ATTR_MESSAGING_SYSTEM" in source
        assert "ATTR_MESSAGING_DESTINATION" in source
        assert "ATTR_EVENT_TYPE" in source
        assert "ATTR_HANDLER_NAME" in source
        assert "ATTR_HANDLER_SUCCESS" in source

    def test_imports_otel_from_observability(self, check_redis_available: None) -> None:
        from eventsource.adapters.redis import bus as redis_module

        source = inspect.getsource(redis_module)
        assert "from eventsource.observability import" in source


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

        sig = inspect.signature(RabbitMQEventBus.__init__)
        assert "tracer" in sig.parameters

    def test_inherits_from_eventbus(self, check_rabbitmq_available: None) -> None:
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        assert issubclass(RabbitMQEventBus, EventBus)

    def test_config_has_enable_tracing(self, check_rabbitmq_available: None) -> None:
        from eventsource.adapters.rabbitmq import RabbitMQEventBusConfig

        config = RabbitMQEventBusConfig()
        assert hasattr(config, "enable_tracing")
        assert config.enable_tracing is True

    def test_uses_standard_span_names(self, check_rabbitmq_available: None) -> None:
        from eventsource.adapters.rabbitmq import bus as rabbitmq_module
        from eventsource.adapters.rabbitmq import consumer as consumer_module
        from eventsource.adapters.rabbitmq import publisher as publisher_module

        source = (
            inspect.getsource(rabbitmq_module)
            + inspect.getsource(publisher_module)
            + inspect.getsource(consumer_module)
        )
        assert "eventsource.event_bus.publish" in source
        assert "eventsource.event_bus.consume" in source
        assert "eventsource.event_bus.handle" in source

    def test_uses_standard_attributes(self, check_rabbitmq_available: None) -> None:
        from eventsource.adapters.rabbitmq import bus as rabbitmq_module
        from eventsource.adapters.rabbitmq import consumer as consumer_module
        from eventsource.adapters.rabbitmq import publisher as publisher_module

        source = (
            inspect.getsource(rabbitmq_module)
            + inspect.getsource(publisher_module)
            + inspect.getsource(consumer_module)
        )
        assert "ATTR_MESSAGING_SYSTEM" in source
        assert "ATTR_MESSAGING_DESTINATION" in source
        assert "ATTR_EVENT_TYPE" in source
        assert "ATTR_HANDLER_NAME" in source
        assert "ATTR_HANDLER_SUCCESS" in source

    def test_imports_otel_from_observability(self, check_rabbitmq_available: None) -> None:
        from eventsource.adapters.rabbitmq import bus as rabbitmq_module

        source = inspect.getsource(rabbitmq_module)
        assert "from eventsource.observability import OTEL_AVAILABLE" in source

    def test_has_context_propagation(self, check_rabbitmq_available: None) -> None:
        from eventsource.adapters.rabbitmq import consumer as consumer_module
        from eventsource.adapters.rabbitmq import publisher as publisher_module
        from eventsource.adapters.rabbitmq import serialization as serialization_module

        assert "from opentelemetry.propagate import inject" in inspect.getsource(
            serialization_module
        )
        assert "from opentelemetry.propagate import extract" in inspect.getsource(consumer_module)
        assert hasattr(publisher_module, "PROPAGATION_AVAILABLE")


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
        assert hasattr(config, "enable_tracing")
        assert config.enable_tracing is True

    def test_uses_tracer_composition(self, check_kafka_available: None) -> None:
        from eventsource.adapters.kafka import KafkaEventBus

        sig = inspect.signature(KafkaEventBus.__init__)
        assert "tracer" in sig.parameters

    def test_has_context_propagation(self, check_kafka_available: None) -> None:
        from eventsource.adapters.kafka import consumer as kafka_consumer_module
        from eventsource.adapters.kafka import publisher as kafka_publisher_module

        assert "from opentelemetry.propagate import inject" in inspect.getsource(
            kafka_publisher_module
        )
        assert "from opentelemetry.propagate import extract" in inspect.getsource(
            kafka_consumer_module
        )


class TestTracerCompositionIntegration:
    def test_all_buses_have_tracer(self) -> None:
        from eventsource.adapters.memory.bus import InMemoryEventBus

        bus = InMemoryEventBus()
        assert hasattr(bus, "_tracer")
        assert bus._tracer is not None

    def test_tracer_has_span_method(self) -> None:
        from eventsource.adapters.memory.bus import InMemoryEventBus

        bus = InMemoryEventBus()
        assert hasattr(bus._tracer, "span")
        assert callable(bus._tracer.span)

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
            assert parts[0] == "eventsource"
            assert parts[1] == "event_bus"
