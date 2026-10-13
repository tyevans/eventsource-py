"""Barrel export facade for RedisEventBus tracing unit tests."""

from tests.unit.test_redis_event_bus_tracing._fixtures import (
    FailingTracingHandler,
    RedisTracingTestEvent,
    TracingTestHandler,
    bus,
    config,
    event_registry,
    mock_redis,
    mock_tracer,
)
from tests.unit.test_redis_event_bus_tracing.test_redis_tracing_consumer import (
    TestRedisEventBusDispatchSpanCreation,
    TestRedisEventBusHandlerSpanCreation,
    TestRedisEventBusProcessMessageSpanCreation,
)
from tests.unit.test_redis_event_bus_tracing.test_redis_tracing_core import (
    TestRedisEventBusPublishSpanCreation,
    TestRedisEventBusSpanNaming,
    TestRedisEventBusStandardAttributes,
    TestRedisEventBusTracerComposition,
    TestRedisEventBusTracingDisabled,
)

__all__ = [
    "FailingTracingHandler",
    "RedisTracingTestEvent",
    "TestRedisEventBusDispatchSpanCreation",  # pragma: allowlist secret
    "TestRedisEventBusHandlerSpanCreation",  # pragma: allowlist secret
    "TestRedisEventBusProcessMessageSpanCreation",  # pragma: allowlist secret
    "TestRedisEventBusPublishSpanCreation",  # pragma: allowlist secret
    "TestRedisEventBusSpanNaming",
    "TestRedisEventBusStandardAttributes",  # pragma: allowlist secret
    "TestRedisEventBusTracerComposition",  # pragma: allowlist secret
    "TestRedisEventBusTracingDisabled",  # pragma: allowlist secret
    "TracingTestHandler",
    "bus",
    "config",
    "event_registry",
    "mock_redis",
    "mock_tracer",
]
