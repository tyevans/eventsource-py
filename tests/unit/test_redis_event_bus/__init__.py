"""Barrel export facade for RedisEventBus unit tests."""

from ._fixtures import (
    FailingHandler,
    OrderHandler,
    SampleOrderCreated,
    SampleOrderShipped,
    SamplePaymentReceived,
    SampleSubscriber,
    SyncHandler,
    bus,
    config,
    event_registry,
    mock_redis,
)
from .test_redis_event_bus_config import (
    TestRedisEventBusConfig,
    TestRedisEventBusStats,
)
from .test_redis_event_bus_consumer import (
    TestRedisEventBusDeserialization,
    TestRedisEventBusDLQ,
    TestRedisEventBusMessageProcessing,
    TestRedisEventBusPendingRecovery,
)
from .test_redis_event_bus_core import (
    TestRedisEventBusConnection,
    TestRedisEventBusErrorHandling,
    TestRedisEventBusPublish,
    TestRedisEventBusShutdown,
    TestRedisEventBusStatistics,
    TestRedisEventBusStreamInfo,
)
from .test_redis_event_bus_pubsub import (
    TestRedisEventBusDispatch,
    TestRedisEventBusIntegration,
    TestRedisEventBusSubscription,
)

__all__ = [
    "FailingHandler",
    "OrderHandler",
    "SampleOrderCreated",
    "SampleOrderShipped",
    "SamplePaymentReceived",
    "SampleSubscriber",
    "SyncHandler",
    "TestRedisEventBusConfig",
    "TestRedisEventBusConnection",
    "TestRedisEventBusDLQ",
    "TestRedisEventBusDeserialization",
    "TestRedisEventBusDispatch",
    "TestRedisEventBusErrorHandling",
    "TestRedisEventBusIntegration",
    "TestRedisEventBusMessageProcessing",
    "TestRedisEventBusPendingRecovery",
    "TestRedisEventBusPublish",
    "TestRedisEventBusShutdown",
    "TestRedisEventBusStatistics",
    "TestRedisEventBusStats",
    "TestRedisEventBusStreamInfo",
    "TestRedisEventBusSubscription",
    "bus",
    "config",
    "event_registry",
    "mock_redis",
]
