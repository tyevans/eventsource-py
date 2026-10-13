"""Event bus unit tests package."""

from tests.unit.test_event_bus._fixtures import (
    FailingHandler,
    OrderCancelled,
    OrderCreated,
    OrderShipped,
    RecordingHandler,
    SampleAsyncEventHandler,
    SelectiveSubscriber,
    SlowHandler,
    SyncRecordingHandler,
    event_bus,
    sample_order_cancelled,
    sample_order_created,
    sample_order_shipped,
)
from tests.unit.test_event_bus.test_event_bus_handlers import (
    TestAsyncEventHandlerABC,
    TestEventHandlerProtocol,
    TestEventSubscriberProtocol,
    TestInvalidHandlerTypes,
)
from tests.unit.test_event_bus.test_event_bus_publishing import (
    TestInMemoryEventBusBackgroundPublishing,
    TestInMemoryEventBusBasicPublishing,
    TestInMemoryEventBusErrorHandling,
    TestInMemoryEventBusMultipleHandlers,
    TestInMemoryEventBusSyncAsyncSupport,
)
from tests.unit.test_event_bus.test_event_bus_subscriptions import (
    TestEventOrdering,
    TestInMemoryEventBusShutdown,
    TestInMemoryEventBusStatistics,
    TestInMemoryEventBusSubscribeAll,
    TestInMemoryEventBusSubscriberManagement,
    TestInMemoryEventBusThreadSafety,
    TestInMemoryEventBusUnsubscribe,
    TestInMemoryEventBusWildcardSubscriptions,
)

__all__ = [
    "FailingHandler",
    "OrderCancelled",
    "OrderCreated",
    "OrderShipped",
    "RecordingHandler",
    "SampleAsyncEventHandler",
    "SelectiveSubscriber",
    "SlowHandler",
    "SyncRecordingHandler",
    "TestAsyncEventHandlerABC",
    "TestEventHandlerProtocol",
    "TestEventOrdering",
    "TestEventSubscriberProtocol",
    "TestInMemoryEventBusBackgroundPublishing",  # pragma: allowlist secret
    "TestInMemoryEventBusBasicPublishing",  # pragma: allowlist secret
    "TestInMemoryEventBusErrorHandling",  # pragma: allowlist secret
    "TestInMemoryEventBusMultipleHandlers",  # pragma: allowlist secret
    "TestInMemoryEventBusShutdown",  # pragma: allowlist secret
    "TestInMemoryEventBusStatistics",  # pragma: allowlist secret
    "TestInMemoryEventBusSubscribeAll",  # pragma: allowlist secret
    "TestInMemoryEventBusSubscriberManagement",  # pragma: allowlist secret
    "TestInMemoryEventBusSyncAsyncSupport",  # pragma: allowlist secret
    "TestInMemoryEventBusThreadSafety",  # pragma: allowlist secret
    "TestInMemoryEventBusUnsubscribe",  # pragma: allowlist secret
    "TestInMemoryEventBusWildcardSubscriptions",  # pragma: allowlist secret
    "TestInvalidHandlerTypes",
    "event_bus",
    "sample_order_cancelled",
    "sample_order_created",
    "sample_order_shipped",
]
