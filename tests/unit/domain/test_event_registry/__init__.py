"""Barrel export facade for event_registry tests."""

from tests.unit.domain.test_event_registry._fixtures import (
    EventWithoutTypeDefault,
    OrderCreated,
    OrderShipped,
    PaymentReceived,
)
from tests.unit.domain.test_event_registry.test_concurrency import (
    TestRegistryIsolation,
    TestThreadSafety,
)
from tests.unit.domain.test_event_registry.test_default_registry import (
    TestDefaultRegistry,
)
from tests.unit.domain.test_event_registry.test_deserialization import (
    TestEventDeserialization,
)
from tests.unit.domain.test_event_registry.test_operations import (
    TestEventRegistryBasicOperations,
    TestEventRegistryLookup,
    TestEventTypeResolution,
    TestRegistryIteration,
)
from tests.unit.domain.test_event_registry.test_registration import (
    TestDecoratorRegistration,
    TestDuplicateRegistration,
    TestErrorMessages,
    TestRegistryClearAndUnregister,
    TestVersionedEventSchemas,
)

__all__ = [
    "EventWithoutTypeDefault",
    "OrderCreated",
    "OrderShipped",
    "PaymentReceived",
    "TestDecoratorRegistration",
    "TestDefaultRegistry",
    "TestDuplicateRegistration",
    "TestErrorMessages",
    "TestEventDeserialization",
    "TestEventRegistryBasicOperations",
    "TestEventRegistryLookup",
    "TestEventTypeResolution",
    "TestRegistryClearAndUnregister",
    "TestRegistryIsolation",
    "TestRegistryIteration",
    "TestThreadSafety",
    "TestVersionedEventSchemas",
]
