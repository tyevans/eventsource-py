"""Barrel export facade for test_assertions package."""

from __future__ import annotations

from tests.unit.testing.test_assertions._fixtures import (
    OtherEvent,
    SampleCreated,
    SampleDeleted,
    SampleUpdated,
    empty_assertions,
    multi_event_assertions,
    single_event_assertions,
)
from tests.unit.testing.test_assertions.test_assertions_filtering import (
    TestAssertEventForAggregate,
    TestAssertEventWithFields,
    TestGetEventsOfType,
)
from tests.unit.testing.test_assertions.test_assertions_init import (
    TestEventAssertionsInit,
)
from tests.unit.testing.test_assertions.test_assertions_integration import (
    TestEventAssertionsIntegration,
)
from tests.unit.testing.test_assertions.test_assertions_publishing import (
    TestAssertEventPublished,
    TestAssertNoEventPublished,
    TestAssertNoEventsPublished,
)
from tests.unit.testing.test_assertions.test_assertions_sequences import (
    TestAssertEventCount,
    TestAssertEventSequence,
)

__all__ = [
    "OtherEvent",
    "SampleCreated",
    "SampleDeleted",
    "SampleUpdated",
    "TestAssertEventCount",
    "TestAssertEventForAggregate",
    "TestAssertEventPublished",
    "TestAssertEventSequence",
    "TestAssertEventWithFields",
    "TestAssertNoEventPublished",
    "TestAssertNoEventsPublished",
    "TestEventAssertionsInit",
    "TestEventAssertionsIntegration",
    "TestGetEventsOfType",
    "empty_assertions",
    "multi_event_assertions",
    "single_event_assertions",
]
