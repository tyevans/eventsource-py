"""Barrel export facade for domain event tests."""

from tests.unit.domain.test_domain_event._fixtures import (
    OrderCreated,
    OrderShipped,
)
from tests.unit.domain.test_domain_event.test_causation import (
    TestDomainEventCausation,
    TestDomainEventMetadata,
)
from tests.unit.domain.test_domain_event.test_creation import (
    TestDomainEventCreation,
    TestDomainEventEquality,
    TestDomainEventImmutability,
    TestDomainEventStringRepresentation,
)
from tests.unit.domain.test_domain_event.test_serialization import (
    TestDomainEventMultiTenancy,
    TestDomainEventSerialization,
)
from tests.unit.domain.test_domain_event.test_validation import (
    TestAggregateTypePattern,
    TestDomainEventAggregateVersion,
    TestDomainEventSubclassing,
    TestDomainEventValidation,
    TestExtraForbid,
    TestTypesVocabulary,
)

__all__ = [
    "OrderCreated",
    "OrderShipped",
    "TestAggregateTypePattern",
    "TestDomainEventAggregateVersion",
    "TestDomainEventCausation",
    "TestDomainEventCreation",
    "TestDomainEventEquality",
    "TestDomainEventImmutability",
    "TestDomainEventMetadata",
    "TestDomainEventMultiTenancy",
    "TestDomainEventSerialization",
    "TestDomainEventStringRepresentation",
    "TestDomainEventSubclassing",
    "TestDomainEventValidation",
    "TestExtraForbid",
    "TestTypesVocabulary",
]
