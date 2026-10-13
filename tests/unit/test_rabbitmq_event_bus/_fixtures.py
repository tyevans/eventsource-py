"""Shared fixtures and test event definitions for RabbitMQ event bus tests."""

from __future__ import annotations

from eventsource.domain.event import DomainEvent


class SerializationTestEvent(DomainEvent):
    """Test event for serialization tests."""

    aggregate_type: str = "TestAggregate"
    data: str = "test"


class SerializationOrderCreatedEvent(DomainEvent):
    """Order created event for serialization tests."""

    event_type: str = "SerializationOrderCreated"
    aggregate_type: str = "Order"
    order_number: str
    customer_id: str


class PublishTestEvent(DomainEvent):
    """Test event for publish tests."""

    aggregate_type: str = "TestAggregate"
    data: str = "test"


class ConsumerTestEvent(DomainEvent):
    """Test event for consumer loop testing."""

    aggregate_type: str = "ConsumerTest"
    data: str = "test"
