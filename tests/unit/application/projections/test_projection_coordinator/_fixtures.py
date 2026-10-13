"""Shared fixtures and event models for projection coordinator tests."""

import pytest
from pydantic import Field

from eventsource.application.projections.coordinator import (
    ProjectionCoordinator,
    ProjectionRegistry,
    SubscriberRegistry,
)
from eventsource.domain.event import DomainEvent


class OrderCreated(DomainEvent):
    """Sample event for testing."""

    aggregate_type: str = "Order"
    order_number: str = Field(..., description="Order number")


class OrderShipped(DomainEvent):
    """Sample event for testing."""

    aggregate_type: str = "Order"
    tracking_number: str = Field(..., description="Tracking number")


@pytest.fixture
def projection_registry() -> ProjectionRegistry:
    """Create a fresh projection registry."""
    return ProjectionRegistry()


@pytest.fixture
def subscriber_registry() -> SubscriberRegistry:
    """Create a fresh subscriber registry."""
    return SubscriberRegistry()


@pytest.fixture
def projection_coordinator(
    projection_registry: ProjectionRegistry,
) -> ProjectionCoordinator:
    """Create a projection coordinator."""
    return ProjectionCoordinator(registry=projection_registry)
