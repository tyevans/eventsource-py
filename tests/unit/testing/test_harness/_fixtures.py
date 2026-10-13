"""Fixtures and sample event models for test harness tests."""

from __future__ import annotations

from eventsource.domain.event import DomainEvent
from eventsource.domain.event_registry import register_event


@register_event
class HarnessSampleEvent(DomainEvent):
    """Sample event for testing."""

    aggregate_type: str = "Sample"


@register_event
class HarnessOtherEvent(DomainEvent):
    """Another event type for testing filtering."""

    aggregate_type: str = "Other"
