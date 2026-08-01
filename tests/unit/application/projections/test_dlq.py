"""Unit tests for application/projections/dlq.py."""

import logging
from uuid import UUID

import pytest
from pydantic import Field

from eventsource.adapters.memory.dlq import InMemoryDLQRepository
from eventsource.application.projections.dlq import read_failed_events, send_to_dlq
from eventsource.domain.event import DomainEvent
from eventsource.observability import create_tracer


class SampleEvent(DomainEvent):
    """Sample event for DLQ tests."""

    event_type: str = "SampleEvent"
    aggregate_type: str = "Sample"

    value: int = Field(default=0)


class RaisingDLQRepository:
    """DLQ repository stub whose write/read methods always raise."""

    async def add_failed_event(self, **kwargs):
        raise RuntimeError("dlq write failed")

    async def get_failed_events(self, **kwargs):
        raise RuntimeError("dlq read failed")


@pytest.fixture
def tracer():
    return create_tracer(__name__, False)


@pytest.fixture
def repo():
    return InMemoryDLQRepository()


async def test_send_to_dlq_returns_true_and_stores_entry(repo, tracer):
    event = SampleEvent(aggregate_id=UUID(int=1), aggregate_version=1)
    error = ValueError("boom")

    result = await send_to_dlq(repo, "MyProjection", event, error, 3, tracer)

    assert result is True
    entries = await read_failed_events(repo, "MyProjection", tracer)
    assert len(entries) == 1
    assert entries[0].event_id == event.event_id


async def test_send_to_dlq_returns_false_and_logs_critical_on_write_failure(caplog, tracer):
    event = SampleEvent(aggregate_id=UUID(int=1), aggregate_version=1)
    error = ValueError("boom")
    repo = RaisingDLQRepository()

    with caplog.at_level(logging.CRITICAL):
        result = await send_to_dlq(repo, "MyProjection", event, error, 3, tracer)

    assert result is False
    assert any(record.levelno == logging.CRITICAL for record in caplog.records)


async def test_read_failed_events_returns_entries_for_named_projection_only(repo, tracer):
    event_a = SampleEvent(aggregate_id=UUID(int=1), aggregate_version=1)
    event_b = SampleEvent(aggregate_id=UUID(int=2), aggregate_version=1)

    await send_to_dlq(repo, "ProjectionA", event_a, ValueError("a"), 1, tracer)
    await send_to_dlq(repo, "ProjectionB", event_b, ValueError("b"), 1, tracer)

    entries = await read_failed_events(repo, "ProjectionA", tracer)

    assert len(entries) == 1
    assert entries[0].event_id == event_a.event_id


async def test_read_failed_events_returns_empty_list_and_logs_error_on_failure(caplog, tracer):
    repo = RaisingDLQRepository()

    with caplog.at_level(logging.ERROR):
        entries = await read_failed_events(repo, "MyProjection", tracer)

    assert entries == []
    assert any(record.levelno == logging.ERROR for record in caplog.records)
