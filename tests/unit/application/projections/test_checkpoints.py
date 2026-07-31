"""Unit tests for application/projections/checkpoints.py."""

from uuid import UUID

import pytest
from pydantic import Field

from eventsource.adapters.memory.checkpoints import InMemoryCheckpointRepository
from eventsource.application.projections.checkpoints import (
    lag_metrics_dict,
    read_checkpoint,
    record_checkpoint,
    reset_checkpoint,
)
from eventsource.events.base import DomainEvent
from eventsource.observability import create_tracer


class SampleEvent(DomainEvent):
    """Sample event for checkpoint tests."""

    event_type: str = "SampleEvent"
    aggregate_type: str = "Sample"

    value: int = Field(default=0)


@pytest.fixture
def tracer():
    return create_tracer(__name__, False)


@pytest.fixture
def repo():
    return InMemoryCheckpointRepository()


async def test_record_checkpoint_writes(repo, tracer):
    event = SampleEvent(aggregate_id=UUID(int=1), aggregate_version=1)

    await record_checkpoint(repo, "MyProjection", event, tracer)

    checkpoint = await read_checkpoint(repo, "MyProjection", tracer)
    assert checkpoint == str(event.event_id)


async def test_record_checkpoint_second_call_increments_events_processed(repo, tracer):
    event1 = SampleEvent(aggregate_id=UUID(int=1), aggregate_version=1)
    event2 = SampleEvent(aggregate_id=UUID(int=1), aggregate_version=2)

    await record_checkpoint(repo, "MyProjection", event1, tracer)
    await record_checkpoint(repo, "MyProjection", event2, tracer)

    metrics = await lag_metrics_dict(repo, "MyProjection", None, tracer)
    assert metrics is not None
    assert metrics["events_processed"] == 2


async def test_read_checkpoint_returns_string_not_uuid(repo, tracer):
    event = SampleEvent(aggregate_id=UUID(int=1), aggregate_version=1)
    await record_checkpoint(repo, "MyProjection", event, tracer)

    checkpoint = await read_checkpoint(repo, "MyProjection", tracer)

    assert isinstance(checkpoint, str)
    assert checkpoint == str(event.event_id)


async def test_read_checkpoint_returns_none_when_absent(repo, tracer):
    checkpoint = await read_checkpoint(repo, "MissingProjection", tracer)

    assert checkpoint is None


async def test_lag_metrics_dict_returns_six_keys_when_checkpoint_exists(repo, tracer):
    event = SampleEvent(aggregate_id=UUID(int=1), aggregate_version=1)
    await record_checkpoint(repo, "MyProjection", event, tracer)

    metrics = await lag_metrics_dict(repo, "MyProjection", None, tracer)

    assert metrics is not None
    assert set(metrics.keys()) == {
        "projection_name",
        "last_event_id",
        "latest_event_id",
        "lag_seconds",
        "events_processed",
        "last_processed_at",
    }


async def test_lag_metrics_dict_returns_none_when_repo_returns_none(repo, tracer):
    metrics = await lag_metrics_dict(repo, "MissingProjection", None, tracer)

    assert metrics is None


async def test_reset_checkpoint_makes_read_checkpoint_return_none(repo, tracer):
    event = SampleEvent(aggregate_id=UUID(int=1), aggregate_version=1)
    await record_checkpoint(repo, "MyProjection", event, tracer)
    assert await read_checkpoint(repo, "MyProjection", tracer) is not None

    await reset_checkpoint(repo, "MyProjection", tracer)

    assert await read_checkpoint(repo, "MyProjection", tracer) is None
