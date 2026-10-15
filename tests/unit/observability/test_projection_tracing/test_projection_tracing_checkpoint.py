"""Tests for CheckpointTrackingProjection tracing support."""

from __future__ import annotations

from unittest.mock import MagicMock, Mock
from uuid import uuid4

import pytest

from eventsource.adapters.memory.checkpoints import InMemoryCheckpointRepository
from eventsource.adapters.memory.dlq import InMemoryDLQRepository
from eventsource.application.projections.base import CheckpointTrackingProjection
from eventsource.application.projections.retry import ExponentialBackoffRetryPolicy
from eventsource.application.subscriptions.retry import RetryConfig
from eventsource.domain.event import DomainEvent
from eventsource.observability.attributes import (
    ATTR_EVENT_ID,
    ATTR_EVENT_TYPE,
    ATTR_PROJECTION_NAME,
    ATTR_RETRY_COUNT,
)

from .test_projection_tracing_order import OrderCreated


class _SampleProjection(CheckpointTrackingProjection):
    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [OrderCreated]

    async def _process_event(self, event: DomainEvent) -> None:
        pass


class _FailingProjection(CheckpointTrackingProjection):
    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [OrderCreated]

    async def _process_event(self, event: DomainEvent) -> None:
        raise ValueError("Test error")


def _make_mock_tracer() -> tuple[Mock, MagicMock]:
    mock_tracer = Mock()
    mock_span = MagicMock()
    mock_span_cm = MagicMock()
    mock_span_cm.__enter__ = Mock(return_value=mock_span)
    mock_span_cm.__exit__ = Mock(return_value=None)
    mock_tracer.span.return_value = mock_span_cm
    mock_tracer.enabled = True
    return mock_tracer, mock_span


class TestCheckpointTrackingProjectionTracing:
    """Tests for CheckpointTrackingProjection tracing support."""

    def test_enable_tracing_parameter_default_false(self):
        projection = _SampleProjection()
        assert projection._enable_tracing is False
        assert projection._tracer is not None
        assert projection._tracer.enabled is False

    def test_enable_tracing_parameter_true(self):
        projection = _SampleProjection(enable_tracing=True)
        assert projection._enable_tracing is True
        assert projection._tracer is not None

    @pytest.mark.asyncio
    async def test_handle_creates_span_when_enabled(self):
        mock_tracer, _ = _make_mock_tracer()

        class TestProjection(_SampleProjection):
            pass

        projection = TestProjection(enable_tracing=True)
        projection._tracer = mock_tracer

        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        await projection.handle(event)

        mock_tracer.span.assert_called_once()
        call_args = mock_tracer.span.call_args
        assert call_args[0][0] == "eventsource.projection.handle"
        attrs = call_args[0][1]
        assert attrs[ATTR_PROJECTION_NAME] == "TestProjection"
        assert attrs[ATTR_EVENT_TYPE] == "OrderCreated"
        assert ATTR_EVENT_ID in attrs

    @pytest.mark.asyncio
    async def test_handle_no_span_when_disabled(self):
        projection = _SampleProjection(enable_tracing=False)
        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        await projection.handle(event)
        assert projection._tracer is not None
        assert projection._tracer.enabled is False

    @pytest.mark.asyncio
    async def test_span_sets_checkpoint_updated_on_success(self):
        mock_tracer, mock_span = _make_mock_tracer()
        projection = _SampleProjection(
            checkpoint_repo=InMemoryCheckpointRepository(), enable_tracing=True
        )
        projection._tracer = mock_tracer
        await projection.handle(OrderCreated(aggregate_id=uuid4(), order_number="ORD-001"))
        mock_span.set_attribute.assert_called_with("checkpoint.updated", True)

    @pytest.mark.asyncio
    async def test_span_sets_checkpoint_updated_false_without_repo(self):
        mock_tracer, mock_span = _make_mock_tracer()
        projection = _SampleProjection(enable_tracing=True)
        projection._tracer = mock_tracer
        await projection.handle(OrderCreated(aggregate_id=uuid4(), order_number="ORD-001"))
        mock_span.set_attribute.assert_called_with("checkpoint.updated", False)

    @pytest.mark.asyncio
    async def test_span_sets_retry_count_on_failure(self):
        mock_tracer, mock_span = _make_mock_tracer()
        retry_policy = ExponentialBackoffRetryPolicy(
            config=RetryConfig(
                max_retries=2, initial_delay=0.001, exponential_base=1.01, jitter=0.0
            )
        )
        projection = _FailingProjection(
            checkpoint_repo=InMemoryCheckpointRepository(),
            dlq_repo=InMemoryDLQRepository(),
            retry_policy=retry_policy,
            enable_tracing=True,
        )
        projection._tracer = mock_tracer
        with pytest.raises(ValueError):
            await projection.handle(OrderCreated(aggregate_id=uuid4(), order_number="ORD-001"))
        mock_span.set_attribute.assert_called_with(ATTR_RETRY_COUNT, 3)
