"""Tests for CheckpointTrackingProjection tracing support."""

from __future__ import annotations

from unittest.mock import MagicMock, Mock
from uuid import uuid4

import pytest

from eventsource.domain.event import DomainEvent
from eventsource.observability.attributes import (
    ATTR_EVENT_ID,
    ATTR_EVENT_TYPE,
    ATTR_PROJECTION_NAME,
    ATTR_RETRY_COUNT,
)

from .test_projection_tracing_order import OrderCreated


class TestCheckpointTrackingProjectionTracing:
    """Tests for CheckpointTrackingProjection tracing support."""

    def test_enable_tracing_parameter_default_false(self):
        """enable_tracing defaults to False for projections."""
        from eventsource.application.projections.base import CheckpointTrackingProjection

        class TestProjection(CheckpointTrackingProjection):
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def _process_event(self, event: DomainEvent) -> None:
                pass

        projection = TestProjection()

        # Tracing should be off by default
        assert projection._enable_tracing is False
        # With composition-based tracing, _tracer is always set
        # but it will be a NullTracer when disabled
        assert projection._tracer is not None
        assert projection._tracer.enabled is False

    def test_enable_tracing_parameter_true(self):
        """enable_tracing=True enables tracing."""
        from eventsource.application.projections.base import CheckpointTrackingProjection

        class TestProjection(CheckpointTrackingProjection):
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def _process_event(self, event: DomainEvent) -> None:
                pass

        projection = TestProjection(enable_tracing=True)

        # Tracing should be enabled
        assert projection._enable_tracing is True
        assert projection._tracer is not None

    @pytest.mark.asyncio
    async def test_handle_creates_span_when_enabled(self):
        """handle() creates span when tracing enabled."""
        from eventsource.application.projections.base import CheckpointTrackingProjection

        mock_tracer = Mock()
        mock_span = MagicMock()
        mock_span_cm = MagicMock()
        mock_span_cm.__enter__ = Mock(return_value=mock_span)
        mock_span_cm.__exit__ = Mock(return_value=None)
        mock_tracer.span.return_value = mock_span_cm
        mock_tracer.enabled = True

        class TestProjection(CheckpointTrackingProjection):
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def _process_event(self, event: DomainEvent) -> None:
                pass

        projection = TestProjection(enable_tracing=True)
        projection._tracer = mock_tracer

        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        await projection.handle(event)

        # Verify span was created with correct name and attributes
        mock_tracer.span.assert_called_once()
        call_args = mock_tracer.span.call_args
        assert call_args[0][0] == "eventsource.projection.handle"
        attrs = call_args[0][1]
        assert attrs[ATTR_PROJECTION_NAME] == "TestProjection"
        assert attrs[ATTR_EVENT_TYPE] == "OrderCreated"
        assert ATTR_EVENT_ID in attrs

    @pytest.mark.asyncio
    async def test_handle_no_span_when_disabled(self):
        """handle() does not create span when tracing disabled."""
        from eventsource.application.projections.base import CheckpointTrackingProjection

        class TestProjection(CheckpointTrackingProjection):
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def _process_event(self, event: DomainEvent) -> None:
                pass

        projection = TestProjection(enable_tracing=False)

        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        await projection.handle(event)

        # With composition-based tracing, _tracer is always set but disabled
        assert projection._tracer is not None
        assert projection._tracer.enabled is False

    @pytest.mark.asyncio
    async def test_span_sets_checkpoint_updated_on_success(self):
        """checkpoint.updated reflects whether a checkpoint repo is configured."""
        from eventsource.adapters.memory.checkpoints import InMemoryCheckpointRepository
        from eventsource.application.projections.base import CheckpointTrackingProjection

        mock_tracer = Mock()
        mock_span = MagicMock()
        mock_span_cm = MagicMock()
        mock_span_cm.__enter__ = Mock(return_value=mock_span)
        mock_span_cm.__exit__ = Mock(return_value=None)
        mock_tracer.span.return_value = mock_span_cm
        mock_tracer.enabled = True

        class TestProjection(CheckpointTrackingProjection):
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def _process_event(self, event: DomainEvent) -> None:
                pass

        projection = TestProjection(
            checkpoint_repo=InMemoryCheckpointRepository(), enable_tracing=True
        )
        projection._tracer = mock_tracer

        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        await projection.handle(event)

        # Verify checkpoint.updated attribute was set when a checkpoint repo exists
        mock_span.set_attribute.assert_called_with("checkpoint.updated", True)

    @pytest.mark.asyncio
    async def test_span_sets_checkpoint_updated_false_without_repo(self):
        """checkpoint.updated is False when checkpoint tracking is disabled."""
        from eventsource.application.projections.base import CheckpointTrackingProjection

        mock_tracer = Mock()
        mock_span = MagicMock()
        mock_span_cm = MagicMock()
        mock_span_cm.__enter__ = Mock(return_value=mock_span)
        mock_span_cm.__exit__ = Mock(return_value=None)
        mock_tracer.span.return_value = mock_span_cm
        mock_tracer.enabled = True

        class TestProjection(CheckpointTrackingProjection):
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def _process_event(self, event: DomainEvent) -> None:
                pass

        projection = TestProjection(enable_tracing=True)
        projection._tracer = mock_tracer

        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        await projection.handle(event)

        # No checkpoint repo configured -- the attribute must say so truthfully
        mock_span.set_attribute.assert_called_with("checkpoint.updated", False)

    @pytest.mark.asyncio
    async def test_span_sets_retry_count_on_failure(self):
        """Failed event processing sets retry count attribute."""
        from eventsource.adapters.memory.checkpoints import InMemoryCheckpointRepository
        from eventsource.adapters.memory.dlq import InMemoryDLQRepository
        from eventsource.application.projections.base import CheckpointTrackingProjection
        from eventsource.application.projections.retry import ExponentialBackoffRetryPolicy
        from eventsource.application.subscriptions.retry import RetryConfig

        mock_tracer = Mock()
        mock_span = MagicMock()
        mock_span_cm = MagicMock()
        mock_span_cm.__enter__ = Mock(return_value=mock_span)
        mock_span_cm.__exit__ = Mock(return_value=None)
        mock_tracer.span.return_value = mock_span_cm
        mock_tracer.enabled = True

        class FailingProjection(CheckpointTrackingProjection):
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def _process_event(self, event: DomainEvent) -> None:
                raise ValueError("Test error")

        # Create a retry policy with 2 retries (3 total attempts) and minimal backoff
        retry_policy = ExponentialBackoffRetryPolicy(
            config=RetryConfig(
                max_retries=2,
                initial_delay=0.001,  # Minimal delay
                exponential_base=1.01,  # Must be > 1.0
                jitter=0.0,
            )
        )

        projection = FailingProjection(
            checkpoint_repo=InMemoryCheckpointRepository(),
            dlq_repo=InMemoryDLQRepository(),
            retry_policy=retry_policy,
            enable_tracing=True,
        )
        projection._tracer = mock_tracer

        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")

        # The event will go to DLQ after exhausting retries, then raise
        with pytest.raises(ValueError):
            await projection.handle(event)

        # Verify retry count attribute was set (3 attempts = max_retries + 1)
        mock_span.set_attribute.assert_called_with(ATTR_RETRY_COUNT, 3)
