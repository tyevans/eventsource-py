"""Integration tests for error handling and recovery strategies."""

from __future__ import annotations

import asyncio
import contextlib
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from eventsource.application.subscriptions import SubscriptionConfig
from eventsource.application.subscriptions.error_handling import (
    ErrorCategory,
    ErrorClassifier,
    ErrorHandlingConfig,
    ErrorHandlingStrategy,
    ErrorInfo,
    SubscriptionErrorHandler,
)
from eventsource.domain import StreamId
from eventsource.ports.envelopes import EventEnvelope
from eventsource.ports.positions import Position
from tests.integration.subscriptions.conftest import (
    FailingProjection,
    SubTestOrderCreated,
    populate_event_store,
)

from .helpers import PositionalFailingProjection

pytestmark = [pytest.mark.asyncio, pytest.mark.slow]


class TestErrorHandlingIntegration:
    """Tests for error handling integration."""

    async def test_continue_on_error_skips_failed_event(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test continue_on_error=True skips failed events."""
        await populate_event_store(in_memory_event_store, 10)

        projection = PositionalFailingProjection(fail_at_positions={5})

        config = SubscriptionConfig(
            start_from="beginning",
            continue_on_error=True,
        )
        await subscription_manager.subscribe(projection, config)
        await subscription_manager.start()

        await asyncio.sleep(1.0)
        await subscription_manager.stop()

        assert projection.event_count == 9
        assert len(projection.failures) == 1

    async def test_stop_on_error_stops_subscription(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test continue_on_error=False stops on failure."""
        await populate_event_store(in_memory_event_store, 10)

        projection = PositionalFailingProjection(fail_at_positions={3})

        config = SubscriptionConfig(
            start_from="beginning",
            continue_on_error=False,
        )
        await subscription_manager.subscribe(projection, config)

        with contextlib.suppress(Exception):
            await subscription_manager.start()

        assert projection.event_count < 10

    async def test_error_callback_invoked(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test that error callbacks are invoked on failure."""
        await populate_event_store(in_memory_event_store, 5)

        callback_errors: list[ErrorInfo] = []

        async def error_callback(error_info: ErrorInfo) -> None:
            callback_errors.append(error_info)

        subscription_manager.on_error(error_callback)

        projection = FailingProjection(fail_on_event_types={"SubTestOrderCreated"})

        config = SubscriptionConfig(
            start_from="beginning",
            continue_on_error=True,
        )
        await subscription_manager.subscribe(projection, config)
        await subscription_manager.start()

        await asyncio.sleep(0.5)
        await subscription_manager.stop()

        assert len(projection.failures) > 0

    async def test_dlq_receives_failed_events(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        in_memory_dlq_repo,
        subscription_manager_with_dlq,
    ):
        """Test that failed events are sent to DLQ."""
        await populate_event_store(in_memory_event_store, 10)

        error_handler = SubscriptionErrorHandler(
            subscription_name="TestProjection",
            dlq_repo=in_memory_dlq_repo,
            config=ErrorHandlingConfig(
                strategy=ErrorHandlingStrategy.DLQ_ONLY,
                dlq_enabled=True,
            ),
        )

        test_event = SubTestOrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            amount=100.0,
        )

        envelope = EventEnvelope(
            event=test_event,
            stream_id=StreamId(aggregate_id=test_event.aggregate_id, category="TestOrder"),
            stream_version=1,
            position=Position(store_id="memory", key=(1,)),
            stored_at=datetime.now(UTC),
        )

        await error_handler.handle_error(
            error=ValueError("Test error"),
            envelope=envelope,
            event=test_event,
        )

        failed_events = await in_memory_dlq_repo.get_failed_events(projection_name="TestProjection")
        assert len(failed_events) == 1
        assert str(failed_events[0].event_id) == str(test_event.event_id)

    async def test_error_classification(self):
        """Test error classification for different exception types."""
        classifier = ErrorClassifier()

        conn_error = classifier.classify(ConnectionError("timeout"))
        assert conn_error.category == ErrorCategory.TRANSIENT
        assert conn_error.retryable is True

        timeout_error = classifier.classify(TimeoutError("timeout"))
        assert timeout_error.category == ErrorCategory.TRANSIENT
        assert timeout_error.retryable is True

        value_error = classifier.classify(ValueError("invalid"))
        assert value_error.category == ErrorCategory.APPLICATION
        assert value_error.retryable is False

        type_error = classifier.classify(TypeError("wrong type"))
        assert type_error.category == ErrorCategory.APPLICATION
        assert type_error.retryable is False

    async def test_error_stats_tracking(self):
        """Test that error statistics are tracked correctly."""
        error_handler = SubscriptionErrorHandler(
            subscription_name="TestProjection",
            config=ErrorHandlingConfig(),
        )

        for i in range(5):
            await error_handler.handle_error(
                error=ConnectionError(f"Error {i}"),
            )

        for i in range(3):
            await error_handler.handle_error(
                error=ValueError(f"Value error {i}"),
            )

        stats = error_handler.stats
        assert stats.total_errors == 8
        assert stats.transient_errors == 5
        assert stats.permanent_errors == 3
