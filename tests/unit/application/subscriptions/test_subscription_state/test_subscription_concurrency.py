"""Unit tests for subscription concurrency, type aliases, and module imports."""

import asyncio
import contextlib
from uuid import uuid4

import pytest

from eventsource.application.subscriptions import (
    BatchHandler,
    EventHandler,
    Subscription,
    SubscriptionConfig,
    SubscriptionState,
    SubscriptionStateError,
)
from eventsource.domain.event import DomainEvent
from tests.unit.application.subscriptions.test_subscription_state._fixtures import (
    MockSubscriber,
)


class TestConcurrency:
    """Tests for concurrent access safety."""

    @pytest.mark.asyncio
    async def test_concurrent_state_transitions_are_serialized(
        self, mock_subscriber: MockSubscriber, config: SubscriptionConfig
    ):
        """Test concurrent state transitions are serialized."""
        subscription = Subscription(
            name="TestSubscription",
            config=config,
            subscriber=mock_subscriber,
        )

        # Attempt concurrent transitions
        async def transition_to_catching_up():
            await subscription.transition_to(SubscriptionState.CATCHING_UP)

        async def transition_to_live():
            # Wait a bit to ensure first transition starts
            await asyncio.sleep(0.001)
            # This should succeed since we'll be in CATCHING_UP
            with contextlib.suppress(SubscriptionStateError):
                await subscription.transition_to(SubscriptionState.LIVE)

        # Run concurrently
        await asyncio.gather(
            transition_to_catching_up(),
            transition_to_live(),
        )

        # State should be either CATCHING_UP or LIVE (both valid end states)
        assert subscription.state in {
            SubscriptionState.CATCHING_UP,
            SubscriptionState.LIVE,
        }

    @pytest.mark.asyncio
    async def test_concurrent_event_recording(
        self, mock_subscriber: MockSubscriber, config: SubscriptionConfig
    ):
        """Test concurrent event recording is safe."""
        subscription = Subscription(
            name="TestSubscription",
            config=config,
            subscriber=mock_subscriber,
        )

        async def record_event(position: int):
            await subscription.record_event_processed(
                position=position,
                event_id=uuid4(),
                event_type="OrderCreated",
            )

        # Record 100 events concurrently
        await asyncio.gather(*[record_event(i) for i in range(100)])

        # All events should be counted
        assert subscription.events_processed == 100


class TestTypeAliases:
    """Tests for EventHandler and BatchHandler type aliases."""

    def test_event_handler_type_alias(self):
        """Test EventHandler type alias can be used."""

        async def my_handler(event: DomainEvent) -> None:
            pass

        handler: EventHandler = my_handler
        assert callable(handler)

    def test_batch_handler_type_alias(self):
        """Test BatchHandler type alias can be used."""
        from collections.abc import Sequence

        async def my_batch_handler(events: Sequence[DomainEvent]) -> None:
            pass

        handler: BatchHandler = my_batch_handler
        assert callable(handler)


class TestModuleImports:
    """Tests for module imports."""

    def test_import_from_subscription_module(self):
        """Test imports from subscription module."""
        from eventsource.application.subscriptions.subscription import (
            VALID_TRANSITIONS,
            BatchHandler,
            EventHandler,
            Subscription,
            SubscriptionState,
            SubscriptionStatus,
            is_valid_transition,
        )

        assert SubscriptionState is not None
        assert SubscriptionStatus is not None
        assert Subscription is not None
        assert is_valid_transition is not None
        assert VALID_TRANSITIONS is not None
        assert EventHandler is not None
        assert BatchHandler is not None

    def test_import_from_subscriptions_package(self):
        """Test imports from subscriptions package."""
        from eventsource.application.subscriptions import (
            VALID_TRANSITIONS,
            BatchHandler,
            EventHandler,
            Subscription,
            SubscriptionState,
            SubscriptionStatus,
            is_valid_transition,
        )

        assert SubscriptionState is not None
        assert SubscriptionStatus is not None
        assert Subscription is not None
        assert is_valid_transition is not None
        assert VALID_TRANSITIONS is not None
        assert EventHandler is not None
        assert BatchHandler is not None
