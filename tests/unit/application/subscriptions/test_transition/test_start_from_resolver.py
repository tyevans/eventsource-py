"""Tests for StartFromResolver."""

from uuid import uuid4

import pytest

from eventsource.adapters.memory.checkpoints import InMemoryCheckpointRepository
from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.subscriptions import (
    Subscription,
    SubscriptionConfig,
)
from eventsource.application.subscriptions.transition import (
    StartFromResolver,
)
from eventsource.ports.positions import Position
from tests.unit.application.subscriptions.test_transition._fixtures import (
    MockTransitionSubscriber,
    add_events_to_store,
    current,
)


class TestStartFromResolver:
    """Tests for StartFromResolver."""

    @pytest.mark.asyncio
    async def test_resolve_beginning(
        self,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
        subscriber: MockTransitionSubscriber,
    ):
        """Test resolve('beginning') returns None -- read from the feed start."""
        config = SubscriptionConfig(start_from="beginning")
        subscription = Subscription(
            name="BeginningTest",
            config=config,
            subscriber=subscriber,
        )

        resolver = StartFromResolver(event_store, checkpoint_repo)
        position = await resolver.resolve(subscription)

        assert position is None

    @pytest.mark.asyncio
    async def test_resolve_end(
        self,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
        subscriber: MockTransitionSubscriber,
    ):
        """Test resolve('end') returns current max position."""
        # Add events
        await add_events_to_store(event_store, 10)

        config = SubscriptionConfig(start_from="end")
        subscription = Subscription(
            name="EndTest",
            config=config,
            subscriber=subscriber,
        )

        resolver = StartFromResolver(event_store, checkpoint_repo)
        position = await resolver.resolve(subscription)

        assert position == await current(event_store)

    @pytest.mark.asyncio
    async def test_resolve_checkpoint_with_existing(
        self,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
        subscriber: MockTransitionSubscriber,
    ):
        """Test resolve('checkpoint') returns existing checkpoint position."""
        # Save a checkpoint
        checkpointed = Position(store_id="memory", key=(42,))
        await checkpoint_repo.save_position(
            subscription_id="CheckpointTest",
            position=checkpointed,
            event_id=uuid4(),
            event_type="TestEvent",
        )

        config = SubscriptionConfig(start_from="checkpoint")
        subscription = Subscription(
            name="CheckpointTest",
            config=config,
            subscriber=subscriber,
        )

        resolver = StartFromResolver(event_store, checkpoint_repo)
        position = await resolver.resolve(subscription)

        assert position == checkpointed

    @pytest.mark.asyncio
    async def test_resolve_checkpoint_without_existing(
        self,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
        subscriber: MockTransitionSubscriber,
    ):
        """Test resolve('checkpoint') returns None when no checkpoint exists."""
        config = SubscriptionConfig(start_from="checkpoint")
        subscription = Subscription(
            name="NoCheckpointTest",
            config=config,
            subscriber=subscriber,
        )

        resolver = StartFromResolver(event_store, checkpoint_repo)
        position = await resolver.resolve(subscription)

        assert position is None

    @pytest.mark.asyncio
    async def test_resolve_explicit_position(
        self,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
        subscriber: MockTransitionSubscriber,
    ):
        """Test resolve(Position) returns the explicit position."""
        explicit = Position(store_id="memory", key=(100,))
        config = SubscriptionConfig(start_from=explicit)
        subscription = Subscription(
            name="ExplicitTest",
            config=config,
            subscriber=subscriber,
        )

        resolver = StartFromResolver(event_store, checkpoint_repo)
        position = await resolver.resolve(subscription)

        assert position == explicit

    @pytest.mark.asyncio
    async def test_resolve_unknown_start_from(
        self,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
        subscriber: MockTransitionSubscriber,
    ):
        """Test resolve raises for unknown start_from value."""
        # Create config with valid value, then modify it
        config = SubscriptionConfig(start_from="beginning")
        subscription = Subscription(
            name="UnknownTest",
            config=config,
            subscriber=subscriber,
        )
        # Hack to set invalid value (normally prevented by validation)
        object.__setattr__(config, "start_from", "invalid")

        resolver = StartFromResolver(event_store, checkpoint_repo)

        with pytest.raises(ValueError, match="Unknown start_from value"):
            await resolver.resolve(subscription)
