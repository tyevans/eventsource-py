"""Tests for BaseSubscriber abstract base class and helper functions."""

from uuid import uuid4

import pytest

from eventsource.application.subscriptions.subscriber import (
    BaseSubscriber,
    Subscriber,
    get_subscribed_event_types,
)
from eventsource.domain.event import DomainEvent
from tests.unit.application.subscriptions.test_subscriber._fixtures import (
    OrderCreated,
    OrderShipped,
)


class TestGetSubscribedEventTypes:
    """Tests for get_subscribed_event_types utility function."""

    def test_returns_event_types(self) -> None:
        """Returns event types from subscribed_to."""

        class MySubscriber:
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated, OrderShipped]

            async def handle(self, event: DomainEvent) -> None:
                pass

        types = get_subscribed_event_types(MySubscriber())

        assert len(types) == 2
        assert OrderCreated in types
        assert OrderShipped in types

    def test_returns_empty_list(self) -> None:
        """Handles subscriber with no subscriptions."""

        class EmptySubscriber:
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return []

            async def handle(self, event: DomainEvent) -> None:
                pass

        types = get_subscribed_event_types(EmptySubscriber())
        assert types == []

    def test_raises_on_invalid_return(self) -> None:
        """Raises ValueError when subscribed_to returns non-list."""

        class InvalidSubscriber:
            def subscribed_to(self) -> tuple:  # type: ignore[override]
                return (OrderCreated,)

            async def handle(self, event: DomainEvent) -> None:
                pass

        with pytest.raises(ValueError, match="must return a list"):
            get_subscribed_event_types(InvalidSubscriber())  # type: ignore


class TestBaseSubscriber:
    """Tests for BaseSubscriber abstract class."""

    def test_cannot_instantiate_directly(self) -> None:
        """BaseSubscriber cannot be instantiated directly."""
        with pytest.raises(TypeError, match="abstract"):
            BaseSubscriber()  # type: ignore[abstract]

    def test_subclass_must_implement_subscribed_to(self) -> None:
        """Subclass missing subscribed_to raises TypeError."""

        with pytest.raises(TypeError, match="abstract"):

            class IncompleteSubscriber(BaseSubscriber):
                async def handle(self, event: DomainEvent) -> None:
                    pass

            IncompleteSubscriber()

    def test_subclass_must_implement_handle(self) -> None:
        """Subclass missing handle raises TypeError."""

        with pytest.raises(TypeError, match="abstract"):

            class IncompleteSubscriber(BaseSubscriber):
                def subscribed_to(self) -> list[type[DomainEvent]]:
                    return [OrderCreated]

            IncompleteSubscriber()

    @pytest.mark.asyncio
    async def test_complete_subclass_works(self) -> None:
        """Complete BaseSubscriber subclass works correctly."""
        handled: list[DomainEvent] = []

        class MySubscriber(BaseSubscriber):
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated, OrderShipped]

            async def handle(self, event: DomainEvent) -> None:
                handled.append(event)

        subscriber = MySubscriber()
        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")

        await subscriber.handle(event)

        assert len(handled) == 1

    def test_can_handle_returns_true_for_subscribed_types(self) -> None:
        """can_handle returns True for event types in subscribed_to."""

        class MySubscriber(BaseSubscriber):
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def handle(self, event: DomainEvent) -> None:
                pass

        subscriber = MySubscriber()
        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")

        assert subscriber.can_handle(event) is True

    def test_can_handle_returns_false_for_other_types(self) -> None:
        """can_handle returns False for non-subscribed event types."""

        class MySubscriber(BaseSubscriber):
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def handle(self, event: DomainEvent) -> None:
                pass

        subscriber = MySubscriber()
        event = OrderShipped(aggregate_id=uuid4(), tracking_number="TRK-001")

        assert subscriber.can_handle(event) is False

    def test_repr_includes_event_types(self) -> None:
        """repr shows subscribed event types."""

        class MySubscriber(BaseSubscriber):
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated, OrderShipped]

            async def handle(self, event: DomainEvent) -> None:
                pass

        subscriber = MySubscriber()
        repr_str = repr(subscriber)

        assert "MySubscriber" in repr_str
        assert "OrderCreated" in repr_str
        assert "OrderShipped" in repr_str

    def test_matches_subscriber_protocol(self) -> None:
        """BaseSubscriber subclass matches Subscriber protocol."""

        class MySubscriber(BaseSubscriber):
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def handle(self, event: DomainEvent) -> None:
                pass

        subscriber = MySubscriber()
        assert isinstance(subscriber, Subscriber)
