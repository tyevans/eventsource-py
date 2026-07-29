"""Unit tests for BaseEventBus."""

import asyncio

from eventsource.bus.base import BaseEventBus
from eventsource.events.base import DomainEvent
from eventsource.events.registry import EventRegistry, register_event


@register_event
class BaseBusEvent(DomainEvent):
    event_type: str = "BaseBusEvent"
    aggregate_type: str = "BaseBus"


class StubBus(BaseEventBus):
    """Minimal concrete bus: records what it was asked to publish."""

    def __init__(self, **kwargs: object) -> None:
        super().__init__(**kwargs)  # type: ignore[arg-type]
        self.published: list[DomainEvent] = []

    async def publish(self, events: list[DomainEvent], background: bool = False) -> None:
        self.published.extend(events)


def _handler(event: DomainEvent) -> None: ...


def test_subscription_methods_delegate_to_the_registry() -> None:
    bus = StubBus()

    bus.subscribe(BaseBusEvent, _handler)
    assert bus.get_subscriber_count(BaseBusEvent) == 1
    assert bus.get_subscriber_count() == 1

    bus.subscribe_to_all_events(_handler)
    assert bus.get_wildcard_subscriber_count() == 1

    assert bus.unsubscribe(BaseBusEvent, _handler) is True
    assert bus.unsubscribe(BaseBusEvent, _handler) is False
    assert bus.unsubscribe_from_all_events(_handler) is True
    assert bus.unsubscribe_from_all_events(_handler) is False


def test_clear_subscribers_empties_both_registries() -> None:
    bus = StubBus()
    bus.subscribe(BaseBusEvent, _handler)
    bus.subscribe_to_all_events(_handler)

    bus.clear_subscribers()

    assert bus.get_subscriber_count() == 0
    assert bus.get_wildcard_subscriber_count() == 0


def test_subscribe_all_uses_subscribed_to() -> None:
    class Subscriber:
        def subscribed_to(self) -> list[type[DomainEvent]]:
            return [BaseBusEvent]

        async def handle(self, event: DomainEvent) -> None: ...

    bus = StubBus()
    bus.subscribe_all(Subscriber())

    assert bus.get_subscriber_count(BaseBusEvent) == 1


def test_resolve_event_class_uses_injected_registry_first() -> None:
    registry = EventRegistry()
    registry.register(BaseBusEvent, "CustomName")
    bus = StubBus(event_registry=registry)

    assert bus._resolve_event_class("CustomName") is BaseBusEvent


def test_resolve_event_class_falls_back_to_default_registry() -> None:
    bus = StubBus()

    assert bus._resolve_event_class("BaseBusEvent") is BaseBusEvent


def test_resolve_event_class_returns_none_for_unknown_type() -> None:
    bus = StubBus()

    assert bus._resolve_event_class("NoSuchEventTypeAnywhere") is None


async def test_track_background_runs_and_is_drained() -> None:
    bus = StubBus()
    ran = asyncio.Event()

    async def work() -> None:
        ran.set()

    bus._track_background(work())
    assert bus.get_background_task_count() == 1

    await bus._drain_background(timeout=5.0)

    assert ran.is_set()
    assert bus.get_background_task_count() == 0


async def test_drain_background_with_no_tasks_returns_immediately() -> None:
    bus = StubBus()

    await bus._drain_background(timeout=5.0)

    assert bus.get_background_task_count() == 0


async def test_background_task_failure_does_not_propagate() -> None:
    bus = StubBus()

    async def boom() -> None:
        raise ValueError("background failure")

    bus._track_background(boom())
    await bus._drain_background(timeout=5.0)

    assert bus.get_background_task_count() == 0


async def test_drain_cancels_tasks_that_exceed_the_timeout() -> None:
    bus = StubBus()

    async def slow() -> None:
        await asyncio.sleep(30)

    task = bus._track_background(slow())
    await bus._drain_background(timeout=0.05)

    assert task.cancelled() or task.done()
