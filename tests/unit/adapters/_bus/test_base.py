"""Unit tests for BaseEventBus."""

import asyncio

import pytest

from eventsource.adapters._bus.base import BaseEventBus
from eventsource.domain.event import DomainEvent
from eventsource.domain.event_registry import EventRegistry, default_registry


class BaseBusEvent(DomainEvent):
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
    assert bus.get_subscriber_count(BaseBusEvent) == 1 and bus.get_subscriber_count() == 1
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
    assert bus.get_subscriber_count() == 0 and bus.get_wildcard_subscriber_count() == 0


def test_subscribe_all_and_unsubscribe_all_use_subscribed_to() -> None:
    class Subscriber:
        def subscribed_to(self) -> list[type[DomainEvent]]:
            return [BaseBusEvent]

        async def handle(self, event: DomainEvent) -> None: ...

    bus, sub = StubBus(), Subscriber()
    bus.subscribe_all(sub)
    assert bus.get_subscriber_count(BaseBusEvent) == 1
    assert bus.unsubscribe_all(sub) == 1
    assert bus.get_subscriber_count(BaseBusEvent) == 0 and bus.unsubscribe_all(sub) == 0


def test_resolve_event_class_uses_injected_registry_first() -> None:
    registry = EventRegistry()
    registry.register(BaseBusEvent, "CustomName")
    bus = StubBus(event_registry=registry)
    assert bus._resolve_event_class("CustomName") is BaseBusEvent


def test_resolve_event_class_falls_back_to_default_registry() -> None:
    bus = StubBus()
    default_registry.register(BaseBusEvent)
    try:
        assert bus._resolve_event_class("BaseBusEvent") is BaseBusEvent
    finally:
        default_registry.unregister("BaseBusEvent")


def test_resolve_event_class_returns_none_for_unknown_type() -> None:
    assert StubBus()._resolve_event_class("NoSuchEventTypeAnywhere") is None


async def test_track_background_runs_and_is_drained() -> None:
    bus, ran = StubBus(), asyncio.Event()

    async def work() -> None:
        ran.set()

    await bus._track_background(work())
    assert bus.get_background_task_count() == 1
    await bus._drain_background(timeout=5.0)
    assert ran.is_set() and bus.get_background_task_count() == 0


async def test_drain_background_with_no_tasks_returns_immediately() -> None:
    bus = StubBus()
    await bus._drain_background(timeout=5.0)
    assert bus.get_background_task_count() == 0


async def test_background_task_failure_does_not_propagate() -> None:
    bus = StubBus()

    async def boom() -> None:
        raise ValueError("background failure")

    await bus._track_background(boom())
    await bus._drain_background(timeout=5.0)
    assert bus.get_background_task_count() == 0


async def test_drain_cancels_tasks_that_exceed_the_timeout(caplog) -> None:  # type: ignore[no-untyped-def]
    bus = StubBus()
    task = await bus._track_background(asyncio.sleep(30))
    with caplog.at_level("INFO", logger="eventsource.adapters._bus.base"):
        await bus._drain_background(timeout=0.05)

    assert task is not None and (task.cancelled() or task.done())
    own_records = [r for r in caplog.records if r.name == "eventsource.adapters._bus.base"]
    assert any("Draining 1 background task(s)" in r.message for r in own_records)
    warnings = [r for r in own_records if r.levelname == "WARNING"]
    assert len(warnings) == 1 and warnings[0].__dict__.get("remaining_tasks") == 1
    assert "1 background task(s) did not complete within 0.05s; cancelling" in warnings[0].message


async def test_drain_background_default_timeout_lets_a_20s_task_finish(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bus, captured_timeout, real_wait = StubBus(), [], asyncio.wait

    async def spy_wait(*args: object, **kwargs: object) -> object:
        captured_timeout.append(kwargs.get("timeout"))
        return await real_wait(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(asyncio, "wait", spy_wait)
    await bus._track_background(asyncio.sleep(0))
    await bus._drain_background()
    assert captured_timeout == [30.0]


async def test_drain_background_logs_and_suppresses_wait_errors(
    monkeypatch: pytest.MonkeyPatch,
    caplog,  # type: ignore[no-untyped-def]
) -> None:
    bus = StubBus()
    await bus._track_background(asyncio.sleep(30))

    async def boom(*args: object, **kwargs: object) -> None:
        raise RuntimeError("distinctive wait failure")

    monkeypatch.setattr(asyncio, "wait", boom)
    with caplog.at_level("ERROR", logger="eventsource.adapters._bus.base"):
        await bus._drain_background(timeout=1.0)

    errors = [r for r in caplog.records if r.levelname == "ERROR"]
    assert len(errors) == 1 and errors[0].exc_info
    assert "Error draining background tasks: distinctive wait failure" in errors[0].message
    for task in bus._background_tasks:
        task.cancel()


def test_get_subscriber_count_is_scoped_to_the_requested_event_type() -> None:
    class OtherBusEvent(DomainEvent):
        aggregate_type: str = "BaseBus"

    bus = StubBus()
    bus.subscribe(BaseBusEvent, _handler)
    bus.subscribe(OtherBusEvent, _handler)
    bus.subscribe(OtherBusEvent, _handler)
    assert bus.get_subscriber_count(BaseBusEvent) == 1
    assert bus.get_subscriber_count(OtherBusEvent) == 2 and bus.get_subscriber_count() == 3


async def test_on_background_task_done_logs_the_actual_task_exception(caplog) -> None:  # type: ignore[no-untyped-def]
    bus = StubBus()

    async def boom() -> None:
        raise ValueError("distinctive background failure")

    with caplog.at_level("ERROR", logger="eventsource.adapters._bus.base"):
        await bus._track_background(boom())
        await bus._drain_background(timeout=5.0)

    own_records = [r for r in caplog.records if r.name == "eventsource.adapters._bus.base"]
    assert len(own_records) == 1 and own_records[0].exc_info is not None
    assert "Background task failed: distinctive background failure" in own_records[0].message


async def test_drain_background_waits_for_every_pending_task_not_just_the_first() -> None:
    bus, fast_done, slow_done = StubBus(), asyncio.Event(), asyncio.Event()

    async def fast() -> None:
        fast_done.set()

    async def slow() -> None:
        await asyncio.sleep(0.1)
        slow_done.set()

    await bus._track_background(fast())
    await bus._track_background(slow())
    await bus._drain_background(timeout=5.0)
    assert fast_done.is_set() and slow_done.is_set()
