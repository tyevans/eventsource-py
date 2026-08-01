"""Unit tests for BaseEventBus."""

import asyncio

import pytest

from eventsource.adapters._bus.base import BaseEventBus
from eventsource.domain.event import DomainEvent
from eventsource.domain.event_registry import EventRegistry, default_registry


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
    default_registry.register(BaseBusEvent)
    try:
        assert bus._resolve_event_class("BaseBusEvent") is BaseBusEvent
    finally:
        default_registry.unregister("BaseBusEvent")


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


async def test_drain_cancels_tasks_that_exceed_the_timeout(caplog) -> None:  # type: ignore[no-untyped-def]
    bus = StubBus()

    async def slow() -> None:
        await asyncio.sleep(30)

    task = bus._track_background(slow())
    with caplog.at_level("INFO", logger="eventsource.adapters._bus.base"):
        await bus._drain_background(timeout=0.05)

    assert task.cancelled() or task.done()

    own_records = [r for r in caplog.records if r.name == "eventsource.adapters._bus.base"]
    info_messages = [r.message for r in own_records if r.levelname == "INFO"]
    assert any("Draining 1 background task(s)" in m for m in info_messages)

    warnings = [r for r in own_records if r.levelname == "WARNING"]
    assert len(warnings) == 1
    assert "1 background task(s) did not complete within 0.05s; cancelling" in warnings[0].message
    assert warnings[0].__dict__.get("remaining_tasks") == 1


async def test_drain_background_default_timeout_lets_a_20s_task_finish(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression test for the 30.0s default. Calling ``_drain_background()``
    with no explicit timeout must wait long enough for a task that takes
    noticeably longer than mutmut's off-by-one mutant (29.0s/31.0s) would
    still distinguish -- so instead of waiting for real time, patch
    ``asyncio.wait`` to capture the timeout it was actually invoked with.
    """
    bus = StubBus()
    captured_timeout: list[float | None] = []
    real_wait = asyncio.wait

    async def spy_wait(*args: object, **kwargs: object) -> object:
        captured_timeout.append(kwargs.get("timeout"))  # type: ignore[arg-type]
        return await real_wait(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(asyncio, "wait", spy_wait)

    async def instant() -> None:
        return None

    bus._track_background(instant())
    await bus._drain_background()

    assert captured_timeout == [30.0]


async def test_drain_background_logs_and_suppresses_wait_errors(
    monkeypatch: pytest.MonkeyPatch,
    caplog,  # type: ignore[no-untyped-def]
) -> None:
    bus = StubBus()

    async def never_finishes() -> None:
        await asyncio.sleep(30)

    bus._track_background(never_finishes())

    async def boom(*args: object, **kwargs: object) -> None:
        raise RuntimeError("distinctive wait failure")

    monkeypatch.setattr(asyncio, "wait", boom)

    with caplog.at_level("ERROR", logger="eventsource.adapters._bus.base"):
        await bus._drain_background(timeout=1.0)

    own_records = [r for r in caplog.records if r.name == "eventsource.adapters._bus.base"]
    errors = [r for r in own_records if r.levelname == "ERROR"]
    assert len(errors) == 1
    assert "Error draining background tasks: distinctive wait failure" in errors[0].message
    # exc_info must be truthy (True or a real exc_info tuple) -- `False` would
    # also satisfy `is not None`, so check truthiness explicitly.
    assert errors[0].exc_info

    for task in bus._background_tasks:
        task.cancel()


def test_get_subscriber_count_is_scoped_to_the_requested_event_type() -> None:
    class OtherBusEvent(DomainEvent):
        event_type: str = "OtherBusEvent"
        aggregate_type: str = "BaseBus"

    bus = StubBus()
    bus.subscribe(BaseBusEvent, _handler)
    bus.subscribe(OtherBusEvent, _handler)
    bus.subscribe(OtherBusEvent, _handler)

    assert bus.get_subscriber_count(BaseBusEvent) == 1
    assert bus.get_subscriber_count(OtherBusEvent) == 2
    assert bus.get_subscriber_count() == 3


async def test_on_background_task_done_logs_the_actual_task_exception(caplog) -> None:  # type: ignore[no-untyped-def]
    bus = StubBus()

    async def boom() -> None:
        raise ValueError("distinctive background failure")

    with caplog.at_level("ERROR", logger="eventsource.adapters._bus.base"):
        bus._track_background(boom())
        await bus._drain_background(timeout=5.0)

    own_records = [r for r in caplog.records if r.name == "eventsource.adapters._bus.base"]
    assert len(own_records) == 1
    assert "Background task failed: distinctive background failure" in own_records[0].message
    assert own_records[0].exc_info is not None


async def test_drain_background_waits_for_every_pending_task_not_just_the_first() -> None:
    bus = StubBus()
    fast_done = asyncio.Event()
    slow_done = asyncio.Event()

    async def fast() -> None:
        fast_done.set()

    async def slow() -> None:
        await asyncio.sleep(0.1)
        slow_done.set()

    bus._track_background(fast())
    bus._track_background(slow())

    await bus._drain_background(timeout=5.0)

    assert fast_done.is_set()
    assert slow_done.is_set()
