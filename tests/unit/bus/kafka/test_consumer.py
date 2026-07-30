"""Unit tests for the extracted ``KafkaConsumerLoop``.

Covers the consume-side invariants in isolation from ``KafkaEventBus``:
error isolation (every handler runs), ``HandlerDispatchError`` aggregation,
no-commit-on-failure (at-least-once redelivery), retry republish below the
max, DLQ routing at the max with error metadata headers, and class-keyed
handler lookup (two event classes sharing a type-name string must not
cross-dispatch).
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

pytest.importorskip("aiokafka", reason="aiokafka not installed")

from eventsource.bus.kafka.config import KafkaEventBusConfig  # noqa: E402
from eventsource.bus.kafka.connection import KafkaConnectionManager  # noqa: E402
from eventsource.bus.kafka.consumer import KafkaConsumerLoop  # noqa: E402
from eventsource.bus.kafka.models import KafkaEventBusStats  # noqa: E402
from eventsource.bus.kafka.serialization import EventSerializer  # noqa: E402
from eventsource.bus.retry import RetryPolicy  # noqa: E402
from eventsource.events.base import DomainEvent  # noqa: E402
from eventsource.exceptions import HandlerDispatchError  # noqa: E402
from eventsource.handlers.adapter import HandlerAdapter  # noqa: E402
from eventsource.observability import create_tracer  # noqa: E402


class SampleConsumerEvent(DomainEvent):
    event_type: str = "SampleConsumerEvent"
    aggregate_type: str = "SampleAggregate"
    payload: str = ""


def _make_event() -> SampleConsumerEvent:
    return SampleConsumerEvent(aggregate_id=uuid4(), aggregate_version=1, payload="x")


def _make_message(event: DomainEvent, *, retry_count: int | None = None) -> Any:
    """Build a stand-in for an aiokafka ConsumerRecord."""
    headers: list[tuple[str, bytes]] = [
        ("event_type", event.event_type.encode("utf-8")),
        ("event_id", str(event.event_id).encode("utf-8")),
    ]
    if retry_count is not None:
        headers.append(("retry_count", str(retry_count).encode("utf-8")))
    return SimpleNamespace(
        value=EventSerializer().serialize(event),
        key=str(event.aggregate_id).encode("utf-8"),
        headers=headers,
        topic="events.stream",
        partition=0,
        offset=42,
    )


def _make_loop(
    *,
    handlers: tuple[HandlerAdapter, ...] = (),
    event_class: type[DomainEvent] | None = SampleConsumerEvent,
    max_retries: int = 3,
    enable_dlq: bool = True,
) -> tuple[KafkaConsumerLoop, Any, Any]:
    """Build a consumer loop wired to mock producer/consumer clients."""
    with patch.object(KafkaEventBusConfig, "_validate_security_config"):
        config = KafkaEventBusConfig(  # type: ignore[arg-type]
            bootstrap_servers="localhost:9092",
            max_retries=max_retries,
            enable_dlq=enable_dlq,
        )
    stats = KafkaEventBusStats()
    # Plain MagicMock: KafkaEventBusMetrics builds its instruments in
    # __init__, so spec= would not expose them as attributes.
    metrics = MagicMock()
    producer = AsyncMock()
    consumer = AsyncMock()

    connection = KafkaConnectionManager(config=config, stats=stats, metrics=metrics)
    connection._producer = producer
    connection._consumer = consumer
    connection._connected = True

    loop = KafkaConsumerLoop(
        config=config,
        connection=connection,
        serializer=EventSerializer(),
        stats=stats,
        metrics=metrics,
        retry_policy=RetryPolicy(
            base_delay=config.retry_base_delay,
            max_delay=config.retry_max_delay,
            jitter=config.retry_jitter,
            max_retries=config.max_retries,
        ),
        handlers_for=lambda _cls: handlers,
        resolve_event_class=lambda _name: event_class,
        tracer=create_tracer(__name__, False),
        enable_tracing=False,
        shutdown_event=asyncio.Event(),
    )
    return loop, producer, consumer


class TestDispatchErrorIsolation:
    @pytest.mark.asyncio
    async def test_all_handlers_run_even_when_one_fails(self) -> None:
        calls: list[str] = []

        async def ok(event: DomainEvent) -> None:
            calls.append("ok")

        async def boom(event: DomainEvent) -> None:
            calls.append("boom")
            raise ValueError("handler exploded")

        async def also_ok(event: DomainEvent) -> None:
            calls.append("also_ok")

        handlers = (HandlerAdapter(boom), HandlerAdapter(ok), HandlerAdapter(also_ok))
        loop, _producer, _consumer = _make_loop(handlers=handlers)

        with pytest.raises(HandlerDispatchError):
            await loop._dispatch_to_handlers(_make_event(), handlers)

        assert calls == ["boom", "ok", "also_ok"]

    @pytest.mark.asyncio
    async def test_multiple_failures_aggregate(self) -> None:
        async def boom_one(event: DomainEvent) -> None:
            raise ValueError("one")

        async def boom_two(event: DomainEvent) -> None:
            raise KeyError("two")

        handlers = (HandlerAdapter(boom_one), HandlerAdapter(boom_two))
        loop, _producer, _consumer = _make_loop(handlers=handlers)

        with pytest.raises(HandlerDispatchError) as exc_info:
            await loop._dispatch_to_handlers(_make_event(), handlers)

        assert len(exc_info.value.failures) == 2

    @pytest.mark.asyncio
    async def test_handler_failure_commits_only_after_dlq_send(self) -> None:
        """The success-path commit is skipped; only the DLQ path commits.

        With max_retries=0 a first failure routes straight to the DLQ, and
        that path commits deliberately to avoid infinite reprocessing. The
        offset-safety invariant is that the commit happens exactly once and
        strictly *after* the message is safely in the DLQ -- never on the
        success path ahead of it.
        """

        async def boom(event: DomainEvent) -> None:
            raise ValueError("nope")

        handlers = (HandlerAdapter(boom),)
        loop, producer, consumer = _make_loop(handlers=handlers, max_retries=0)

        order: list[str] = []
        producer.send.side_effect = lambda **kwargs: order.append(f"send:{kwargs['topic']}")
        consumer.commit.side_effect = lambda *a, **kw: order.append("commit")

        await loop._process_message(_make_message(_make_event()))

        assert consumer.commit.await_count == 1
        assert order == [f"send:{loop._config.dlq_topic_name}", "commit"]
        assert loop._stats.events_processed_success == 0
        assert loop._stats.events_processed_failed == 1

    @pytest.mark.asyncio
    async def test_no_commit_when_dlq_send_fails(self) -> None:
        """A message that could not be parked in the DLQ is never committed.

        ``_send_to_dlq`` re-raises on producer failure, so the error escapes
        ``_process_message`` entirely and no commit is issued -- Kafka
        redelivers the message (at-least-once).
        """

        async def boom(event: DomainEvent) -> None:
            raise ValueError("nope")

        handlers = (HandlerAdapter(boom),)
        loop, producer, consumer = _make_loop(handlers=handlers, max_retries=0)
        producer.send.side_effect = RuntimeError("broker down")

        with pytest.raises(RuntimeError, match="broker down"):
            await loop._process_message(_make_message(_make_event()))

        consumer.commit.assert_not_awaited()
        assert loop._stats.events_processed_success == 0

    @pytest.mark.asyncio
    async def test_dlq_disabled_drops_message_and_commits(self) -> None:
        """Pins current behavior: with the DLQ disabled the message is dropped.

        ``_send_to_dlq`` returns early (logging a warning) and
        ``_handle_processing_error`` still commits, so the failed message is
        discarded rather than redelivered forever.
        """

        async def boom(event: DomainEvent) -> None:
            raise ValueError("nope")

        handlers = (HandlerAdapter(boom),)
        loop, producer, consumer = _make_loop(handlers=handlers, max_retries=0, enable_dlq=False)

        await loop._process_message(_make_message(_make_event()))

        producer.send.assert_not_awaited()
        assert consumer.commit.await_count == 1
        assert loop._stats.messages_sent_to_dlq == 0

    @pytest.mark.asyncio
    async def test_success_commits_offset(self) -> None:
        async def ok(event: DomainEvent) -> None:
            return None

        handlers = (HandlerAdapter(ok),)
        loop, _producer, consumer = _make_loop(handlers=handlers)

        await loop._process_message(_make_message(_make_event()))

        consumer.commit.assert_awaited()
        assert loop._stats.events_processed_success == 1


class TestClassKeyedDispatch:
    @pytest.mark.asyncio
    async def test_handlers_looked_up_by_class_not_name(self) -> None:
        """Two classes sharing an event_type string must not cross-dispatch."""
        seen: list[type[DomainEvent]] = []

        class OtherConsumerEvent(DomainEvent):
            event_type: str = "SampleConsumerEvent"
            aggregate_type: str = "SampleAggregate"
            payload: str = ""

        def handlers_for(cls: type[DomainEvent]) -> tuple[HandlerAdapter, ...]:
            seen.append(cls)
            return ()

        loop, _producer, _consumer = _make_loop()
        loop._handlers_for = handlers_for  # type: ignore[assignment]
        loop._resolve_event_class = lambda _name: OtherConsumerEvent  # type: ignore[assignment]

        await loop._process_message(_make_message(_make_event()))

        # The lookup key is the deserialized class, not the header name.
        assert seen == [OtherConsumerEvent]


class TestRetryAndDlq:
    @pytest.mark.asyncio
    async def test_republishes_for_retry_below_max(self) -> None:
        loop, producer, _consumer = _make_loop(max_retries=3)
        message = _make_message(_make_event(), retry_count=1)

        await loop._handle_processing_error(message, ValueError("boom"), 1)

        producer.send.assert_awaited_once()
        kwargs = producer.send.await_args.kwargs
        assert kwargs["topic"] == loop._config.topic_name
        headers = dict(kwargs["headers"])
        assert headers["retry_count"] == b"2"

    @pytest.mark.asyncio
    async def test_sends_to_dlq_at_max_with_error_headers(self) -> None:
        loop, producer, _consumer = _make_loop(max_retries=3)
        message = _make_message(_make_event(), retry_count=3)

        await loop._handle_processing_error(message, ValueError("boom"), 3)

        producer.send.assert_awaited_once()
        kwargs = producer.send.await_args.kwargs
        assert kwargs["topic"] == loop._config.dlq_topic_name
        headers = dict(kwargs["headers"])
        assert headers["dlq_reason"] == b"max_retries_exceeded"
        assert headers["dlq_error_type"] == b"ValueError"
        assert headers["dlq_error_message"] == b"boom"
        assert headers["dlq_retry_count"] == b"3"
        assert headers["dlq_original_topic"] == b"events.stream"
        assert loop._stats.messages_sent_to_dlq == 1

    @pytest.mark.asyncio
    async def test_single_handler_failure_unwrapped_for_dlq_error_type(self) -> None:
        async def boom(event: DomainEvent) -> None:
            raise KeyError("only-one")

        handlers = (HandlerAdapter(boom),)
        loop, producer, _consumer = _make_loop(handlers=handlers, max_retries=0)

        await loop._process_message(_make_message(_make_event()))

        headers = dict(producer.send.await_args.kwargs["headers"])
        assert headers["dlq_error_type"] == b"KeyError"


class TestLifecycle:
    @pytest.mark.asyncio
    async def test_start_requires_connection(self) -> None:
        loop, _producer, _consumer = _make_loop()
        loop._connection._connected = False
        loop._connection._consumer = None

        with pytest.raises(RuntimeError, match="Not connected"):
            await loop.start()

    @pytest.mark.asyncio
    async def test_stop_clears_is_consuming(self) -> None:
        loop, _producer, _consumer = _make_loop()
        loop._consuming = True
        assert loop.is_consuming is True

        await loop.stop()

        assert loop.is_consuming is False

    @pytest.mark.asyncio
    async def test_start_in_background_rejects_double_start(self) -> None:
        loop, _producer, consumer = _make_loop()

        async def _never_ending() -> Any:
            await asyncio.sleep(3600)
            yield None  # pragma: no cover

        consumer.__aiter__ = lambda _self=None: _never_ending()

        task = loop.start_in_background()
        await asyncio.sleep(0)
        try:
            with pytest.raises(RuntimeError, match="already running"):
                loop.start_in_background()
        finally:
            task.cancel()
            with pytest.raises((asyncio.CancelledError, RuntimeError)):
                await task
