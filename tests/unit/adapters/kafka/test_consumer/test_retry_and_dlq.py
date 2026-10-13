"""Unit tests for KafkaConsumerLoop retry scheduling, DLQ routing, and offset commit safety."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from unittest.mock import patch

import pytest

pytest.importorskip("aiokafka", reason="aiokafka not installed")

from eventsource.adapters._bus.handler_adapter import HandlerAdapter  # noqa: E402
from eventsource.domain.event import DomainEvent  # noqa: E402

from ._fixtures import _make_event, _make_loop, _make_message  # noqa: E402


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


class TestRetryBackoffIsHonored:
    """`retry_after` is scheduled via non-blocking RetryScheduler (ADR 0048, TASK-0182)."""

    @pytest.mark.asyncio
    async def test_republished_message_waits_out_its_scheduled_time(self) -> None:
        loop, _producer, _consumer = _make_loop()
        event = _make_event()
        message = _make_message(event, retry_count=1)
        due_in = 12.5
        message.headers.append(
            ("retry_after", str(datetime.now(UTC).timestamp() + due_in).encode("utf-8"))
        )

        slept: list[float] = []

        async def fake_sleep(seconds: float) -> None:
            slept.append(seconds)

        with patch.object(asyncio, "sleep", fake_sleep):
            await loop._process_message(message)
            await loop.drain_retries()

        assert slept, "the scheduled backoff was ignored"
        assert 0 < slept[0] <= due_in
        assert loop._stats.events_processed_success == 1

    @pytest.mark.asyncio
    async def test_first_delivery_does_not_wait(self) -> None:
        loop, _producer, _consumer = _make_loop()
        message = _make_message(_make_event())

        slept: list[float] = []

        async def fake_sleep(seconds: float) -> None:
            slept.append(seconds)

        with patch.object(asyncio, "sleep", fake_sleep):
            await loop._process_message(message)

        assert slept == []

    @pytest.mark.asyncio
    async def test_an_elapsed_retry_after_does_not_wait(self) -> None:
        loop, _producer, _consumer = _make_loop()
        message = _make_message(_make_event(), retry_count=1)
        message.headers.append(
            ("retry_after", str(datetime.now(UTC).timestamp() - 60).encode("utf-8"))
        )

        slept: list[float] = []

        async def fake_sleep(seconds: float) -> None:
            slept.append(seconds)

        with patch.object(asyncio, "sleep", fake_sleep):
            await loop._process_message(message)

        assert slept == []


class TestOffsetsAreNotCommittedForUnretainedEvents:
    """Committing an offset claims the event is handled (ADR 0048)."""

    @pytest.mark.asyncio
    async def test_no_commit_when_the_producer_cannot_republish_or_dlq(self) -> None:
        async def boom(event: DomainEvent) -> None:
            raise ValueError("handler exploded")

        loop, _producer, consumer = _make_loop(handlers=(HandlerAdapter(boom),), max_retries=3)
        loop._connection._producer = None

        await loop._process_message(_make_message(_make_event(), retry_count=1))

        consumer.commit.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_no_commit_at_max_retries_when_the_dlq_send_cannot_happen(self) -> None:
        async def boom(event: DomainEvent) -> None:
            raise ValueError("handler exploded")

        loop, _producer, consumer = _make_loop(handlers=(HandlerAdapter(boom),), max_retries=1)
        loop._connection._producer = None

        await loop._process_message(_make_message(_make_event(), retry_count=5))

        consumer.commit.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_commits_when_the_republish_succeeds(self) -> None:
        async def boom(event: DomainEvent) -> None:
            raise ValueError("handler exploded")

        loop, producer, consumer = _make_loop(handlers=(HandlerAdapter(boom),), max_retries=3)

        await loop._process_message(_make_message(_make_event(), retry_count=1))

        producer.send.assert_awaited()
        consumer.commit.assert_awaited()

    @pytest.mark.asyncio
    async def test_commits_when_the_dlq_is_deliberately_disabled(self) -> None:
        """Turning the DLQ off is an explicit choice to drop poison messages."""

        async def boom(event: DomainEvent) -> None:
            raise ValueError("handler exploded")

        loop, _producer, consumer = _make_loop(
            handlers=(HandlerAdapter(boom),), max_retries=1, enable_dlq=False
        )

        await loop._process_message(_make_message(_make_event(), retry_count=5))

        consumer.commit.assert_awaited()
