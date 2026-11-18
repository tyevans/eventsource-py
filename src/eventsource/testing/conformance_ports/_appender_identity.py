"""Event-identity cases for `AppenderConformance`: `event_id` uniqueness.

Composed into `AppenderConformance` (see `appender.py`). Not a public suite:
import `AppenderConformance` from `eventsource.testing.conformance_ports`.
"""

from typing import Protocol

import pytest

from eventsource.domain.exceptions import DuplicateEventError
from eventsource.ports import ExpectedVersion
from eventsource.ports.store import EventAppender, StreamReader
from eventsource.testing.conformance_ports._fixtures import make_event, make_stream


class _AppenderUnderTest(EventAppender, StreamReader, Protocol):
    """Adapter surface needed by the appender suites: append plus version lookup."""


class AppenderIdentityConformance:
    """Mixin of `event_id` uniqueness cases; relies on the host suite's `store` fixture."""

    async def test_duplicate_event_id_raises_and_batch_is_atomic(
        self, store: _AppenderUnderTest
    ) -> None:
        stream = make_stream()
        first = make_event(stream.aggregate_id)
        await store.append(stream, [first], ExpectedVersion.no_stream())
        duplicate_batch = [make_event(stream.aggregate_id), first]
        with pytest.raises(DuplicateEventError):
            await store.append(stream, duplicate_batch, ExpectedVersion.any_())
        assert await store.get_stream_version(stream) == 1

    async def test_duplicate_event_id_in_other_stream_raises(
        self, store: _AppenderUnderTest
    ) -> None:
        """`event_id` is unique across the store, not per stream (accepted behavior, ADR-0113)."""
        owner, other = make_stream(), make_stream()
        event = make_event(owner.aggregate_id)
        await store.append(owner, [event], ExpectedVersion.no_stream())
        with pytest.raises(DuplicateEventError):
            await store.append(other, [event], ExpectedVersion.no_stream())
        assert await store.get_stream_version(other) == 0
