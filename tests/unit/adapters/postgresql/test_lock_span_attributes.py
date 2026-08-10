"""
Unit tests confirming the ATTR_LOCK_* telemetry constants are actually set on
real spans emitted by ``PostgreSQLLockManager``, not merely declared and left
inert (recurring-defects.md #3).

The lock manager -- not the user -- is contractually obligated to emit these
attributes, so each test drives the real ``acquire()`` call against a stubbed
session factory and then inspects the finished spans via an in-memory
exporter. A test that set the attributes on a Span double directly would not
establish that anything reaches them.

Each test builds a fully isolated OpenTelemetry TracerProvider (never touching
the process-global provider, so these tests are immune to pytest-randomly
reordering across the suite) and passes it explicitly as ``tracer=``.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from eventsource.adapters.postgresql.locks import PostgreSQLLockManager
from eventsource.observability.attributes import (
    ATTR_LOCK_ACQUIRED,
    ATTR_LOCK_ID,
    ATTR_LOCK_KEY,
    ATTR_LOCK_TIMEOUT,
)
from eventsource.ports.exceptions import LockAcquisitionError

pytestmark = pytest.mark.asyncio


class _IsolatedOtelTracer:
    """A `Tracer` bound to a private, per-test TracerProvider."""

    def __init__(self, otel_tracer: Any) -> None:
        self._tracer = otel_tracer

    @property
    def enabled(self) -> bool:
        return True

    def span(self, name: str, attributes: dict[str, Any] | None = None) -> Any:
        return self._tracer.start_as_current_span(name, attributes=attributes or {})


@pytest.fixture
def isolated_tracer() -> tuple[Any, Any]:
    """A (tracer, exporter) pair backed by a fresh, private TracerProvider."""
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
        InMemorySpanExporter,
    )

    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return _IsolatedOtelTracer(provider.get_tracer("test")), exporter


def _session_factory(*, acquired: bool = True) -> Any:
    """A stub session factory whose advisory-lock queries always succeed."""
    session = MagicMock()
    result = MagicMock()
    result.scalar.return_value = acquired
    session.execute = AsyncMock(return_value=result)
    session.close = AsyncMock()
    return MagicMock(return_value=session)


def _spans_by_name(exporter: Any, name: str) -> list[Any]:
    return [s for s in exporter.get_finished_spans() if s.name == name]


def _attrs(span: Any) -> dict[str, Any]:
    return dict(span.attributes or {})


class TestLockSpanAttributes:
    """The declared ATTR_LOCK_* constants appear on the emitted spans."""

    async def test_acquire_records_key_id_timeout_and_acquired(
        self, isolated_tracer: tuple[Any, Any]
    ) -> None:
        """acquire() records all four ATTR_LOCK_* constants on its real span."""
        tracer, exporter = isolated_tracer
        manager = PostgreSQLLockManager(_session_factory(), tracer=tracer)

        async with manager.acquire("migration:tenant-abc", timeout=5.0):
            pass

        spans = _spans_by_name(exporter, "eventsource.lock.acquire")
        assert len(spans) == 1
        attrs = _attrs(spans[0])
        assert attrs[ATTR_LOCK_KEY] == "migration:tenant-abc"
        assert attrs[ATTR_LOCK_ID] == PostgreSQLLockManager._key_to_lock_id("migration:tenant-abc")
        assert attrs[ATTR_LOCK_TIMEOUT] == 5.0
        assert attrs[ATTR_LOCK_ACQUIRED] is True

    async def test_acquire_without_timeout_records_sentinel(
        self, isolated_tracer: tuple[Any, Any]
    ) -> None:
        """An unbounded wait is recorded as the documented -1 sentinel."""
        tracer, exporter = isolated_tracer
        manager = PostgreSQLLockManager(_session_factory(), tracer=tracer)

        async with manager.acquire("migration:tenant-abc"):
            pass

        attrs = _attrs(_spans_by_name(exporter, "eventsource.lock.acquire")[0])
        assert attrs[ATTR_LOCK_TIMEOUT] == -1

    async def test_failed_acquire_records_acquired_false(
        self, isolated_tracer: tuple[Any, Any]
    ) -> None:
        """A timed-out acquisition records ATTR_LOCK_ACQUIRED as False."""
        tracer, exporter = isolated_tracer
        manager = PostgreSQLLockManager(_session_factory(acquired=False), tracer=tracer)

        with pytest.raises(LockAcquisitionError):
            async with manager.acquire("migration:tenant-abc", timeout=0.0):
                pass

        attrs = _attrs(_spans_by_name(exporter, "eventsource.lock.acquire")[0])
        assert attrs[ATTR_LOCK_ACQUIRED] is False

    async def test_release_records_key_and_id(self, isolated_tracer: tuple[Any, Any]) -> None:
        """The release span carries the lock key and derived lock id."""
        tracer, exporter = isolated_tracer
        manager = PostgreSQLLockManager(_session_factory(), tracer=tracer)

        async with manager.acquire("migration:tenant-abc", timeout=5.0):
            pass

        spans = _spans_by_name(exporter, "eventsource.lock.release")
        assert len(spans) == 1
        attrs = _attrs(spans[0])
        assert attrs[ATTR_LOCK_KEY] == "migration:tenant-abc"
        assert attrs[ATTR_LOCK_ID] == PostgreSQLLockManager._key_to_lock_id("migration:tenant-abc")
