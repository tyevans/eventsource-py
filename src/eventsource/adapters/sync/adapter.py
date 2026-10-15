"""
Synchronous adapter for async EventStore implementations.

This module provides SyncEventStoreAdapter, which wraps any async EventStore
implementation and provides synchronous versions of all its methods.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Coroutine, Sequence
from typing import Any, TypeVar
from uuid import UUID

from eventsource.domain import StreamId
from eventsource.domain.event import DomainEvent
from eventsource.ports import (
    AppendResult,
    CategoryReadOptions,
    EventEnvelope,
    ExpectedVersion,
    FeedReadOptions,
    FullEventStore,
    Position,
    StreamReadOptions,
    collect,
)
from eventsource.ports.lifecycle import SupportsClose

logger = logging.getLogger(__name__)

T = TypeVar("T")


class SyncEventStoreAdapter:
    """
    Synchronous adapter for async EventStore implementations.

    Provides sync versions of all EventStore methods for use in synchronous
    contexts like Celery tasks, Django management commands, or RQ workers.

    There is exactly one supported calling context: a thread with no running
    event loop. Each call gets a fresh loop via asyncio.run(). Calling from a
    thread that already has a running loop raises RuntimeError.

    Thread Safety:
        Thread-safe for concurrent calls from multiple threads.

    Warning:
        Calling this adapter from a thread that already runs an event loop
        raises RuntimeError. Await the async EventStore directly instead,
        or move the sync call to a worker thread (asyncio.to_thread).

    Related:
        `eventsource.testing.sync_facade.SyncStoreFacade` is the test-machinery
        counterpart. This adapter is for production sync callers: per-call
        `asyncio.run` and a timeout on every operation.
    """

    def __init__(
        self,
        store: FullEventStore,
        timeout: float = 30.0,
    ) -> None:
        """Initialize the sync adapter with the wrapped async store and default timeout."""
        self._store = store
        self._timeout = timeout

    def _run_sync(self, coro: Coroutine[Any, Any, T], timeout: float | None = None) -> T:
        """Execute a coroutine synchronously on a fresh event loop."""
        effective_timeout = timeout if timeout is not None else self._timeout

        try:
            asyncio.get_running_loop()
        except RuntimeError:
            pass
        else:
            coro.close()
            raise RuntimeError(
                "SyncEventStoreAdapter was called from a thread with a running "
                "event loop. Blocking that loop on its own work would deadlock. "
                "Await the async EventStore directly, or run this call in a "
                "worker thread (e.g. await asyncio.to_thread(...))."
            )

        try:
            return asyncio.run(asyncio.wait_for(coro, timeout=effective_timeout))
        except TimeoutError as e:
            raise TimeoutError(f"Sync operation timed out after {effective_timeout}s") from e

    def append(
        self,
        stream: StreamId,
        events: Sequence[DomainEvent],
        expected: ExpectedVersion,
        *,
        timeout: float | None = None,
    ) -> AppendResult:
        """Synchronously append events to a stream."""
        return self._run_sync(self._store.append(stream, events, expected), timeout=timeout)

    def read_stream(
        self,
        stream: StreamId,
        options: StreamReadOptions | None = None,
        *,
        timeout: float | None = None,
    ) -> list[EventEnvelope]:
        """Synchronously read events from a stream."""
        return self._run_sync(collect(self._store.read_stream(stream, options)), timeout=timeout)

    def get_stream_version(self, stream: StreamId, *, timeout: float | None = None) -> int:
        """Synchronously get the current version of a stream (0 if no events)."""
        return self._run_sync(self._store.get_stream_version(stream), timeout=timeout)

    def event_exists(self, event_id: UUID, *, timeout: float | None = None) -> bool:
        """Synchronously check if an event exists."""
        return self._run_sync(self._store.event_exists(event_id), timeout=timeout)

    def read_all(
        self,
        from_position: Position | None = None,
        options: FeedReadOptions | None = None,
        *,
        timeout: float | None = None,
    ) -> list[EventEnvelope]:
        """Synchronously read events across the global feed into a list."""
        return self._run_sync(
            collect(self._store.read_all(from_position, options)), timeout=timeout
        )

    def read_category(
        self,
        category: str,
        options: CategoryReadOptions | None = None,
        *,
        timeout: float | None = None,
    ) -> list[EventEnvelope]:
        """Synchronously read events for a category (all aggregates of a type)."""
        return self._run_sync(
            collect(self._store.read_category(category, options)), timeout=timeout
        )

    def current_position(self, *, timeout: float | None = None) -> Position | None:
        """Synchronously get the current maximum position in the global feed."""
        return self._run_sync(self._store.current_position(), timeout=timeout)

    def close(self, *, timeout: float | None = None) -> None:
        """Synchronously release the wrapped store's resources if it implements SupportsClose."""
        store = self._store
        if isinstance(store, SupportsClose):
            self._run_sync(store.close(), timeout=timeout)

    def __enter__(self) -> SyncEventStoreAdapter:
        """Enter a context manager that closes the store on exit."""
        return self

    def __exit__(self, *exc_info: object) -> None:
        """Close the wrapped store."""
        self.close()

    @property
    def wrapped_store(self) -> FullEventStore:
        """Get the underlying async event store."""
        return self._store

    @property
    def timeout(self) -> float:
        """Get the default timeout for operations."""
        return self._timeout

    def __repr__(self) -> str:
        """Return string representation."""
        return f"SyncEventStoreAdapter({type(self._store).__name__}, timeout={self._timeout})"


__all__ = ["SyncEventStoreAdapter"]
