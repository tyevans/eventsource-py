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

    Provides sync versions of all EventStore methods for use in
    synchronous contexts like Celery tasks, Django management commands,
    or RQ workers.

    There is exactly one supported calling context: a thread with no running
    event loop. Each call gets a fresh loop via asyncio.run(). Calling from a
    thread that already has a running loop raises RuntimeError -- see the
    Warning below.

    Thread Safety:
        The adapter is thread-safe for concurrent calls from multiple threads.
        Each sync call is independent and uses appropriate synchronization.

    Example:
        >>> from sqlalchemy.ext.asyncio import create_async_engine
        >>> from eventsource.adapters.postgresql import PostgreSQLEventStore
        >>> from eventsource.domain import StreamId
        >>> from eventsource.ports import ExpectedVersion
        >>> from eventsource.adapters.sync import SyncEventStoreAdapter
        >>>
        >>> engine = create_async_engine(database_url)
        >>> sync_store = SyncEventStoreAdapter(PostgreSQLEventStore(engine), timeout=30.0)
        >>>
        >>> # In a Celery task
        >>> @celery.task
        >>> def process_order(order_id: str):
        ...     stream = StreamId(aggregate_id=UUID(order_id), category="Order")
        ...     envelopes = sync_store.read_stream(stream)
        ...     sync_store.append(
        ...         stream,
        ...         [new_event],
        ...         ExpectedVersion.exact(len(envelopes)),
        ...     )

    Warning:
        Calling this adapter from a thread that already runs an event loop
        (e.g. an async view reaching for a sync helper) raises RuntimeError.
        There is no correct way to block a running loop's own thread on work
        that loop must execute. Await the async EventStore directly instead,
        or move the sync call to a worker thread (asyncio.to_thread).

    Related:
        `eventsource.testing.sync_facade.SyncStoreFacade` is the test-machinery
        counterpart: it owns one private event loop for its lifetime and has no
        timeouts. This adapter is for production sync callers (Celery, Django
        management commands, RQ): per-call `asyncio.run` and a timeout on every
        operation.
    """

    def __init__(
        self,
        store: FullEventStore,
        timeout: float = 30.0,
    ) -> None:
        """Initialize the sync adapter.

        Args:
            store: The async, port-shaped event store to wrap
            timeout: Default timeout in seconds for all operations (default: 30.0)
        """
        self._store = store
        self._timeout = timeout

    def _run_sync(self, coro: Coroutine[Any, Any, T], timeout: float | None = None) -> T:
        """
        Execute a coroutine synchronously on a fresh event loop.

        Args:
            coro: The coroutine to execute
            timeout: Optional timeout override

        Returns:
            The result of the coroutine

        Raises:
            RuntimeError: If called from a thread with a running event loop
            TimeoutError: If operation exceeds timeout
            Exception: Any exception raised by the coroutine
        """
        effective_timeout = timeout if timeout is not None else self._timeout

        try:
            asyncio.get_running_loop()
        except RuntimeError:
            pass
        else:
            # Running the coroutine on the caller's own loop and then blocking
            # that loop's thread for the result is a guaranteed deadlock: the
            # loop cannot execute what it was just handed. There is no fallback
            # that makes this work, so refuse it plainly.
            coro.close()
            raise RuntimeError(
                "SyncEventStoreAdapter was called from a thread with a running "
                "event loop. Blocking that loop on its own work would deadlock. "
                "Await the async EventStore directly, or run this call in a "
                "worker thread (e.g. await asyncio.to_thread(...))."
            )

        # Create a new event loop with asyncio.run()
        # This is the recommended approach for Python 3.10+
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
        """
        Synchronously append events to a stream.

        Args:
            stream: Identity of the stream to append to
            events: Sequence of events to append
            expected: Expected version for optimistic concurrency control
            timeout: Override default timeout for this operation

        Returns:
            AppendResult with new version and global position

        Raises:
            OptimisticLockError: If expected doesn't match current version
            TimeoutError: If operation exceeds timeout
            EventStoreError: If storage operation fails
        """
        return self._run_sync(self._store.append(stream, events, expected), timeout=timeout)

    def read_stream(
        self,
        stream: StreamId,
        options: StreamReadOptions | None = None,
        *,
        timeout: float | None = None,
    ) -> list[EventEnvelope]:
        """
        Synchronously read events from a stream.

        Args:
            stream: Identity of the stream to read
            options: Read options for direction, version range, and limit
            timeout: Override default timeout for this operation

        Returns:
            List of EventEnvelope for the matching events
        """
        return self._run_sync(collect(self._store.read_stream(stream, options)), timeout=timeout)

    def get_stream_version(self, stream: StreamId, *, timeout: float | None = None) -> int:
        """
        Synchronously get the current version of a stream.

        Args:
            stream: Identity of the stream
            timeout: Override default timeout for this operation

        Returns:
            Current stream version (0 if no events)
        """
        return self._run_sync(self._store.get_stream_version(stream), timeout=timeout)

    def event_exists(self, event_id: UUID, *, timeout: float | None = None) -> bool:
        """
        Synchronously check if an event exists.

        Args:
            event_id: The event's unique identifier
            timeout: Override default timeout for this operation

        Returns:
            True if event exists, False otherwise
        """
        return self._run_sync(self._store.event_exists(event_id), timeout=timeout)

    def read_all(
        self,
        from_position: Position | None = None,
        options: FeedReadOptions | None = None,
        *,
        timeout: float | None = None,
    ) -> list[EventEnvelope]:
        """
        Synchronously read events across the global feed.

        Note: This method collects all events from the async iterator into a list.
        For large event stores, consider using pagination via FeedReadOptions.limit.

        Args:
            from_position: Position to read from (exclusive); None for the start
            options: Read options for tenant filtering and limit
            timeout: Override default timeout for this operation

        Returns:
            List of EventEnvelope for the matching events
        """
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
        """
        Synchronously read events for a category (all aggregates of a type).

        Args:
            category: The category to read
            options: Read options for tenant filtering, timestamp, and limit
            timeout: Override default timeout for this operation

        Returns:
            List of EventEnvelope for the matching events
        """
        return self._run_sync(
            collect(self._store.read_category(category, options)), timeout=timeout
        )

    def current_position(self, *, timeout: float | None = None) -> Position | None:
        """
        Synchronously get the current maximum position in the global feed.

        Args:
            timeout: Override default timeout for this operation

        Returns:
            The maximum position, or None if the store is empty.
        """
        return self._run_sync(self._store.current_position(), timeout=timeout)

    def close(self, *, timeout: float | None = None) -> None:
        """
        Synchronously release the wrapped store's resources.

        A no-op when the wrapped store does not implement `SupportsClose`.
        Without this, a sync caller holding a store that owns a connection
        (SQLite, PostgreSQL with `owns_engine`) has no way to release it and
        the process hangs at exit. Idempotent, like the port it delegates to.

        Args:
            timeout: Override default timeout for this operation
        """
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
