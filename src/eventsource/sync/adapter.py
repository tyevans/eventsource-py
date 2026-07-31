"""
Synchronous adapter for async EventStore implementations.

This module provides SyncEventStoreAdapter, which wraps any async EventStore
implementation and provides synchronous versions of all its methods.
"""

from __future__ import annotations

import asyncio
import logging
import threading
from collections.abc import Coroutine, Sequence
from concurrent.futures import ThreadPoolExecutor
from typing import Any, TypeVar
from uuid import UUID

from eventsource.domain import StreamId
from eventsource.events.base import DomainEvent
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

logger = logging.getLogger(__name__)

T = TypeVar("T")


class SyncEventStoreAdapter:
    """
    Synchronous adapter for async EventStore implementations.

    Provides sync versions of all EventStore methods for use in
    synchronous contexts like Celery tasks, Django management commands,
    or RQ workers.

    Handles two event loop scenarios:
    1. No running event loop -> uses asyncio.run() (a fresh loop per call)
    2. A running event loop is detected on the calling thread ->
       uses run_coroutine_threadsafe() against that loop directly

    Thread Safety:
        The adapter is thread-safe for concurrent calls from multiple threads.
        Each sync call is independent and uses appropriate synchronization.

    Example:
        >>> from sqlalchemy.ext.asyncio import create_async_engine
        >>> from eventsource.adapters.postgresql import PostgreSQLEventStore
        >>> from eventsource.domain import StreamId
        >>> from eventsource.ports import ExpectedVersion
        >>> from eventsource.sync import SyncEventStoreAdapter
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
        Using this adapter from within a running async context (e.g., an
        async view that calls a sync library) will log a warning and use
        run_coroutine_threadsafe(), which has additional overhead. Consider
        using the async EventStore directly in async contexts.

    Related:
        `eventsource.testing.sync_facade.SyncStoreFacade` is the test-machinery
        counterpart: it owns one private event loop for its lifetime and has no
        timeouts. This adapter is for production sync callers (Celery, Django
        management commands, RQ): per-call `asyncio.run`, a running-loop
        `run_coroutine_threadsafe` fallback, and a timeout on every operation.
    """

    # Retained for API compatibility only: `_run_sync` never dispatches work
    # to this executor. It is not used by the running-loop path either --
    # that path runs the coroutine on the caller's own loop via
    # `run_coroutine_threadsafe`, not on a worker thread from this pool.
    _executor: ThreadPoolExecutor | None = None
    _executor_lock: threading.Lock = threading.Lock()

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

    @classmethod
    def _get_executor(cls) -> ThreadPoolExecutor:
        """Get or create the shared thread pool executor.

        Retained for API compatibility; `_run_sync` does not use this
        executor for either event loop scenario it handles.
        """
        with cls._executor_lock:
            if cls._executor is None:
                cls._executor = ThreadPoolExecutor(
                    max_workers=4,
                    thread_name_prefix="sync_adapter",
                )
            return cls._executor

    @classmethod
    def shutdown_executor(cls) -> None:
        """
        Shutdown the shared thread pool executor.

        Retained for API compatibility. This executor is not used by
        `_run_sync` -- the running-loop path runs coroutines on the caller's
        own loop via `run_coroutine_threadsafe`, not on a worker thread here.
        Calling this is safe (a no-op beyond releasing the pool, if one was
        ever created via `_get_executor`) but has no effect on adapter
        behavior. After calling this, the executor will be recreated on next
        use of `_get_executor`.
        """
        with cls._executor_lock:
            if cls._executor is not None:
                cls._executor.shutdown(wait=True)
                cls._executor = None

    def _run_sync(self, coro: Coroutine[Any, Any, T], timeout: float | None = None) -> T:
        """
        Execute coroutine synchronously, handling all event loop scenarios.

        Args:
            coro: The coroutine to execute
            timeout: Optional timeout override

        Returns:
            The result of the coroutine

        Raises:
            TimeoutError: If operation exceeds timeout
            Exception: Any exception raised by the coroutine
        """
        effective_timeout = timeout if timeout is not None else self._timeout

        try:
            # Check if there's a running event loop
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop is not None:
            # We're in a running loop - use thread pool
            logger.warning(
                "SyncEventStoreAdapter called from running event loop. "
                "Consider using async EventStore directly for better performance."
            )

            future = asyncio.run_coroutine_threadsafe(coro, loop)

            try:
                return future.result(timeout=effective_timeout)
            except TimeoutError:
                future.cancel()
                raise TimeoutError(
                    f"Sync operation timed out after {effective_timeout}s "
                    "(called from running event loop)"
                ) from None

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
