"""
Synchronous adapters for async eventsource components.

This module provides synchronous wrappers for async components,
enabling their use in synchronous contexts like Celery tasks,
Django management commands, or RQ workers.

Example:
    >>> from sqlalchemy.ext.asyncio import create_async_engine
    >>> from eventsource.adapters.postgresql import PostgreSQLEventStore
    >>> from eventsource.domain import StreamId
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
    ...     # Process events...
"""

from eventsource.sync.adapter import SyncEventStoreAdapter

__all__ = ["SyncEventStoreAdapter"]
