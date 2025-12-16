"""
Synchronous adapters for async eventsource components.

This module provides synchronous wrappers for async components,
enabling their use in synchronous contexts like Celery tasks,
Django management commands, or RQ workers.

Example:
    >>> from eventsource.stores import PostgreSQLEventStore
    >>> from eventsource.sync import SyncEventStoreAdapter
    >>>
    >>> async_store = PostgreSQLEventStore(database_url)
    >>> sync_store = SyncEventStoreAdapter(async_store, timeout=30.0)
    >>>
    >>> # In a Celery task
    >>> @celery.task
    >>> def process_order(order_id: str):
    ...     events = sync_store.get_events_sync(UUID(order_id), "Order")
    ...     # Process events...
"""

from eventsource.sync.adapter import SyncEventStoreAdapter

__all__ = ["SyncEventStoreAdapter"]
