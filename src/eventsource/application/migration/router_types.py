"""
Errors and type re-exports for tenant store router.
"""

from __future__ import annotations

from eventsource.application.migration.write_pause import (
    PauseMetrics,
    WritePausedError,
    WritePauseManager,
)
from eventsource.domain.exceptions import EventSourceError


class StoreNotFoundError(EventSourceError):
    """
    Raised when a store ID cannot be resolved to a registered store.

    This error indicates that a routing configuration references a store ID
    that has not been registered with the router.

    Attributes:
        store_id: The store ID that could not be found.
    """

    def __init__(self, store_id: str):
        self.store_id = store_id
        super().__init__(f"Store not found: {store_id}")


__all__ = [
    "PauseMetrics",
    "StoreNotFoundError",
    "WritePausedError",
    "WritePauseManager",
]
