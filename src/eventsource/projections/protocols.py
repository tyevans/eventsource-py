"""
Event subscriber and handler protocols.

Protocol definitions are in eventsource.protocols.
Use `from eventsource.protocols import EventHandler` instead.

This module provides the AsyncEventHandler abstract base class.
"""

# Import from canonical location for re-export
from eventsource.protocols import AsyncEventHandler

__all__ = [
    "AsyncEventHandler",
]
