"""
Event subscriber and handler protocols.

Note: Protocol definitions have been moved to eventsource.protocols.
Imports from this module still work but emit deprecation warnings.
Use `from eventsource.protocols import EventHandler` instead.

Defines protocols for subscribing to and handling domain events.
These protocols enable a clean separation between the event bus
and the projection system.
"""

import warnings

# Import canonical protocols for re-export
from eventsource.protocols import (
    AsyncEventHandler as _AsyncEventHandler,
)
from eventsource.protocols import (
    EventHandler as _EventHandler,
)
from eventsource.protocols import (
    EventSubscriber as _EventSubscriber,
)
from eventsource.protocols import (
    SyncEventHandler as _SyncEventHandler,
)


def __getattr__(name: str) -> type:
    """
    Handle deprecated imports with warnings.

    This enables deprecation warnings when importing protocols
    (EventHandler, SyncEventHandler, EventSubscriber) from this module.
    """
    if name == "EventHandler":
        warnings.warn(
            "Importing 'EventHandler' from eventsource.projections.protocols is deprecated. "
            "Use 'from eventsource.protocols import EventHandler' instead. "
            "This import will be removed in version 0.5.0.",
            DeprecationWarning,
            stacklevel=2,
        )
        return _EventHandler
    elif name == "SyncEventHandler":
        warnings.warn(
            "Importing 'SyncEventHandler' from eventsource.projections.protocols is deprecated. "
            "Use 'from eventsource.protocols import SyncEventHandler' instead. "
            "This import will be removed in version 0.5.0.",
            DeprecationWarning,
            stacklevel=2,
        )
        return _SyncEventHandler
    elif name == "EventSubscriber":
        warnings.warn(
            "Importing 'EventSubscriber' from eventsource.projections.protocols is deprecated. "
            "Use 'from eventsource.protocols import EventSubscriber' instead. "
            "This import will be removed in version 0.5.0.",
            DeprecationWarning,
            stacklevel=2,
        )
        return _EventSubscriber
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


# For internal use without deprecation warnings
# These are re-exported for backward compatibility in internal code
EventHandler = _EventHandler
SyncEventHandler = _SyncEventHandler
EventSubscriber = _EventSubscriber
AsyncEventHandler = _AsyncEventHandler


__all__ = [
    "EventHandler",
    "SyncEventHandler",
    "EventSubscriber",
    "AsyncEventHandler",
]
