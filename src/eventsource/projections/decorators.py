"""
Event handler decorators (deprecated location).

.. deprecated::
    This module re-exports decorators from eventsource.handlers.decorators
    for backward compatibility. Use ``from eventsource.handlers import handles``
    instead.

This import path will be removed in version 0.4.0.
"""

import warnings
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    # For type checkers, expose the actual types
    from eventsource.handlers.decorators import (
        get_handled_event_type as get_handled_event_type,
    )
    from eventsource.handlers.decorators import (
        handles as handles,
    )
    from eventsource.handlers.decorators import (
        is_event_handler as is_event_handler,
    )


def __getattr__(name: str) -> object:
    """Lazy import with deprecation warning for backward compatibility."""
    if name in ("handles", "get_handled_event_type", "is_event_handler"):
        warnings.warn(
            f"Importing '{name}' from eventsource.projections.decorators is deprecated. "
            f"Use 'from eventsource.handlers import {name}' instead. "
            "This import will be removed in version 0.4.0.",
            DeprecationWarning,
            stacklevel=2,
        )
        from eventsource.handlers import decorators

        return getattr(decorators, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    """List available names for tab completion."""
    return ["handles", "get_handled_event_type", "is_event_handler"]
