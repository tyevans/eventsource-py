"""
Shared JSON encoder for UUID and datetime serialization.

.. deprecated::
    This module is deprecated. Import from eventsource.serialization instead:
    ``from eventsource.serialization import json_dumps, EventSourceJSONEncoder``

    This import path will be removed in version 0.4.0.
"""

import warnings
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from eventsource.serialization.json import (
        EventSourceJSONEncoder as EventSourceJSONEncoder,
    )
    from eventsource.serialization.json import (
        json_dumps as json_dumps,
    )
    from eventsource.serialization.json import (
        json_loads as json_loads,
    )


def __getattr__(name: str) -> object:
    """Lazy import with deprecation warning for backward compatibility."""
    if name in ("EventSourceJSONEncoder", "json_dumps", "json_loads"):
        warnings.warn(
            f"Importing '{name}' from eventsource.repositories._json is deprecated. "
            f"Use 'from eventsource.serialization import {name}' instead. "
            "This import will be removed in version 0.4.0.",
            DeprecationWarning,
            stacklevel=2,
        )
        from eventsource.serialization import json as json_module

        return getattr(json_module, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    """List available names for tab completion."""
    return ["EventSourceJSONEncoder", "json_dumps", "json_loads"]
