"""Deprecated import path for the distributed-lock subsystem.

Every name below still resolves, each with a `DeprecationWarning` naming its
new home. This package is removed in 0.8.0.

- `LockInfo`, `migration_lock_key` -> `eventsource.ports.locks`
- `LockAcquisitionError`, `LockNotHeldError` -> `eventsource.exceptions`
- `PostgreSQLLockManager` -> `eventsource.adapters.postgresql.locks`

Resolution is lazy: importing this module pulls in neither sqlalchemy nor
the PostgreSQL adapter until a name that needs them is actually read.
"""

import importlib
import warnings

_MOVED = {
    "LockInfo": "eventsource.ports.locks",
    "migration_lock_key": "eventsource.ports.locks",
    "LockAcquisitionError": "eventsource.exceptions",
    "LockNotHeldError": "eventsource.exceptions",
    "PostgreSQLLockManager": "eventsource.adapters.postgresql.locks",
}

__all__ = [
    "LockAcquisitionError",
    "LockInfo",
    "LockNotHeldError",
    "PostgreSQLLockManager",
    "migration_lock_key",
]


def __getattr__(name: str) -> object:
    try:
        module_name = _MOVED[name]
    except KeyError:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from None
    warnings.warn(
        f"eventsource.locks.{name} is deprecated; "
        f"import it from {module_name} instead. "
        f"eventsource.locks is removed in 0.8.0.",
        DeprecationWarning,
        stacklevel=2,
    )
    return getattr(importlib.import_module(module_name), name)


def __dir__() -> list[str]:
    return sorted(__all__)
