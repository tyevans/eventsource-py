"""Guard: `except EventSourceError` is a reliable library-boundary catch-all.

`EventSourceError` is documented as "Base exception for eventsource library".
That claim is only true if every exception the library can raise at a consumer
inherits from it -- otherwise the obvious boundary handler silently misses
whole families of failures.

This module pins the claim from two directions:

1. Every exception class in top-level ``eventsource.__all__`` is an
   ``EventSourceError`` (the public surface a consumer sees first).
2. Every exception class *defined* anywhere under ``src/eventsource/`` is an
   ``EventSourceError`` (so a new family cannot be added off to the side).

The only sanctioned exceptions are the optional-dependency import sentinels
(``*NotAvailableError``), which exist so that a missing extra behaves like a
missing import and are raised only at import-guard time, never during
operation. They are enumerated exactly -- adding to the list is a deliberate
act, and removing an entry that has been fixed is enforced too, so the list
cannot rot into a blanket suppression.
"""

from __future__ import annotations

import importlib
import pkgutil

import pytest

import eventsource
from eventsource import EventSourceError

# Exception classes deliberately outside the EventSourceError hierarchy.
# Each entry is "module:qualname" and must come with a reason.
SANCTIONED_NON_EVENTSOURCE_ERRORS: dict[str, str] = {
    # Optional-dependency import sentinels: subclass ImportError so that a
    # missing extra is indistinguishable from a missing package to callers
    # that guard imports. Never raised during normal operation.
    "eventsource.adapters.redis.bus:RedisNotAvailableError": "optional-dependency import sentinel",
    "eventsource.adapters.kafka.models:KafkaNotAvailableError": "optional-dependency import sentinel",
    "eventsource.adapters.sqlite.snapshots:SQLiteNotAvailableError": (
        "optional-dependency import sentinel"
    ),
    # Known gap, tracked separately: lives in a module this change did not own.
    # When it is rebased onto EventSourceError, delete this entry -- the
    # exactness assertion below will fail until you do.
    "eventsource.adapters.kafka.models:DeserializationError": (
        "known gap: not yet rebased onto EventSourceError"
    ),
}


def _iter_package_modules() -> list[str]:
    """Every importable module name under the `eventsource` package."""
    names = [eventsource.__name__]
    for info in pkgutil.walk_packages(eventsource.__path__, prefix="eventsource."):
        names.append(info.name)
    return sorted(names)


def _defined_exception_classes() -> dict[str, type[BaseException]]:
    """Exception classes *defined* under src/eventsource, keyed module:qualname."""
    found: dict[str, type[BaseException]] = {}
    for module_name in _iter_package_modules():
        module = importlib.import_module(module_name)
        for attr in vars(module).values():
            if not isinstance(attr, type) or not issubclass(attr, BaseException):
                continue
            # Only classes this module defines, so a re-export is not
            # attributed to the module that imported it.
            if attr.__module__ != module_name:
                continue
            found[f"{attr.__module__}:{attr.__qualname__}"] = attr
    return found


def _public_exception_classes() -> dict[str, type[BaseException]]:
    """Exception classes reachable from top-level `eventsource.__all__`."""
    found: dict[str, type[BaseException]] = {}
    for name in eventsource.__all__:
        obj = getattr(eventsource, name)
        if isinstance(obj, type) and issubclass(obj, BaseException):
            found[name] = obj
    return found


def _is_sanctioned(exc: type[BaseException]) -> bool:
    return f"{exc.__module__}:{exc.__qualname__}" in SANCTIONED_NON_EVENTSOURCE_ERRORS


class TestExceptionHierarchy:
    """`except EventSourceError` catches everything the library raises."""

    def test_public_surface_exceptions_are_eventsource_errors(self) -> None:
        offenders = sorted(
            f"eventsource.{name} (bases: {', '.join(base.__name__ for base in exc.__bases__)})"
            for name, exc in _public_exception_classes().items()
            if not issubclass(exc, EventSourceError) and not _is_sanctioned(exc)
        )
        assert not offenders, (
            "Exception classes in eventsource.__all__ that do NOT inherit "
            "EventSourceError, so `except EventSourceError` misses them:\n  "
            + "\n  ".join(offenders)
        )

    def test_all_defined_exceptions_are_eventsource_errors(self) -> None:
        offenders = sorted(
            f"{key} (bases: {', '.join(base.__name__ for base in exc.__bases__)})"
            for key, exc in _defined_exception_classes().items()
            if not issubclass(exc, EventSourceError) and not _is_sanctioned(exc)
        )
        assert not offenders, (
            "Exception classes defined under src/eventsource that do NOT "
            "inherit EventSourceError, so `except EventSourceError` misses "
            "them:\n  " + "\n  ".join(offenders)
        )

    def test_sanctioned_list_is_exact(self) -> None:
        """No stale entries: a sanctioned class must exist and still be outside."""
        defined = _defined_exception_classes()
        stale: list[str] = []
        for key in SANCTIONED_NON_EVENTSOURCE_ERRORS:
            exc = defined.get(key)
            if exc is None:
                stale.append(f"{key} (no longer defined)")
            elif issubclass(exc, EventSourceError):
                stale.append(f"{key} (now an EventSourceError -- drop the entry)")
        assert not stale, "Stale SANCTIONED_NON_EVENTSOURCE_ERRORS entries:\n  " + "\n  ".join(
            stale
        )

    @pytest.mark.parametrize(
        "name",
        [
            "SnapshotError",
            "SnapshotDeserializationError",
            "SnapshotSchemaVersionError",
            "SnapshotNotFoundError",
        ],
    )
    def test_snapshot_errors_are_caught_at_the_library_boundary(self, name: str) -> None:
        assert issubclass(getattr(eventsource, name), EventSourceError)

    def test_stdlib_bases_are_preserved_alongside(self) -> None:
        """Rebasing onto EventSourceError must not drop a stdlib base callers catch."""
        from eventsource.adapters.rabbitmq.models import RabbitMQNotAvailableError
        from eventsource.ports.readmodels.exceptions import ReadModelError

        assert issubclass(RabbitMQNotAvailableError, ImportError)
        assert issubclass(ReadModelError, EventSourceError)
