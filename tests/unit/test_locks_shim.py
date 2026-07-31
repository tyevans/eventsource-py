"""The `eventsource.locks` deprecation shim resolves every legacy name."""

import warnings

import pytest

MOVED = {
    "LockInfo": "eventsource.ports.locks",
    "migration_lock_key": "eventsource.ports.locks",
    "LockAcquisitionError": "eventsource.exceptions",
    "LockNotHeldError": "eventsource.exceptions",
    "PostgreSQLLockManager": "eventsource.adapters.postgresql.locks",
}


@pytest.mark.parametrize(("name", "new_path"), sorted(MOVED.items()))
def test_legacy_name_resolves_with_a_deprecation_warning(name: str, new_path: str) -> None:
    import eventsource.locks as shim

    with pytest.warns(DeprecationWarning, match=new_path):
        attribute = getattr(shim, name)
    assert attribute is not None


def test_dir_lists_every_moved_name() -> None:
    import eventsource.locks as shim

    assert set(MOVED) <= set(dir(shim))


def test_unknown_attribute_raises_attribute_error() -> None:
    import eventsource.locks as shim

    with pytest.raises(AttributeError):
        shim.NotAThing  # noqa: B018


def test_lock_exceptions_are_eventsource_errors() -> None:
    from eventsource.exceptions import (
        EventSourceError,
        LockAcquisitionError,
        LockNotHeldError,
    )

    assert issubclass(LockAcquisitionError, EventSourceError)
    assert issubclass(LockNotHeldError, EventSourceError)


def test_importing_the_shim_emits_no_warning_by_itself() -> None:
    import importlib

    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        importlib.import_module("eventsource.locks")
