"""`eventsource.domain.exceptions` and `eventsource.domain.types` are home.

The entities ring owns the exception hierarchy and the type aliases outright
(no deprecation shim -- the library has no external users yet). This test
locks in that every name is importable from its ring home and that the
subset re-exported at the top level still resolves there too.
"""

import eventsource
import eventsource.domain
import eventsource.domain.exceptions as domain_exceptions
import eventsource.domain.types as domain_types

EXCEPTION_NAMES = [
    "EventSourceError",
    "OptimisticLockError",
    "EventNotFoundError",
    "ProjectionError",
    "AggregateNotFoundError",
    "CommandRejectedError",
    "EventStoreError",
    "EventBusError",
    "SerializationError",
    "EventVersionError",
    "UnhandledEventError",
    "AggregateNotCreatedError",
    "HandlerDispatchError",
    "DuplicateEventError",
    "SnapshotError",
    "SnapshotDeserializationError",
    "SnapshotSchemaVersionError",
    "SnapshotNotFoundError",
]

TYPE_NAMES = [
    "TState",
    "AggregateId",
    "EventId",
    "TenantId",
    "CorrelationId",
    "CausationId",
    "Version",
    "StreamPosition",
    "GlobalPosition",
]

TOP_LEVEL_EXCEPTION_NAMES = [
    "AggregateNotCreatedError",
    "AggregateNotFoundError",
    "CommandRejectedError",
    "DuplicateEventError",
    "EventNotFoundError",
    "EventSourceError",
    "EventVersionError",
    "HandlerDispatchError",
    "OptimisticLockError",
    "ProjectionError",
    "SnapshotDeserializationError",
    "SnapshotError",
    "SnapshotNotFoundError",
    "SnapshotSchemaVersionError",
]

TOP_LEVEL_TYPE_NAMES = [
    "TState",
    "AggregateId",
    "EventId",
    "TenantId",
    "CorrelationId",
    "CausationId",
]


def test_every_exception_lives_in_domain_exceptions() -> None:
    for name in EXCEPTION_NAMES:
        attribute = getattr(domain_exceptions, name)
        assert issubclass(attribute, Exception)


def test_every_type_alias_lives_in_domain_types() -> None:
    for name in TYPE_NAMES:
        assert hasattr(domain_types, name)


def test_snapshot_error_hierarchy() -> None:
    assert issubclass(
        domain_exceptions.SnapshotDeserializationError, domain_exceptions.SnapshotError
    )
    assert issubclass(domain_exceptions.SnapshotSchemaVersionError, domain_exceptions.SnapshotError)
    assert issubclass(domain_exceptions.SnapshotNotFoundError, domain_exceptions.SnapshotError)
    # Deliberately not an EventSourceError -- preserved verbatim from the
    # pre-move contract.
    assert not issubclass(domain_exceptions.SnapshotError, domain_exceptions.EventSourceError)


def test_domain_package_re_exports_exceptions_and_types() -> None:
    for name in EXCEPTION_NAMES:
        assert getattr(eventsource.domain, name) is getattr(domain_exceptions, name)
    for name in TYPE_NAMES:
        assert getattr(eventsource.domain, name) is getattr(domain_types, name)


def test_top_level_package_re_exports_without_going_through_a_shim() -> None:
    for name in TOP_LEVEL_EXCEPTION_NAMES:
        assert getattr(eventsource, name) is getattr(domain_exceptions, name)
    for name in TOP_LEVEL_TYPE_NAMES:
        assert getattr(eventsource, name) is getattr(domain_types, name)


class TestNoBuiltinBases:
    def test_registry_errors_are_not_builtin_lookup_errors(self) -> None:
        from eventsource.domain.exceptions import (
            DuplicateEventTypeError,
            EventTypeNotFoundError,
            HandlerSignatureError,
        )

        assert not issubclass(EventTypeNotFoundError, KeyError)
        assert not issubclass(DuplicateEventTypeError, ValueError)
        assert not issubclass(HandlerSignatureError, ValueError)

    def test_not_found_message_is_not_requoted(self) -> None:
        from eventsource.domain.exceptions import EventTypeNotFoundError

        err = EventTypeNotFoundError("OrderCreated", ["A", "B"])
        assert not str(err).startswith("'")  # KeyError.__str__ used to re-quote


class TestDomainFacadeComplete:
    def test_every_public_domain_exception_is_exported(self) -> None:
        import eventsource.domain as domain
        from eventsource.domain import exceptions as ex

        public = {
            name
            for name in dir(ex)
            if isinstance(getattr(ex, name), type)
            and issubclass(getattr(ex, name), Exception)
            and not name.startswith("_")
            and getattr(ex, name).__module__ == "eventsource.domain.exceptions"
        }
        exported = set(domain.__all__)
        missing = public - exported
        assert not missing, f"domain/__init__ is missing: {sorted(missing)}"
