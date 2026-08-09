"""Public API surface tests.

Asserts that every name intended for top-level export actually imports
from `eventsource` and appears in `eventsource.__all__`. Also pins the
blessed store surface: after the legacy `eventsource.stores` package was
retired, each store name at top level has exactly one referent -- a port
value object or an adapter class -- and the legacy names are gone.
"""

import pytest

import eventsource

# Names that must be importable from the top-level package and present
# in __all__.
CORE_RINGS_EXPORTS = [
    "StreamId",
    "Position",
    "EventEnvelope",
    "AppendResult",
    "ExpectedVersion",
    "ReadDirection",
    "StreamReadOptions",
    "FeedReadOptions",
    "CategoryReadOptions",
    "EventAppender",
    "StreamReader",
    "EventLookup",
    "GlobalEventFeed",
    "CategoryQuery",
    "FullEventStore",
    "AggregateStore",
    "EventPublisher",
    "collect",
    "InMemoryEventStore",
    "PostgreSQLEventStore",
    "SQLiteEventStore",
    "ASYNCPG_AVAILABLE",
    "AIOSQLITE_AVAILABLE",
    "SyncEventStoreAdapter",
    "DuplicateEventError",
    "PositionDecodeError",
    "PositionForeignError",
    "IntPositionCodec",
    "Snapshot",
    "SnapshotStore",
    "InMemorySnapshotStore",
    "SnapshotError",
    "SnapshotDeserializationError",
    "SnapshotSchemaVersionError",
    "SnapshotNotFoundError",
    "OutboxRepository",
    "OutboxEntry",
    "OutboxStats",
    "outbox_event_data",
    "InMemoryOutboxRepository",
    "PostgreSQLOutboxRepository",
]

# Spec section 4.2: names retired with the legacy store surface. None of
# them may be reachable from the top-level package any more.
DEAD_NAMES = [
    "EventStore",
    "EventStream",
    "StoredEvent",
    "ReadOptions",
    "LegacyStoreAdapter",
    "TypeConverter",
    "DefaultTypeConverter",
    "DEFAULT_UUID_FIELDS",
    "DEFAULT_STRING_ID_FIELDS",
    "MemoryEventStore",
    "OutboxRepositoryProtocol",
]


def test_core_rings_names_are_importable_from_eventsource() -> None:
    for name in CORE_RINGS_EXPORTS:
        assert hasattr(eventsource, name), f"eventsource.{name} is not importable"


def test_core_rings_names_are_in_dunder_all() -> None:
    for name in CORE_RINGS_EXPORTS:
        assert name in eventsource.__all__, f"eventsource.{name} is missing from __all__"


def test_dunder_all_has_no_duplicates() -> None:
    assert len(eventsource.__all__) == len(set(eventsource.__all__))


def test_every_dunder_all_name_is_importable() -> None:
    for name in eventsource.__all__:
        assert hasattr(eventsource, name), f"eventsource.{name} is listed in __all__ but missing"


class TestBlessedStoreSurface:
    """Spec section 4.1: one referent per name. The ports and adapters own them all."""

    def test_top_level_expected_version_is_the_port_vo(self) -> None:
        from eventsource.ports import ExpectedVersion

        assert eventsource.ExpectedVersion is ExpectedVersion

    def test_top_level_read_direction_is_the_port_enum(self) -> None:
        from eventsource.ports import ReadDirection

        assert eventsource.ReadDirection is ReadDirection

    def test_top_level_append_result_is_the_port_vo(self) -> None:
        from eventsource.ports import AppendResult

        assert eventsource.AppendResult is AppendResult

    def test_top_level_event_publisher_is_the_port_protocol(self) -> None:
        from eventsource.ports import EventPublisher

        assert eventsource.EventPublisher is EventPublisher

    def test_top_level_in_memory_event_store_is_the_memory_adapter(self) -> None:
        from eventsource.adapters.memory.store import InMemoryEventStore

        assert eventsource.InMemoryEventStore is InMemoryEventStore

    def test_top_level_sqlite_event_store_is_the_sqlite_adapter(self) -> None:
        from eventsource.adapters.sqlite import SQLiteEventStore

        assert eventsource.SQLiteEventStore is SQLiteEventStore

    def test_top_level_postgresql_event_store_is_the_postgresql_adapter(self) -> None:
        from eventsource.adapters.postgresql import PostgreSQLEventStore

        assert eventsource.PostgreSQLEventStore is PostgreSQLEventStore

    def test_top_level_outbox_repository_is_the_port_protocol(self) -> None:
        from eventsource.ports import OutboxRepository

        assert eventsource.OutboxRepository is OutboxRepository

    def test_top_level_outbox_entry_is_the_port_vo(self) -> None:
        from eventsource.ports import OutboxEntry

        assert eventsource.OutboxEntry is OutboxEntry

    def test_top_level_outbox_stats_is_the_port_vo(self) -> None:
        from eventsource.ports import OutboxStats

        assert eventsource.OutboxStats is OutboxStats

    def test_top_level_in_memory_outbox_is_the_memory_adapter(self) -> None:
        from eventsource.adapters.memory.outbox import InMemoryOutboxRepository

        assert eventsource.InMemoryOutboxRepository is InMemoryOutboxRepository

    def test_top_level_postgresql_outbox_is_the_postgresql_adapter(self) -> None:
        from eventsource.adapters.postgresql import PostgreSQLOutboxRepository

        assert eventsource.PostgreSQLOutboxRepository is PostgreSQLOutboxRepository

    def test_top_level_domain_event_is_the_domain_entity(self) -> None:
        from eventsource.domain.event import DomainEvent

        assert eventsource.DomainEvent is DomainEvent

    def test_top_level_event_registry_is_the_domain_registry(self) -> None:
        from eventsource.domain.event_registry import EventRegistry

        assert eventsource.EventRegistry is EventRegistry

    def test_top_level_event_type_not_found_error_is_the_domain_exception(self) -> None:
        from eventsource.domain.exceptions import EventTypeNotFoundError

        assert eventsource.EventTypeNotFoundError is EventTypeNotFoundError

    def test_top_level_duplicate_event_type_error_is_the_domain_exception(self) -> None:
        from eventsource.domain.exceptions import DuplicateEventTypeError

        assert eventsource.DuplicateEventTypeError is DuplicateEventTypeError


class TestLegacyStoreSurfaceIsGone:
    """Spec section 4.2: the legacy names and their import path are retired."""

    def test_dead_names_are_gone_from_the_public_api(self) -> None:
        for name in DEAD_NAMES:
            assert not hasattr(eventsource, name), f"eventsource.{name} should not exist"
            assert name not in eventsource.__all__

    def test_legacy_stores_package_is_not_importable(self) -> None:
        with pytest.raises(ModuleNotFoundError):
            import eventsource.stores  # noqa: F401

    def test_legacy_repositories_package_is_not_importable(self) -> None:
        with pytest.raises(ModuleNotFoundError):
            import eventsource.repositories  # noqa: F401

    def test_outbox_repository_protocol_has_no_list_pending_events(self) -> None:
        from eventsource.ports import OutboxRepository

        assert not hasattr(OutboxRepository, "list_pending_events")

    def test_legacy_subscriptions_package_is_not_importable(self) -> None:
        with pytest.raises(ModuleNotFoundError):
            import eventsource.subscriptions  # noqa: F401

    def test_legacy_events_package_is_not_importable(self) -> None:
        with pytest.raises(ModuleNotFoundError):
            import eventsource.events  # noqa: F401

    def test_legacy_handlers_package_is_not_importable(self) -> None:
        with pytest.raises(ModuleNotFoundError):
            import eventsource.handlers  # noqa: F401

    def test_legacy_internal_package_is_not_importable(self) -> None:
        with pytest.raises(ModuleNotFoundError):
            import eventsource._internal  # noqa: F401

    def test_legacy_migration_package_is_not_importable(self) -> None:
        with pytest.raises(ModuleNotFoundError):
            import eventsource.migration  # noqa: F401

    def test_legacy_multitenancy_package_is_not_importable(self) -> None:
        with pytest.raises(ModuleNotFoundError):
            import eventsource.multitenancy  # noqa: F401

    def test_legacy_migrations_package_is_not_importable(self) -> None:
        with pytest.raises(ModuleNotFoundError):
            import eventsource.migrations  # noqa: F401
