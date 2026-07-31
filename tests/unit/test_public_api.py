"""Public API surface tests.

Asserts that every name intended for top-level export actually imports
from `eventsource` and appears in `eventsource.__all__`. Also pins the
Task 14 collision decisions: names that intentionally do NOT get rebound
at top level because a same-named, different class already occupies that
slot in the legacy surface.
"""

import eventsource

# Core-rings surface (Task 14): names that must be importable from the
# top-level package and present in __all__.
CORE_RINGS_EXPORTS = [
    "StreamId",
    "Position",
    "EventEnvelope",
    "AppendResult",
    "StreamReadOptions",
    "FeedReadOptions",
    "CategoryReadOptions",
    "EventAppender",
    "StreamReader",
    "EventLookup",
    "GlobalEventFeed",
    "CategoryQuery",
    "FullEventStore",
    "collect",
    "MemoryEventStore",
    "LegacyStoreAdapter",
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


class TestCollisionDecisions:
    """Task 14: colliding new names stay path-only, existing exports untouched.

    `eventsource.AppendResult`, `eventsource.ExpectedVersion`, and
    `eventsource.ReadDirection` remain bound to the legacy
    `stores.interface` classes. The new port-layer VOs of the same name
    are available only via `eventsource.ports`.
    """

    def test_top_level_append_result_is_legacy_class(self) -> None:
        from eventsource.stores.interface import AppendResult as LegacyAppendResult

        assert eventsource.AppendResult is LegacyAppendResult

    def test_top_level_expected_version_is_legacy_class(self) -> None:
        from eventsource.stores.interface import ExpectedVersion as LegacyExpectedVersion

        assert eventsource.ExpectedVersion is LegacyExpectedVersion

    def test_top_level_read_direction_is_legacy_enum(self) -> None:
        from eventsource.stores.interface import ReadDirection as LegacyReadDirection

        assert eventsource.ReadDirection is LegacyReadDirection

    def test_ports_append_result_is_a_distinct_class(self) -> None:
        from eventsource.ports import AppendResult as PortAppendResult

        assert PortAppendResult is not eventsource.AppendResult

    def test_ports_expected_version_is_a_distinct_class(self) -> None:
        from eventsource.ports import ExpectedVersion as PortExpectedVersion

        assert PortExpectedVersion is not eventsource.ExpectedVersion

    def test_ports_read_direction_is_a_distinct_class(self) -> None:
        from eventsource.ports import ReadDirection as PortReadDirection

        assert PortReadDirection is not eventsource.ReadDirection

    def test_top_level_sqlite_event_store_is_legacy_class(self) -> None:
        from eventsource.stores.sqlite import SQLiteEventStore as LegacySQLiteEventStore

        assert eventsource.SQLiteEventStore is LegacySQLiteEventStore

    def test_top_level_postgresql_event_store_is_legacy_class(self) -> None:
        from eventsource.stores.postgresql import (
            PostgreSQLEventStore as LegacyPostgreSQLEventStore,
        )

        assert eventsource.PostgreSQLEventStore is LegacyPostgreSQLEventStore

    def test_adapter_sqlite_event_store_is_a_distinct_class(self) -> None:
        from eventsource.adapters.sqlite import SQLiteEventStore as AdapterSQLiteEventStore

        assert AdapterSQLiteEventStore is not eventsource.SQLiteEventStore

    def test_adapter_postgresql_event_store_is_a_distinct_class(self) -> None:
        from eventsource.adapters.postgresql import (
            PostgreSQLEventStore as AdapterPostgreSQLEventStore,
        )

        assert AdapterPostgreSQLEventStore is not eventsource.PostgreSQLEventStore
