"""The PostgreSQL feed's safe-horizon predicate is wraparound-safe.

`xmin` is a 32-bit xid whose textual value wraps at 2^32, while
`pg_snapshot_xmin(pg_current_snapshot())` returns an epoch-extended
64-bit `xid8`. Comparing them makes the predicate universally true once a
cluster crosses its first xid epoch -- fail-open, in exactly the
high-write-volume deployments that need the no-skip guard. The predicate
now filters on the `events.txid xid8` column against a horizon bound once
per read_all/current_position call. True epoch wraparound cannot be reproduced in a
testcontainer, so these query-shape assertions are the regression guard;
ADR 0027 records the reasoning.
"""

from __future__ import annotations

from pathlib import Path

from eventsource.adapters.postgresql import store as pg_store
from eventsource.migrations import get_all_schemas, get_schema


class TestHorizonPredicateShape:
    def test_predicate_filters_on_the_txid_column(self) -> None:
        assert "txid IS NULL OR txid <" in pg_store._HORIZON_PREDICATE

    def test_predicate_binds_the_horizon_as_a_parameter(self) -> None:
        assert ":txid_horizon" in pg_store._HORIZON_PREDICATE

    def test_no_xmin_cast_remains_in_the_adapter_module(self) -> None:
        source = Path(pg_store.__file__).read_text()
        assert "xmin" not in source


class TestTxidReachesComposedSchemas:
    def test_events_schema_carries_the_column(self) -> None:
        assert "txid" in get_schema("events")

    def test_partitioned_events_schema_carries_the_column(self) -> None:
        assert "txid" in get_schema("events_partitioned")

    def test_all_schema_carries_the_column(self) -> None:
        assert "txid" in get_all_schemas()

    def test_base_schema_alone_does_not_carry_the_column(self) -> None:
        assert "txid" not in get_schema("events", additive=False)

    def test_sqlite_schemas_do_not_carry_the_column(self) -> None:
        # SQLite is a single serialized writer with no horizon predicate.
        assert "txid" not in get_all_schemas(backend="sqlite")
