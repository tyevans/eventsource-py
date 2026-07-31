"""Property-based tests for snapshotting collaborators.

Covers the pure predicates (EveryNEvents / Never) and the load-path schema
validation (read_valid_snapshot) with Hypothesis-generated inputs, so the
boundary conditions aren't limited to whatever examples a human thought of.
"""

from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import uuid4

import pytest
from hypothesis import given
from hypothesis import strategies as st

from eventsource.adapters.memory.snapshots import InMemorySnapshotStore
from eventsource.application.aggregates.snapshotting import (
    EveryNEvents,
    Never,
    read_valid_snapshot,
)
from eventsource.ports.snapshots import Snapshot


def fake_aggregate(version: int):
    """Policies only read .version — a stub keeps these tests pure."""
    return SimpleNamespace(version=version, aggregate_id=uuid4())


@given(
    version=st.integers(min_value=0, max_value=10_000),
    n=st.integers(min_value=1, max_value=500),
)
def test_every_n_events_matches_boundary_predicate(version: int, n: int) -> None:
    expected = version > 0 and version % n == 0
    assert EveryNEvents(n).should_snapshot(fake_aggregate(version), 1) is expected


@given(
    version=st.integers(min_value=0, max_value=10_000),
    since=st.integers(min_value=0, max_value=10_000),
)
def test_never_is_never(version: int, since: int) -> None:
    assert Never().should_snapshot(fake_aggregate(version), since) is False


@given(n=st.integers(max_value=0))
def test_every_n_events_rejects_nonpositive(n: int) -> None:
    with pytest.raises(ValueError, match="EveryNEvents requires n >= 1"):
        EveryNEvents(n)


class VersionedFactory:
    schema_version = 3


@given(stored_schema=st.integers(min_value=1, max_value=10))
async def test_read_valid_snapshot_iff_schema_matches(stored_schema: int) -> None:
    store = InMemorySnapshotStore()
    aid = uuid4()
    await store.save_snapshot(
        Snapshot(
            aggregate_id=aid,
            aggregate_type="Thing",
            version=7,
            state={"x": 1},
            schema_version=stored_schema,
            created_at=datetime.now(UTC),
        )
    )
    result = await read_valid_snapshot(store, aid, "Thing", VersionedFactory)  # type: ignore[arg-type]
    if stored_schema == VersionedFactory.schema_version:
        assert result is not None
        assert result.schema_version == stored_schema
    else:
        assert result is None
