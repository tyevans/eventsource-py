"""Tests for bench domain events, payload generators, and the bench aggregate."""

from uuid import uuid4

from bench.core.domain import (
    PAYLOAD_SIZES,
    SNAPSHOT_SIZES,
    BenchCounter,
    BenchEvent,
    make_events,
    make_registry,
    make_snapshot_state,
)


def test_make_events_versions_and_payload_size() -> None:
    aggregate_id = uuid4()
    events = make_events(aggregate_id, count=3, start_version=5, payload="large")
    assert [e.aggregate_version for e in events] == [5, 6, 7]
    assert all(e.aggregate_id == aggregate_id for e in events)
    assert all(len(e.payload) == PAYLOAD_SIZES["large"] for e in events)
    assert [e.seq for e in events] == [0, 1, 2]


def test_make_snapshot_state_size() -> None:
    state = make_snapshot_state(SNAPSHOT_SIZES["medium"])
    assert len(state["blob"]) == SNAPSHOT_SIZES["medium"]


def test_bench_counter_applies_increments() -> None:
    counter = BenchCounter(uuid4())
    counter.increment()
    counter.increment(2)
    assert counter.state is not None
    assert counter.state.value == 3
    assert counter.version == 2  # local version tracks applied events, not persistence
    assert len(counter.uncommitted_events) == 2


def test_make_registry_contains_bench_events() -> None:
    registry = make_registry()
    assert registry.get("BenchEvent") is BenchEvent
