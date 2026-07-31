"""Property-based tests for InMemoryDLQRepository."""

from uuid import UUID, uuid4

from hypothesis import given
from hypothesis import strategies as st

from eventsource.adapters.memory import InMemoryDLQRepository

adds = st.lists(
    st.tuples(st.builds(uuid4), st.sampled_from(["A", "B", "C"])),
    min_size=1,
    max_size=15,
)


@given(pairs=adds)
async def test_entry_count_equals_distinct_event_projection_pairs(
    pairs: list[tuple[UUID, str]],
) -> None:
    repo = InMemoryDLQRepository()
    for event_id, projection in pairs:
        await repo.add_failed_event(
            event_id=event_id,
            projection_name=projection,
            event_type="Created",
            event_data={},
            error=RuntimeError("boom"),
        )

    entries = await repo.get_failed_events(limit=1000)
    assert len(entries) == len(set(pairs))


@given(pairs=adds, limit=st.integers(min_value=1, max_value=20))
async def test_limit_caps_results_and_order_is_non_increasing_by_first_failed_at(
    pairs: list[tuple[UUID, str]], limit: int
) -> None:
    repo = InMemoryDLQRepository()
    for event_id, projection in pairs:
        await repo.add_failed_event(
            event_id=event_id,
            projection_name=projection,
            event_type="Created",
            event_data={},
            error=RuntimeError("boom"),
        )

    entries = await repo.get_failed_events(limit=limit)

    assert len(entries) == min(limit, len(set(pairs)))
    timestamps = [e.first_failed_at for e in entries]
    assert timestamps == sorted(timestamps, reverse=True)


@given(pairs=adds)
async def test_clear_empties_entries_and_resets_the_id_counter(
    pairs: list[tuple[UUID, str]],
) -> None:
    repo = InMemoryDLQRepository()
    for event_id, projection in pairs:
        await repo.add_failed_event(
            event_id=event_id,
            projection_name=projection,
            event_type="Created",
            event_data={},
            error=RuntimeError("boom"),
        )

    await repo.clear()

    assert await repo.get_failed_events(limit=1000) == []
    await repo.add_failed_event(
        event_id=uuid4(),
        projection_name="A",
        event_type="Created",
        event_data={},
        error=RuntimeError("boom"),
    )
    (entry,) = await repo.get_failed_events(limit=1000)
    assert entry.id == 1
