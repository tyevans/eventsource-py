"""Property-based tests for InMemoryCheckpointRepository."""

from uuid import UUID, uuid4

from hypothesis import given
from hypothesis import strategies as st

from eventsource.adapters.memory import InMemoryCheckpointRepository
from eventsource.ports.positions import Position

uuids = st.builds(uuid4)


@given(event_ids=st.lists(uuids, min_size=1, max_size=20))
async def test_get_checkpoint_is_the_last_event_written(event_ids: list[UUID]) -> None:
    repo = InMemoryCheckpointRepository()
    for event_id in event_ids:
        await repo.update_checkpoint("P", event_id, "Created")

    assert await repo.get_checkpoint("P") == event_ids[-1]


@given(event_ids=st.lists(uuids, min_size=1, max_size=20))
async def test_events_processed_equals_the_update_count(event_ids: list[UUID]) -> None:
    repo = InMemoryCheckpointRepository()
    for event_id in event_ids:
        await repo.update_checkpoint("P", event_id, "Created")

    (checkpoint,) = await repo.get_all_checkpoints()
    assert checkpoint.events_processed == len(event_ids)


@given(positions=st.lists(st.integers(min_value=0, max_value=10**9), min_size=1, max_size=20))
async def test_get_position_is_the_last_position_written(positions: list[int]) -> None:
    repo = InMemoryCheckpointRepository()
    tokens = [Position(store_id="test", key=(n,)) for n in positions]
    for token in tokens:
        await repo.save_position("S", token, uuid4(), "Created")

    assert await repo.get_position("S") == tokens[-1]


@given(event_ids=st.lists(uuids, max_size=20))
async def test_reset_returns_the_projection_to_the_empty_state(event_ids: list[UUID]) -> None:
    repo = InMemoryCheckpointRepository()
    for event_id in event_ids:
        await repo.update_checkpoint("P", event_id, "Created")

    await repo.reset_checkpoint("P")

    assert await repo.get_checkpoint("P") is None
    assert await repo.get_lag_metrics("P") is None
    assert await repo.get_all_checkpoints() == []


@given(
    a_events=st.lists(uuids, min_size=1, max_size=10),
    b_events=st.lists(uuids, min_size=1, max_size=10),
)
async def test_distinct_projection_names_never_interfere(
    a_events: list[UUID], b_events: list[UUID]
) -> None:
    repo = InMemoryCheckpointRepository()
    for event_id in a_events:
        await repo.update_checkpoint("A", event_id, "Created")
    for event_id in b_events:
        await repo.update_checkpoint("B", event_id, "Created")

    assert await repo.get_checkpoint("A") == a_events[-1]
    assert await repo.get_checkpoint("B") == b_events[-1]
