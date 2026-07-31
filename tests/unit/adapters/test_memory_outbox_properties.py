"""Property-based tests for InMemoryOutboxRepository."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

from hypothesis import given
from hypothesis import strategies as st

from eventsource.adapters.memory import InMemoryOutboxRepository
from eventsource.testing.conformance_ports._fixtures import make_event

errors = st.one_of(st.none(), st.text(max_size=20))
retry_sequences = st.lists(errors, min_size=1, max_size=15)

statuses = st.sampled_from(["pending", "published", "failed"])
entry_status_lists = st.lists(statuses, min_size=0, max_size=10)


@given(errs=retry_sequences)
async def test_retry_count_and_last_error_track_the_applied_sequence(
    errs: list[str | None],
) -> None:
    repo = InMemoryOutboxRepository()
    outbox_id = await repo.add_event(make_event(aggregate_id=uuid4()))

    for i, err in enumerate(errs):
        # Interleave increment_retry with mark_failed, then continue
        # incrementing the same entry -- mark_failed changes status, not
        # retry_count or last_error, so it must not perturb either.
        if i % 3 == 1:
            await repo.mark_failed(outbox_id, "interleaved failure")
        await repo.increment_retry(outbox_id, err)

    entry = repo._entries[outbox_id]
    assert entry.retry_count == len(errs)
    assert entry.last_error == errs[-1]


@given(statuses_=entry_status_lists, days=st.integers(min_value=0, max_value=30))
async def test_cleanup_published_partitions_by_status(statuses_: list[str], days: int) -> None:
    repo = InMemoryOutboxRepository()

    for status in statuses_:
        outbox_id = await repo.add_event(make_event(aggregate_id=uuid4()))
        if status == "published":
            await repo.mark_published(outbox_id)
            # Force every published entry safely past any cutoff so the
            # property doesn't depend on wall-clock timing.
            repo._entries[outbox_id].published_at = datetime.now(UTC) - timedelta(days=days + 1)
        elif status == "failed":
            await repo.mark_failed(outbox_id, "boom")

    expected_published = statuses_.count("published")
    expected_pending = statuses_.count("pending")
    expected_failed = statuses_.count("failed")

    deleted = await repo.cleanup_published(days=days)

    assert deleted == expected_published
    stats = await repo.get_stats()
    assert stats.pending_count == expected_pending
    assert stats.failed_count == expected_failed
