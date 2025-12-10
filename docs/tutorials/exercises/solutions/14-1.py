"""
Tutorial 14 - Exercise 1 Solution: Add Snapshotting to Aggregate

This solution demonstrates adding snapshotting to an existing aggregate
and measuring the performance improvement.
"""

import asyncio
import time
from uuid import uuid4

from pydantic import BaseModel

from eventsource import (
    AggregateRepository,
    AggregateRoot,
    DomainEvent,
    InMemoryEventStore,
    register_event,
)
from eventsource.snapshots import InMemorySnapshotStore


@register_event
class CounterIncremented(DomainEvent):
    event_type: str = "CounterIncremented"
    aggregate_type: str = "Counter"


class CounterState(BaseModel):
    counter_id: str
    value: int = 0


class CounterAggregate(AggregateRoot[CounterState]):
    aggregate_type = "Counter"
    schema_version = 1  # Added for snapshot compatibility

    def _get_initial_state(self) -> CounterState:
        return CounterState(counter_id=str(self.aggregate_id))

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, CounterIncremented):
            if self._state is None:
                self._state = CounterState(counter_id=str(self.aggregate_id), value=1)
            else:
                self._state = self._state.model_copy(update={"value": self._state.value + 1})

    def increment(self) -> None:
        self.apply_event(
            CounterIncremented(
                aggregate_id=self.aggregate_id,
                aggregate_version=self.get_next_version(),
            )
        )


async def main():
    print("=== Exercise 14-1: Add Snapshotting ===\n")

    event_store = InMemoryEventStore()
    snapshot_store = InMemorySnapshotStore()

    # Step 1: Create aggregate with 500 events
    print("Step 1: Creating aggregate with 500 events...")
    counter_id = uuid4()

    repo_no_snap = AggregateRepository(
        event_store=event_store,
        aggregate_factory=CounterAggregate,
        aggregate_type="Counter",
    )

    counter = repo_no_snap.create_new(counter_id)
    for _ in range(500):
        counter.increment()
    await repo_no_snap.save(counter)
    print(f"   Counter value: {counter.state.value}")
    print(f"   Counter version: {counter.version}")

    # Step 2: Measure load time without snapshots
    print("\nStep 2: Measuring load time without snapshots...")
    iterations = 20
    start = time.perf_counter()
    for _ in range(iterations):
        await repo_no_snap.load(counter_id)
    time_no_snap = (time.perf_counter() - start) / iterations
    print(f"   Average load time: {time_no_snap * 1000:.2f}ms")

    # Step 3: Add snapshot support to repository
    print("\nStep 3: Adding snapshot support...")
    repo_with_snap = AggregateRepository(
        event_store=event_store,
        aggregate_factory=CounterAggregate,
        aggregate_type="Counter",
        snapshot_store=snapshot_store,
        snapshot_threshold=100,
        snapshot_mode="sync",
    )
    print("   Repository configured with snapshots!")
    print(f"   Threshold: {repo_with_snap.snapshot_threshold}")
    print(f"   Mode: {repo_with_snap.snapshot_mode}")

    # Step 4: Create snapshot
    print("\nStep 4: Creating snapshot...")
    counter = await repo_with_snap.load(counter_id)
    snapshot = await repo_with_snap.create_snapshot(counter)
    print(f"   Snapshot created at version {snapshot.version}")
    print(f"   Schema version: {snapshot.schema_version}")

    # Step 5: Measure load time with snapshot
    print("\nStep 5: Measuring load time with snapshot...")
    start = time.perf_counter()
    for _ in range(iterations):
        await repo_with_snap.load(counter_id)
    time_with_snap = (time.perf_counter() - start) / iterations
    print(f"   Average load time: {time_with_snap * 1000:.2f}ms")

    # Step 6: Verify performance improvement
    print("\nStep 6: Performance comparison:")
    print(f"   Without snapshot: {time_no_snap * 1000:.2f}ms")
    print(f"   With snapshot:    {time_with_snap * 1000:.2f}ms")
    if time_with_snap > 0:
        improvement = time_no_snap / time_with_snap
        print(f"   Improvement:      {improvement:.1f}x faster")

    # Verify correctness
    loaded = await repo_with_snap.load(counter_id)
    print("\nVerification:")
    print(f"   Counter value: {loaded.state.value}")
    print(f"   Counter version: {loaded.version}")

    # Additional: Check snapshot exists in store
    stored_snapshot = await snapshot_store.get_snapshot(counter_id, "Counter")
    print("\nSnapshot verification:")
    print(f"   Snapshot exists: {stored_snapshot is not None}")
    if stored_snapshot:
        print(f"   State: {stored_snapshot.state}")

    print("\n=== Exercise Complete! ===")


if __name__ == "__main__":
    asyncio.run(main())
