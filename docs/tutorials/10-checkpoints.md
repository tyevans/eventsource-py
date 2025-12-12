# Tutorial 10: Checkpoints - Tracking Projection Progress

**Checkpoints** track which events a projection has processed, enabling resumption after restart, parallel processing, and lag monitoring.

## Checkpoint Store

```python
from eventsource import InMemoryCheckpointStore

# Create checkpoint store
checkpoint_store = InMemoryCheckpointStore()

# Save checkpoint
await checkpoint_store.save("projection-1", position=150)

# Load checkpoint
position = await checkpoint_store.load("projection-1")
print(f"Resume from: {position}")  # 150
```

## Projection with Checkpoints

```python
from eventsource import DeclarativeProjection, handles

class CheckpointedProjection(DeclarativeProjection):
    def __init__(self, checkpoint_store, projection_id):
        super().__init__()
        self.checkpoint_store = checkpoint_store
        self.projection_id = projection_id
        self.tasks = {}
        self.last_position = 0

    @handles(TaskCreated)
    async def _on_created(self, event: TaskCreated) -> None:
        self.tasks[event.aggregate_id] = event.title
        # Update checkpoint after processing
        await self._save_checkpoint(event.position)

    async def _save_checkpoint(self, position: int):
        self.last_position = position
        await self.checkpoint_store.save(self.projection_id, position)

    async def resume(self, event_store):
        """Resume from last checkpoint."""
        # Load last position
        start_position = await self.checkpoint_store.load(self.projection_id) or 0

        # Read events from that position
        stream = await event_store.read_all(from_position=start_position)

        # Process remaining events
        for stored_event in stream.events:
            await self.handle(stored_event.event)
```

## Complete Example

```python
"""
Tutorial 10: Checkpoints
Run with: python tutorial_10_checkpoints.py
"""
import asyncio
from uuid import uuid4
from eventsource import (
    InMemoryEventStore,
    InMemoryCheckpointStore,
    DeclarativeProjection,
    DomainEvent,
    register_event,
    handles,
)


@register_event
class TaskCreated(DomainEvent):
    event_type: str = "TaskCreated"
    aggregate_type: str = "Task"
    title: str


class CheckpointedProjection(DeclarativeProjection):
    def __init__(self, checkpoint_store, projection_id="main"):
        super().__init__()
        self.checkpoint_store = checkpoint_store
        self.projection_id = projection_id
        self.tasks = {}

    @handles(TaskCreated)
    async def _on_created(self, event: TaskCreated) -> None:
        self.tasks[event.aggregate_id] = event.title
        if hasattr(event, 'position'):
            await self.checkpoint_store.save(self.projection_id, event.position)

    async def _truncate_read_models(self) -> None:
        self.tasks.clear()


async def main():
    store = InMemoryEventStore()
    checkpoint_store = InMemoryCheckpointStore()
    projection = CheckpointedProjection(checkpoint_store)

    # Create events
    for i in range(5):
        event = TaskCreated(
            aggregate_id=uuid4(),
            title=f"Task {i+1}",
            aggregate_version=1,
        )
        await store.append(event.aggregate_id, [event], expected_version=0)
        await projection.handle(event)

    # Check checkpoint
    position = await checkpoint_store.load("main")
    print(f"Checkpoint: {position}")
    print(f"Tasks processed: {len(projection.tasks)}")


if __name__ == "__main__":
    asyncio.run(main())
```

## Next Steps

Continue to [Tutorial 11: PostgreSQL](11-postgresql.md).
