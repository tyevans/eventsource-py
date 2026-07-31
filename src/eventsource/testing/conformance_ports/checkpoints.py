"""Conformance suites for the checkpoint and position ports.

Subclass and provide a `store` fixture yielding a fresh adapter instance.
`ProjectionCheckpointsConformance` and `SubscriptionPositionsConformance`
mirror the ISP split; `CheckpointRepositoryConformance` composes both for
adapters that back one table with both capabilities.
"""

from abc import ABC, abstractmethod
from uuid import uuid4

import pytest

from eventsource.ports.checkpoints import ProjectionCheckpoints, SubscriptionPositions


class ProjectionCheckpointsConformance(ABC):
    """Conformance suite for `ProjectionCheckpoints` implementations."""

    @abstractmethod
    @pytest.fixture
    def store(self) -> object:
        """Yield a fresh adapter instance implementing `ProjectionCheckpoints`."""
        raise NotImplementedError

    async def test_absent_checkpoint_reads_none(self, store: ProjectionCheckpoints) -> None:
        assert await store.get_checkpoint("Missing") is None

    async def test_update_then_read_round_trips(self, store: ProjectionCheckpoints) -> None:
        event_id = uuid4()
        await store.update_checkpoint("P", event_id, "Created")
        assert await store.get_checkpoint("P") == event_id

    async def test_events_processed_increments_across_updates(
        self, store: ProjectionCheckpoints
    ) -> None:
        await store.update_checkpoint("P", uuid4(), "Created")
        await store.update_checkpoint("P", uuid4(), "Updated")
        await store.update_checkpoint("P", uuid4(), "Updated")

        (checkpoint,) = [c for c in await store.get_all_checkpoints() if c.projection_name == "P"]
        assert checkpoint.events_processed == 3

    async def test_last_write_wins(self, store: ProjectionCheckpoints) -> None:
        await store.update_checkpoint("P", uuid4(), "Created")
        last = uuid4()
        await store.update_checkpoint("P", last, "Updated")
        assert await store.get_checkpoint("P") == last

    async def test_reset_makes_checkpoint_absent(self, store: ProjectionCheckpoints) -> None:
        await store.update_checkpoint("P", uuid4(), "Created")
        await store.reset_checkpoint("P")
        assert await store.get_checkpoint("P") is None

    async def test_reset_of_absent_checkpoint_is_a_no_op(
        self, store: ProjectionCheckpoints
    ) -> None:
        await store.reset_checkpoint("NeverExisted")
        assert await store.get_checkpoint("NeverExisted") is None

    async def test_distinct_projections_do_not_interfere(
        self, store: ProjectionCheckpoints
    ) -> None:
        a, b = uuid4(), uuid4()
        await store.update_checkpoint("A", a, "Created")
        await store.update_checkpoint("B", b, "Created")
        await store.reset_checkpoint("A")

        assert await store.get_checkpoint("A") is None
        assert await store.get_checkpoint("B") == b

    async def test_get_all_checkpoints_returns_every_projection_sorted_by_name(
        self, store: ProjectionCheckpoints
    ) -> None:
        for name in ("Charlie", "Alpha", "Bravo"):
            await store.update_checkpoint(name, uuid4(), "Created")

        names = [c.projection_name for c in await store.get_all_checkpoints()]
        assert names == ["Alpha", "Bravo", "Charlie"]

    async def test_get_lag_metrics_is_none_without_a_checkpoint(
        self, store: ProjectionCheckpoints
    ) -> None:
        assert await store.get_lag_metrics("Missing") is None

    async def test_get_lag_metrics_is_not_none_with_a_checkpoint(
        self, store: ProjectionCheckpoints
    ) -> None:
        event_id = uuid4()
        await store.update_checkpoint("P", event_id, "Created")

        metrics = await store.get_lag_metrics("P")

        assert metrics is not None
        assert metrics.projection_name == "P"
        assert metrics.last_event_id == str(event_id)
        assert metrics.events_processed == 1


class SubscriptionPositionsConformance(ABC):
    """Conformance suite for `SubscriptionPositions` implementations."""

    @abstractmethod
    @pytest.fixture
    def store(self) -> object:
        """Yield a fresh adapter instance implementing `SubscriptionPositions`."""
        raise NotImplementedError

    async def test_absent_position_reads_none(self, store: SubscriptionPositions) -> None:
        assert await store.get_position("Missing") is None

    async def test_save_then_get_round_trips(self, store: SubscriptionPositions) -> None:
        await store.save_position("S", 42, uuid4(), "Created")
        assert await store.get_position("S") == 42

    async def test_last_saved_position_wins(self, store: SubscriptionPositions) -> None:
        await store.save_position("S", 1, uuid4(), "Created")
        await store.save_position("S", 99, uuid4(), "Updated")
        assert await store.get_position("S") == 99

    async def test_distinct_subscriptions_do_not_interfere(
        self, store: SubscriptionPositions
    ) -> None:
        await store.save_position("A", 1, uuid4(), "Created")
        await store.save_position("B", 2, uuid4(), "Created")
        assert await store.get_position("A") == 1
        assert await store.get_position("B") == 2


class CheckpointRepositoryConformance(
    ProjectionCheckpointsConformance, SubscriptionPositionsConformance
):
    """Both capabilities, one table."""

    async def test_position_is_none_before_any_position_is_saved(self, store: object) -> None:
        # A checkpoint exists but carries no global position: the two
        # capabilities share a row, and a checkpoint-only write must not
        # invent one.
        await store.update_checkpoint("P", uuid4(), "Created")  # type: ignore[attr-defined]
        assert await store.get_position("P") is None  # type: ignore[attr-defined]

    async def test_save_position_also_advances_the_checkpoint(self, store: object) -> None:
        event_id = uuid4()
        await store.save_position("P", 7, event_id, "Created")  # type: ignore[attr-defined]
        assert await store.get_checkpoint("P") == event_id  # type: ignore[attr-defined]
        assert await store.get_position("P") == 7  # type: ignore[attr-defined]
