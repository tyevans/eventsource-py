"""Conformance suites for the checkpoint and position ports."""

from abc import ABC, abstractmethod
from uuid import uuid4

import pytest

from eventsource.ports.checkpoints import ProjectionCheckpoints, SubscriptionPositions
from eventsource.ports.exceptions import PositionForeignError
from eventsource.ports.positions import Position


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
        await store.update_checkpoint("P", event_id := uuid4(), "Created")
        assert await store.get_checkpoint("P") == event_id

    async def test_events_processed_increments_across_updates(self, store: ProjectionCheckpoints) -> None:  # fmt: skip
        for ev_type in ("Created", "Updated", "Updated"):
            await store.update_checkpoint("P", uuid4(), ev_type)
        (checkpoint,) = (c for c in await store.get_all_checkpoints() if c.projection_name == "P")
        assert checkpoint.events_processed == 3

    async def test_last_write_wins(self, store: ProjectionCheckpoints) -> None:
        await store.update_checkpoint("P", uuid4(), "Created")
        await store.update_checkpoint("P", last := uuid4(), "Updated")
        assert await store.get_checkpoint("P") == last

    async def test_reset_makes_checkpoint_absent(self, store: ProjectionCheckpoints) -> None:
        await store.update_checkpoint("P", uuid4(), "Created")
        await store.reset_checkpoint("P")
        assert await store.get_checkpoint("P") is None

    async def test_reset_of_absent_checkpoint_is_a_no_op(self, store: ProjectionCheckpoints) -> None:  # fmt: skip
        await store.reset_checkpoint("NeverExisted")
        assert await store.get_checkpoint("NeverExisted") is None

    async def test_distinct_projections_do_not_interfere(self, store: ProjectionCheckpoints) -> None:  # fmt: skip
        a, b = uuid4(), uuid4()
        await store.update_checkpoint("A", a, "Created")
        await store.update_checkpoint("B", b, "Created")
        await store.reset_checkpoint("A")
        assert await store.get_checkpoint("A") is None and await store.get_checkpoint("B") == b

    async def test_get_all_checkpoints_returns_every_projection_sorted_by_name(self, store: ProjectionCheckpoints) -> None:  # fmt: skip
        for name in ("Charlie", "Alpha", "Bravo"):
            await store.update_checkpoint(name, uuid4(), "Created")
        assert [c.projection_name for c in await store.get_all_checkpoints()] == ["Alpha", "Bravo", "Charlie"]  # fmt: skip

    async def test_get_lag_metrics_is_none_without_a_checkpoint(self, store: ProjectionCheckpoints) -> None:  # fmt: skip
        assert await store.get_lag_metrics("Missing") is None

    async def test_get_lag_metrics_is_not_none_with_a_checkpoint(self, store: ProjectionCheckpoints) -> None:  # fmt: skip
        await store.update_checkpoint("P", event_id := uuid4(), "Created")
        assert (metrics := await store.get_lag_metrics("P")) is not None
        assert (metrics.projection_name, metrics.last_event_id, metrics.events_processed) == ("P", str(event_id), 1)  # fmt: skip


class SubscriptionPositionsConformance(ABC):
    """Conformance suite for `SubscriptionPositions` implementations."""

    @abstractmethod
    @pytest.fixture
    def store(self) -> object:
        """Yield a fresh adapter instance implementing `SubscriptionPositions`."""
        raise NotImplementedError

    def make_position(self, n: int) -> Position:
        """A comparable, persistable position for this adapter's store."""
        return Position(store_id="conformance", key=(n,))

    async def test_absent_position_reads_none(self, store: SubscriptionPositions) -> None:
        assert await store.get_position("Missing") is None

    async def test_save_then_get_round_trips(self, store: SubscriptionPositions) -> None:
        await store.save_position("S", position := self.make_position(42), uuid4(), "Created")
        assert await store.get_position("S") == position

    async def test_last_saved_position_wins(self, store: SubscriptionPositions) -> None:
        await store.save_position("S", self.make_position(1), uuid4(), "Created")
        await store.save_position("S", latest := self.make_position(99), uuid4(), "Updated")
        assert await store.get_position("S") == latest

    async def test_distinct_subscriptions_do_not_interfere(self, store: SubscriptionPositions) -> None:  # fmt: skip
        a, b = self.make_position(1), self.make_position(2)
        await store.save_position("A", a, uuid4(), "Created")
        await store.save_position("B", b, uuid4(), "Created")
        assert await store.get_position("A") == a and await store.get_position("B") == b

    async def test_multi_element_token_round_trips_verbatim(self, store: SubscriptionPositions) -> None:  # fmt: skip
        position = Position(store_id="s", key=(7, "abc"))
        await store.save_position("S", position, uuid4(), "Created")
        assert await store.get_position("S") == position

    async def test_tokens_from_another_store_are_returned_but_not_comparable(self, store: SubscriptionPositions) -> None:  # fmt: skip
        foreign = Position(store_id="A", key=(1,))
        await store.save_position("S", foreign, uuid4(), "Created")
        assert (stored := await store.get_position("S")) == foreign
        with pytest.raises(PositionForeignError):
            _ = stored < Position(store_id="B", key=(2,))


class CheckpointRepositoryConformance(
    ProjectionCheckpointsConformance, SubscriptionPositionsConformance
):
    """Both capabilities, one table."""

    async def test_position_is_none_before_any_position_is_saved(self, store: object) -> None:
        await store.update_checkpoint("P", uuid4(), "Created")  # type: ignore[attr-defined]
        assert await store.get_position("P") is None  # type: ignore[attr-defined]

    async def test_save_position_also_advances_the_checkpoint(self, store: object) -> None:
        event_id, position = uuid4(), self.make_position(7)
        await store.save_position("P", position, event_id, "Created")  # type: ignore[attr-defined]
        assert await store.get_checkpoint("P") == event_id  # type: ignore[attr-defined]
        assert await store.get_position("P") == position  # type: ignore[attr-defined]

    async def test_update_checkpoint_preserves_a_previously_saved_position(self, store: object) -> None:  # fmt: skip
        position = self.make_position(42)
        await store.save_position("P", position, uuid4(), "Created")  # type: ignore[attr-defined]
        await store.update_checkpoint("P", uuid4(), "Updated")  # type: ignore[attr-defined]
        assert await store.get_position("P") == position  # type: ignore[attr-defined]
