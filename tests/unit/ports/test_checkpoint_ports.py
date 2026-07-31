"""Tests for the checkpoint and position ports."""

from datetime import UTC, datetime
from uuid import UUID, uuid4

from eventsource.ports.checkpoints import (
    CheckpointData,
    CheckpointRepository,
    LagMetrics,
    ProjectionCheckpoints,
    SubscriptionPositions,
)


class TestCheckpointData:
    def test_defaults(self) -> None:
        data = CheckpointData(projection_name="P")
        assert data.last_event_id is None
        assert data.last_event_type is None
        assert data.last_processed_at is None
        assert data.events_processed == 0
        assert data.global_position is None

    def test_is_frozen(self) -> None:
        import dataclasses

        import pytest

        data = CheckpointData(projection_name="P")
        with pytest.raises(dataclasses.FrozenInstanceError):
            data.projection_name = "Q"  # type: ignore[misc]

    def test_holds_values(self) -> None:
        eid = uuid4()
        now = datetime.now(UTC)
        data = CheckpointData(
            projection_name="P",
            last_event_id=eid,
            last_event_type="Created",
            last_processed_at=now,
            events_processed=3,
            global_position=17,
        )
        assert (data.last_event_id, data.events_processed, data.global_position) == (eid, 3, 17)


class TestLagMetrics:
    def test_defaults(self) -> None:
        m = LagMetrics(projection_name="P")
        assert m.last_event_id is None
        assert m.latest_event_id is None
        assert m.lag_seconds == 0.0
        assert m.events_processed == 0
        assert m.last_processed_at is None


class PositionsOnly:
    async def get_position(self, subscription_id: str) -> int | None:
        return None

    async def save_position(
        self, subscription_id: str, position: int, event_id: UUID, event_type: str
    ) -> None:
        return None


class CheckpointsOnly:
    async def get_checkpoint(self, projection_name: str) -> UUID | None:
        return None

    async def update_checkpoint(
        self, projection_name: str, event_id: UUID, event_type: str
    ) -> None:
        return None

    async def reset_checkpoint(self, projection_name: str) -> None:
        return None

    async def get_lag_metrics(
        self, projection_name: str, event_types: list[str] | None = None
    ) -> LagMetrics | None:
        return None

    async def get_all_checkpoints(self) -> list[CheckpointData]:
        return []


class Both(CheckpointsOnly, PositionsOnly):
    pass


class TestProtocolSplit:
    def test_positions_only_satisfies_positions_port(self) -> None:
        assert isinstance(PositionsOnly(), SubscriptionPositions)

    def test_positions_only_does_not_satisfy_checkpoints_port(self) -> None:
        assert not isinstance(PositionsOnly(), ProjectionCheckpoints)

    def test_checkpoints_only_satisfies_checkpoints_port(self) -> None:
        assert isinstance(CheckpointsOnly(), ProjectionCheckpoints)

    def test_checkpoints_only_does_not_satisfy_composed_port(self) -> None:
        assert not isinstance(CheckpointsOnly(), CheckpointRepository)

    def test_both_satisfies_all_three(self) -> None:
        both = Both()
        assert isinstance(both, ProjectionCheckpoints)
        assert isinstance(both, SubscriptionPositions)
        assert isinstance(both, CheckpointRepository)
