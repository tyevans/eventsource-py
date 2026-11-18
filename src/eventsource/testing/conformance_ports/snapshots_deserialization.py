"""Conformance case for the `SnapshotDeserializationError` contract on corrupted snapshot state."""

from abc import ABC, abstractmethod
from uuid import UUID, uuid4

import pytest

from eventsource.domain.exceptions import SnapshotDeserializationError
from eventsource.ports.snapshots import SnapshotStore


class SnapshotDeserializationConformance(ABC):
    """Conformance suite for the `SnapshotDeserializationError` contract on corrupted raw state."""

    @abstractmethod
    @pytest.fixture
    def store(self) -> object:
        """Yield a fresh adapter instance implementing `SnapshotStore`."""
        raise NotImplementedError

    @abstractmethod
    async def _write_raw_state(
        self, store: SnapshotStore, *, aggregate_id: UUID, aggregate_type: str, raw_state: str
    ) -> None:
        """Insert a snapshot row with literal `raw_state` bypassing JSON encoding."""
        raise NotImplementedError

    async def test_get_snapshot_raises_deserialization_error_on_malformed_state(
        self, store: SnapshotStore
    ) -> None:
        aggregate_id = uuid4()
        await self._write_raw_state(
            store, aggregate_id=aggregate_id, aggregate_type="Order", raw_state="{not valid json"
        )
        with pytest.raises(SnapshotDeserializationError) as exc_info:
            await store.get_snapshot(aggregate_id, "Order")
        assert exc_info.value.aggregate_id == aggregate_id
        assert exc_info.value.aggregate_type == "Order"
