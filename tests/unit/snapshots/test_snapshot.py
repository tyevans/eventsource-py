"""Unit tests for the Snapshot dataclass."""

from datetime import UTC, datetime
from uuid import uuid4

import pytest

from eventsource.snapshots import Snapshot


class TestSnapshot:
    """Tests for the Snapshot dataclass."""

    def test_create_snapshot(self):
        """Test basic snapshot creation."""
        aggregate_id = uuid4()
        now = datetime.now(UTC)

        snapshot = Snapshot(
            aggregate_id=aggregate_id,
            aggregate_type="Order",
            version=100,
            state={"status": "shipped"},
            schema_version=1,
            created_at=now,
        )

        assert snapshot.aggregate_id == aggregate_id
        assert snapshot.aggregate_type == "Order"
        assert snapshot.version == 100
        assert snapshot.state == {"status": "shipped"}
        assert snapshot.schema_version == 1
        assert snapshot.created_at == now

    def test_snapshot_is_frozen(self):
        """Test that snapshot is immutable."""
        snapshot = Snapshot(
            aggregate_id=uuid4(),
            aggregate_type="Order",
            version=1,
            state={},
            schema_version=1,
            created_at=datetime.now(UTC),
        )

        with pytest.raises(AttributeError):
            snapshot.version = 2  # type: ignore

    def test_snapshot_str(self):
        """Test string representation."""
        aggregate_id = uuid4()
        snapshot = Snapshot(
            aggregate_id=aggregate_id,
            aggregate_type="Order",
            version=100,
            state={},
            schema_version=2,
            created_at=datetime.now(UTC),
        )

        result = str(snapshot)
        assert "Order" in result
        assert str(aggregate_id) in result
        assert "v100" in result
        assert "schema_v2" in result

    def test_snapshot_repr(self):
        """Test repr for debugging."""
        snapshot = Snapshot(
            aggregate_id=uuid4(),
            aggregate_type="Order",
            version=1,
            state={"key": "value"},
            schema_version=1,
            created_at=datetime.now(UTC),
        )

        result = repr(snapshot)
        assert "aggregate_id=" in result
        assert "state_keys=" in result
        assert "['key']" in result

    def test_snapshot_equality(self):
        """Test snapshot equality based on all fields."""
        aggregate_id = uuid4()
        now = datetime.now(UTC)
        state = {"status": "active"}

        snapshot1 = Snapshot(
            aggregate_id=aggregate_id,
            aggregate_type="Order",
            version=1,
            state=state,
            schema_version=1,
            created_at=now,
        )

        snapshot2 = Snapshot(
            aggregate_id=aggregate_id,
            aggregate_type="Order",
            version=1,
            state=state,
            schema_version=1,
            created_at=now,
        )

        assert snapshot1 == snapshot2

    def test_snapshot_inequality_different_version(self):
        """Test that snapshots with different versions are not equal."""
        aggregate_id = uuid4()
        now = datetime.now(UTC)

        snapshot1 = Snapshot(
            aggregate_id=aggregate_id,
            aggregate_type="Order",
            version=1,
            state={},
            schema_version=1,
            created_at=now,
        )

        snapshot2 = Snapshot(
            aggregate_id=aggregate_id,
            aggregate_type="Order",
            version=2,
            state={},
            schema_version=1,
            created_at=now,
        )

        assert snapshot1 != snapshot2

    def test_snapshot_not_hashable_with_dict_state(self):
        """Test that snapshots are not hashable due to mutable dict state.

        Note: While frozen dataclasses are typically hashable, the presence
        of a mutable dict field (state) prevents hashing. This is expected
        Python behavior. Snapshots are keyed by (aggregate_id, aggregate_type)
        in snapshot stores, so hashability of the full object is not required.
        """
        snapshot = Snapshot(
            aggregate_id=uuid4(),
            aggregate_type="Order",
            version=1,
            state={},
            schema_version=1,
            created_at=datetime.now(UTC),
        )

        # Snapshots cannot be hashed because state is a mutable dict
        with pytest.raises(TypeError, match="unhashable"):
            hash(snapshot)

    def test_snapshot_with_complex_state(self):
        """Test snapshot with complex nested state."""
        aggregate_id = uuid4()
        complex_state = {
            "order_id": str(uuid4()),
            "status": "shipped",
            "items": [
                {"product_id": "prod1", "quantity": 2, "price": 19.99},
                {"product_id": "prod2", "quantity": 1, "price": 49.99},
            ],
            "shipping": {
                "address": {
                    "street": "123 Main St",
                    "city": "Anytown",
                    "country": "US",
                },
                "method": "express",
            },
            "metadata": {"source": "web", "campaign": None},
        }

        snapshot = Snapshot(
            aggregate_id=aggregate_id,
            aggregate_type="Order",
            version=50,
            state=complex_state,
            schema_version=3,
            created_at=datetime.now(UTC),
        )

        assert snapshot.state == complex_state
        assert snapshot.state["items"][0]["quantity"] == 2
        assert snapshot.state["shipping"]["address"]["city"] == "Anytown"

    def test_snapshot_with_empty_state(self):
        """Test snapshot with empty state dict."""
        snapshot = Snapshot(
            aggregate_id=uuid4(),
            aggregate_type="Order",
            version=0,
            state={},
            schema_version=1,
            created_at=datetime.now(UTC),
        )

        assert snapshot.state == {}
        assert "[]" in repr(snapshot)  # state_keys=[]
