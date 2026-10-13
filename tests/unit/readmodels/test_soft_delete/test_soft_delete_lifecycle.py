"""Lifecycle unit tests for soft delete functionality."""

from uuid import uuid4

import pytest

from eventsource.adapters.memory.readmodels import InMemoryReadModelRepository
from tests.unit.readmodels.test_soft_delete._fixtures import TestModel


class TestSoftDelete:
    """Tests for soft_delete() method."""

    @pytest.mark.asyncio
    async def test_soft_delete_sets_timestamp(
        self, repo: InMemoryReadModelRepository[TestModel]
    ) -> None:
        """Test that soft_delete sets deleted_at timestamp."""
        model_id = uuid4()
        model = TestModel(id=model_id, name="test")
        await repo.save(model)

        result = await repo.soft_delete(model_id)

        assert result is True
        # Model hidden from get but exists in storage
        assert await repo.get(model_id) is None
        assert len(repo) == 1

    @pytest.mark.asyncio
    async def test_soft_delete_nonexistent_returns_false(
        self, repo: InMemoryReadModelRepository[TestModel]
    ) -> None:
        """Test soft_delete returns False for non-existent ID."""
        result = await repo.soft_delete(uuid4())
        assert result is False

    @pytest.mark.asyncio
    async def test_soft_delete_already_deleted_returns_false(
        self, repo: InMemoryReadModelRepository[TestModel]
    ) -> None:
        """Test soft_delete returns False if already soft-deleted."""
        model_id = uuid4()
        model = TestModel(id=model_id, name="test")
        await repo.save(model)

        await repo.soft_delete(model_id)
        result = await repo.soft_delete(model_id)

        assert result is False

    @pytest.mark.asyncio
    async def test_soft_delete_updates_updated_at(
        self, repo: InMemoryReadModelRepository[TestModel]
    ) -> None:
        """Test that soft_delete updates the updated_at timestamp."""
        model_id = uuid4()
        model = TestModel(id=model_id, name="test")
        await repo.save(model)

        original_updated_at = model.updated_at
        await repo.soft_delete(model_id)

        # Access the model directly from storage
        deleted_model = await repo.get_deleted(model_id)
        assert deleted_model is not None
        assert deleted_model.updated_at >= original_updated_at


class TestRestore:
    """Tests for restore() method."""

    @pytest.mark.asyncio
    async def test_restore_clears_timestamp(
        self, repo: InMemoryReadModelRepository[TestModel]
    ) -> None:
        """Test that restore clears deleted_at timestamp."""
        model_id = uuid4()
        model = TestModel(id=model_id, name="test")
        await repo.save(model)
        await repo.soft_delete(model_id)

        result = await repo.restore(model_id)

        assert result is True
        restored = await repo.get(model_id)
        assert restored is not None
        assert restored.deleted_at is None

    @pytest.mark.asyncio
    async def test_restore_nonexistent_returns_false(
        self, repo: InMemoryReadModelRepository[TestModel]
    ) -> None:
        """Test restore returns False for non-existent ID."""
        result = await repo.restore(uuid4())
        assert result is False

    @pytest.mark.asyncio
    async def test_restore_not_deleted_returns_false(
        self, repo: InMemoryReadModelRepository[TestModel]
    ) -> None:
        """Test restore returns False if not soft-deleted."""
        model_id = uuid4()
        model = TestModel(id=model_id, name="test")
        await repo.save(model)

        result = await repo.restore(model_id)

        assert result is False

    @pytest.mark.asyncio
    async def test_restore_updates_updated_at(
        self, repo: InMemoryReadModelRepository[TestModel]
    ) -> None:
        """Test that restore updates the updated_at timestamp."""
        model_id = uuid4()
        model = TestModel(id=model_id, name="test")
        await repo.save(model)
        await repo.soft_delete(model_id)

        deleted = await repo.get_deleted(model_id)
        assert deleted is not None
        deleted_updated_at = deleted.updated_at

        await repo.restore(model_id)
        restored = await repo.get(model_id)
        assert restored is not None
        assert restored.updated_at >= deleted_updated_at


class TestSoftDeleteCycle:
    """Tests for full soft-delete/restore/delete cycle."""

    @pytest.mark.asyncio
    async def test_delete_restore_cycle(self, repo: InMemoryReadModelRepository[TestModel]) -> None:
        """Test complete cycle: create -> soft_delete -> restore -> get."""
        model_id = uuid4()
        model = TestModel(id=model_id, name="test")

        # Create
        await repo.save(model)
        assert await repo.get(model_id) is not None
        assert await repo.get_deleted(model_id) is None

        # Soft delete
        await repo.soft_delete(model_id)
        assert await repo.get(model_id) is None
        assert await repo.get_deleted(model_id) is not None

        # Restore
        await repo.restore(model_id)
        assert await repo.get(model_id) is not None
        assert await repo.get_deleted(model_id) is None

    @pytest.mark.asyncio
    async def test_hard_delete_after_soft_delete(
        self, repo: InMemoryReadModelRepository[TestModel]
    ) -> None:
        """Test hard delete of a soft-deleted record."""
        model_id = uuid4()
        model = TestModel(id=model_id, name="test")
        await repo.save(model)
        await repo.soft_delete(model_id)

        # Hard delete
        result = await repo.delete(model_id)

        assert result is True
        assert len(repo) == 0
        assert await repo.get_deleted(model_id) is None
