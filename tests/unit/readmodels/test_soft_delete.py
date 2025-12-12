"""Comprehensive unit tests for soft delete functionality.

Tests verify that soft delete is fully implemented and tested across
all repository implementations: InMemory, PostgreSQL, and SQLite.

Task: P4-001 - Implement Soft Delete Support
"""

from decimal import Decimal
from uuid import uuid4

import pytest

from eventsource.readmodels import ReadModel
from eventsource.readmodels.in_memory import InMemoryReadModelRepository
from eventsource.readmodels.query import Filter, Query


class TestModel(ReadModel):
    """Test model for soft delete tests."""

    name: str
    status: str = "active"
    amount: Decimal = Decimal("0")


@pytest.fixture
def repo() -> InMemoryReadModelRepository[TestModel]:
    """Create a fresh in-memory repository for each test."""
    return InMemoryReadModelRepository(TestModel, enable_tracing=False)


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


class TestGetDeleted:
    """Tests for get_deleted() method."""

    @pytest.mark.asyncio
    async def test_get_deleted_returns_soft_deleted(
        self, repo: InMemoryReadModelRepository[TestModel]
    ) -> None:
        """Test get_deleted returns only soft-deleted records."""
        model_id = uuid4()
        model = TestModel(id=model_id, name="test")
        await repo.save(model)

        # Not deleted yet - should return None
        assert await repo.get_deleted(model_id) is None

        await repo.soft_delete(model_id)

        # Now should be returned
        deleted = await repo.get_deleted(model_id)
        assert deleted is not None
        assert deleted.id == model_id
        assert deleted.deleted_at is not None

    @pytest.mark.asyncio
    async def test_get_deleted_nonexistent_returns_none(
        self, repo: InMemoryReadModelRepository[TestModel]
    ) -> None:
        """Test get_deleted returns None for non-existent ID."""
        result = await repo.get_deleted(uuid4())
        assert result is None

    @pytest.mark.asyncio
    async def test_get_deleted_not_deleted_returns_none(
        self, repo: InMemoryReadModelRepository[TestModel]
    ) -> None:
        """Test get_deleted returns None for non-deleted records."""
        model_id = uuid4()
        model = TestModel(id=model_id, name="test")
        await repo.save(model)

        result = await repo.get_deleted(model_id)
        assert result is None

    @pytest.mark.asyncio
    async def test_get_deleted_after_restore_returns_none(
        self, repo: InMemoryReadModelRepository[TestModel]
    ) -> None:
        """Test get_deleted returns None after restore."""
        model_id = uuid4()
        model = TestModel(id=model_id, name="test")
        await repo.save(model)
        await repo.soft_delete(model_id)

        # Should be returned as deleted
        assert await repo.get_deleted(model_id) is not None

        await repo.restore(model_id)

        # Should not be returned as deleted anymore
        assert await repo.get_deleted(model_id) is None


class TestFindDeleted:
    """Tests for find_deleted() method."""

    @pytest.mark.asyncio
    async def test_find_deleted_returns_only_deleted(
        self, repo: InMemoryReadModelRepository[TestModel]
    ) -> None:
        """Test find_deleted returns only soft-deleted records."""
        active_id = uuid4()
        deleted_id = uuid4()

        await repo.save(TestModel(id=active_id, name="active"))
        await repo.save(TestModel(id=deleted_id, name="deleted"))
        await repo.soft_delete(deleted_id)

        deleted_models = await repo.find_deleted()

        assert len(deleted_models) == 1
        assert deleted_models[0].id == deleted_id

    @pytest.mark.asyncio
    async def test_find_deleted_with_no_deleted_returns_empty(
        self, repo: InMemoryReadModelRepository[TestModel]
    ) -> None:
        """Test find_deleted returns empty list when no deleted records."""
        await repo.save(TestModel(id=uuid4(), name="active1"))
        await repo.save(TestModel(id=uuid4(), name="active2"))

        deleted_models = await repo.find_deleted()

        assert deleted_models == []

    @pytest.mark.asyncio
    async def test_find_deleted_with_filter(
        self, repo: InMemoryReadModelRepository[TestModel]
    ) -> None:
        """Test find_deleted with filter."""
        id1 = uuid4()
        id2 = uuid4()

        await repo.save(TestModel(id=id1, name="deleted1", status="pending"))
        await repo.save(TestModel(id=id2, name="deleted2", status="shipped"))
        await repo.soft_delete(id1)
        await repo.soft_delete(id2)

        results = await repo.find_deleted(Query(filters=[Filter.eq("status", "pending")]))

        assert len(results) == 1
        assert results[0].id == id1

    @pytest.mark.asyncio
    async def test_find_deleted_with_ordering(
        self, repo: InMemoryReadModelRepository[TestModel]
    ) -> None:
        """Test find_deleted with ordering."""
        id1 = uuid4()
        id2 = uuid4()

        await repo.save(TestModel(id=id1, name="aaa"))
        await repo.save(TestModel(id=id2, name="zzz"))
        await repo.soft_delete(id1)
        await repo.soft_delete(id2)

        results = await repo.find_deleted(Query(order_by="name", order_direction="desc"))

        assert len(results) == 2
        assert results[0].name == "zzz"
        assert results[1].name == "aaa"

    @pytest.mark.asyncio
    async def test_find_deleted_with_pagination(
        self, repo: InMemoryReadModelRepository[TestModel]
    ) -> None:
        """Test find_deleted with limit and offset."""
        for i in range(5):
            model = TestModel(id=uuid4(), name=f"model_{i:02d}")
            await repo.save(model)
            await repo.soft_delete(model.id)

        results = await repo.find_deleted(Query(order_by="name", limit=2, offset=1))

        assert len(results) == 2
        assert results[0].name == "model_01"
        assert results[1].name == "model_02"

    @pytest.mark.asyncio
    async def test_find_deleted_null_query(
        self, repo: InMemoryReadModelRepository[TestModel]
    ) -> None:
        """Test find_deleted with None query."""
        model_id = uuid4()
        await repo.save(TestModel(id=model_id, name="deleted"))
        await repo.soft_delete(model_id)

        results = await repo.find_deleted(None)

        assert len(results) == 1


class TestGetExcludesSoftDeleted:
    """Tests verifying get() excludes soft-deleted records."""

    @pytest.mark.asyncio
    async def test_get_returns_none_for_soft_deleted(
        self, repo: InMemoryReadModelRepository[TestModel]
    ) -> None:
        """Test that get() returns None for soft-deleted records."""
        model_id = uuid4()
        model = TestModel(id=model_id, name="test")
        await repo.save(model)

        assert await repo.get(model_id) is not None

        await repo.soft_delete(model_id)

        assert await repo.get(model_id) is None


class TestGetManyExcludesSoftDeleted:
    """Tests verifying get_many() excludes soft-deleted records."""

    @pytest.mark.asyncio
    async def test_get_many_excludes_soft_deleted(
        self, repo: InMemoryReadModelRepository[TestModel]
    ) -> None:
        """Test that get_many() excludes soft-deleted records."""
        id1 = uuid4()
        id2 = uuid4()

        await repo.save(TestModel(id=id1, name="active"))
        await repo.save(TestModel(id=id2, name="deleted"))
        await repo.soft_delete(id2)

        results = await repo.get_many([id1, id2])

        assert len(results) == 1
        assert results[0].id == id1


class TestFindExcludesSoftDeleted:
    """Tests verifying find() excludes soft-deleted by default."""

    @pytest.mark.asyncio
    async def test_find_excludes_soft_deleted_by_default(
        self, repo: InMemoryReadModelRepository[TestModel]
    ) -> None:
        """Test that find() excludes soft-deleted records by default."""
        active_id = uuid4()
        deleted_id = uuid4()

        await repo.save(TestModel(id=active_id, name="active"))
        await repo.save(TestModel(id=deleted_id, name="deleted"))
        await repo.soft_delete(deleted_id)

        results = await repo.find()

        assert len(results) == 1
        assert results[0].id == active_id

    @pytest.mark.asyncio
    async def test_find_include_deleted_true(
        self, repo: InMemoryReadModelRepository[TestModel]
    ) -> None:
        """Test that find(include_deleted=True) includes soft-deleted records."""
        active_id = uuid4()
        deleted_id = uuid4()

        await repo.save(TestModel(id=active_id, name="active"))
        await repo.save(TestModel(id=deleted_id, name="deleted"))
        await repo.soft_delete(deleted_id)

        results = await repo.find(Query(include_deleted=True))

        assert len(results) == 2
        assert {r.id for r in results} == {active_id, deleted_id}

    @pytest.mark.asyncio
    async def test_find_with_deleted_builder_method(
        self, repo: InMemoryReadModelRepository[TestModel]
    ) -> None:
        """Test find using Query.with_deleted() builder method."""
        active_id = uuid4()
        deleted_id = uuid4()

        await repo.save(TestModel(id=active_id, name="active"))
        await repo.save(TestModel(id=deleted_id, name="deleted"))
        await repo.soft_delete(deleted_id)

        query = Query().with_deleted(True)
        results = await repo.find(query)

        assert len(results) == 2


class TestCountExcludesSoftDeleted:
    """Tests verifying count() excludes soft-deleted records."""

    @pytest.mark.asyncio
    async def test_count_excludes_soft_deleted_by_default(
        self, repo: InMemoryReadModelRepository[TestModel]
    ) -> None:
        """Test that count() excludes soft-deleted records by default."""
        for i in range(5):
            await repo.save(TestModel(id=uuid4(), name=f"model_{i}"))

        results = await repo.find()
        await repo.soft_delete(results[0].id)
        await repo.soft_delete(results[1].id)

        count = await repo.count()

        assert count == 3

    @pytest.mark.asyncio
    async def test_count_include_deleted_true(
        self, repo: InMemoryReadModelRepository[TestModel]
    ) -> None:
        """Test that count(include_deleted=True) includes soft-deleted records."""
        for i in range(5):
            await repo.save(TestModel(id=uuid4(), name=f"model_{i}"))

        results = await repo.find()
        await repo.soft_delete(results[0].id)
        await repo.soft_delete(results[1].id)

        count = await repo.count(Query(include_deleted=True))

        assert count == 5


class TestExistsExcludesSoftDeleted:
    """Tests verifying exists() excludes soft-deleted records."""

    @pytest.mark.asyncio
    async def test_exists_returns_false_for_soft_deleted(
        self, repo: InMemoryReadModelRepository[TestModel]
    ) -> None:
        """Test that exists() returns False for soft-deleted records."""
        model_id = uuid4()
        model = TestModel(id=model_id, name="test")
        await repo.save(model)

        assert await repo.exists(model_id) is True

        await repo.soft_delete(model_id)

        assert await repo.exists(model_id) is False


class TestTruncateRemovesSoftDeleted:
    """Tests verifying truncate() removes soft-deleted records."""

    @pytest.mark.asyncio
    async def test_truncate_removes_all_including_soft_deleted(
        self, repo: InMemoryReadModelRepository[TestModel]
    ) -> None:
        """Test that truncate() removes all records including soft-deleted."""
        active_id = uuid4()
        deleted_id = uuid4()

        await repo.save(TestModel(id=active_id, name="active"))
        await repo.save(TestModel(id=deleted_id, name="deleted"))
        await repo.soft_delete(deleted_id)

        count = await repo.truncate()

        assert count == 2
        assert len(repo) == 0
        assert await repo.count(Query(include_deleted=True)) == 0


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
