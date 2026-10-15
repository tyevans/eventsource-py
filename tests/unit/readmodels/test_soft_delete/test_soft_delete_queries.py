"""Query unit tests for soft delete functionality."""

from uuid import uuid4

import pytest

from eventsource.adapters.memory.readmodels import InMemoryReadModelRepository
from eventsource.ports.readmodels.query import Filter, Query
from tests.unit.readmodels.test_soft_delete._fixtures import TestModel


class TestGetDeleted:
    """Tests for get_deleted() method."""

    @pytest.mark.asyncio
    async def test_get_deleted_returns_soft_deleted(
        self, repo: InMemoryReadModelRepository[TestModel]
    ) -> None:
        model_id = uuid4()
        await repo.save(TestModel(id=model_id, name="test"))
        assert await repo.get_deleted(model_id) is None

        await repo.soft_delete(model_id)
        deleted = await repo.get_deleted(model_id)
        assert deleted is not None
        assert deleted.id == model_id
        assert deleted.deleted_at is not None

    @pytest.mark.asyncio
    async def test_get_deleted_nonexistent_returns_none(
        self, repo: InMemoryReadModelRepository[TestModel]
    ) -> None:
        result = await repo.get_deleted(uuid4())
        assert result is None

    @pytest.mark.asyncio
    async def test_get_deleted_not_deleted_returns_none(
        self, repo: InMemoryReadModelRepository[TestModel]
    ) -> None:
        model_id = uuid4()
        await repo.save(TestModel(id=model_id, name="test"))
        result = await repo.get_deleted(model_id)
        assert result is None

    @pytest.mark.asyncio
    async def test_get_deleted_after_restore_returns_none(
        self, repo: InMemoryReadModelRepository[TestModel]
    ) -> None:
        model_id = uuid4()
        await repo.save(TestModel(id=model_id, name="test"))
        await repo.soft_delete(model_id)
        assert await repo.get_deleted(model_id) is not None

        await repo.restore(model_id)
        assert await repo.get_deleted(model_id) is None


class TestFindDeleted:
    """Tests for find_deleted() method."""

    @pytest.mark.asyncio
    async def test_find_deleted_returns_only_deleted(
        self, repo: InMemoryReadModelRepository[TestModel]
    ) -> None:
        active_id, deleted_id = uuid4(), uuid4()
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
        await repo.save(TestModel(id=uuid4(), name="active1"))
        await repo.save(TestModel(id=uuid4(), name="active2"))
        deleted_models = await repo.find_deleted()
        assert deleted_models == []

    @pytest.mark.asyncio
    async def test_find_deleted_with_filter(
        self, repo: InMemoryReadModelRepository[TestModel]
    ) -> None:
        id1, id2 = uuid4(), uuid4()
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
        id1, id2 = uuid4(), uuid4()
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
        model_id = uuid4()
        await repo.save(TestModel(id=model_id, name="test"))
        assert await repo.get(model_id) is not None

        await repo.soft_delete(model_id)
        assert await repo.get(model_id) is None


class TestGetManyExcludesSoftDeleted:
    """Tests verifying get_many() excludes soft-deleted records."""

    @pytest.mark.asyncio
    async def test_get_many_excludes_soft_deleted(
        self, repo: InMemoryReadModelRepository[TestModel]
    ) -> None:
        id1, id2 = uuid4(), uuid4()
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
        active_id, deleted_id = uuid4(), uuid4()
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
        active_id, deleted_id = uuid4(), uuid4()
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
        active_id, deleted_id = uuid4(), uuid4()
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
        model_id = uuid4()
        await repo.save(TestModel(id=model_id, name="test"))
        assert await repo.exists(model_id) is True

        await repo.soft_delete(model_id)
        assert await repo.exists(model_id) is False


class TestTruncateRemovesSoftDeleted:
    """Tests verifying truncate() removes soft-deleted records."""

    @pytest.mark.asyncio
    async def test_truncate_removes_all_including_soft_deleted(
        self, repo: InMemoryReadModelRepository[TestModel]
    ) -> None:
        active_id, deleted_id = uuid4(), uuid4()
        await repo.save(TestModel(id=active_id, name="active"))
        await repo.save(TestModel(id=deleted_id, name="deleted"))
        await repo.soft_delete(deleted_id)

        count = await repo.truncate()
        assert count == 2
        assert len(repo) == 0
        assert await repo.count(Query(include_deleted=True)) == 0
