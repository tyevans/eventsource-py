"""Query unit tests for soft delete functionality."""

from uuid import UUID, uuid4

import pytest

from eventsource.adapters.memory.readmodels import InMemoryReadModelRepository
from eventsource.ports.readmodels.query import Filter, Query
from tests.unit.readmodels.test_soft_delete._fixtures import TestModel

type Repo = InMemoryReadModelRepository[TestModel]


async def _seed_active_and_deleted(repo: Repo) -> tuple[UUID, UUID]:
    active_id, deleted_id = uuid4(), uuid4()
    await repo.save(TestModel(id=active_id, name="active"))
    await repo.save(TestModel(id=deleted_id, name="deleted"))
    await repo.soft_delete(deleted_id)
    return active_id, deleted_id


async def _seed_five_with_two_deleted(repo: Repo) -> None:
    for i in range(5):
        await repo.save(TestModel(id=uuid4(), name=f"model_{i}"))
    results = await repo.find()
    await repo.soft_delete(results[0].id)
    await repo.soft_delete(results[1].id)


class TestGetDeleted:
    @pytest.mark.asyncio
    async def test_get_deleted_returns_soft_deleted(self, repo: Repo) -> None:
        model_id = uuid4()
        await repo.save(TestModel(id=model_id, name="test"))
        assert await repo.get_deleted(model_id) is None

        await repo.soft_delete(model_id)
        deleted = await repo.get_deleted(model_id)
        assert deleted is not None
        assert deleted.id == model_id and deleted.deleted_at is not None

    @pytest.mark.asyncio
    async def test_get_deleted_nonexistent_returns_none(self, repo: Repo) -> None:
        assert await repo.get_deleted(uuid4()) is None

    @pytest.mark.asyncio
    async def test_get_deleted_not_deleted_returns_none(self, repo: Repo) -> None:
        model_id = uuid4()
        await repo.save(TestModel(id=model_id, name="test"))
        assert await repo.get_deleted(model_id) is None

    @pytest.mark.asyncio
    async def test_get_deleted_after_restore_returns_none(self, repo: Repo) -> None:
        model_id = uuid4()
        await repo.save(TestModel(id=model_id, name="test"))
        await repo.soft_delete(model_id)
        assert await repo.get_deleted(model_id) is not None
        await repo.restore(model_id)
        assert await repo.get_deleted(model_id) is None


class TestFindDeleted:
    @pytest.mark.asyncio
    async def test_find_deleted_returns_only_deleted(self, repo: Repo) -> None:
        _, deleted_id = await _seed_active_and_deleted(repo)
        deleted_models = await repo.find_deleted()
        assert [m.id for m in deleted_models] == [deleted_id]

    @pytest.mark.asyncio
    async def test_find_deleted_with_no_deleted_returns_empty(self, repo: Repo) -> None:
        await repo.save(TestModel(id=uuid4(), name="active1"))
        await repo.save(TestModel(id=uuid4(), name="active2"))
        assert await repo.find_deleted() == []

    @pytest.mark.asyncio
    async def test_find_deleted_with_filter(self, repo: Repo) -> None:
        id1, id2 = uuid4(), uuid4()
        await repo.save(TestModel(id=id1, name="deleted1", status="pending"))
        await repo.save(TestModel(id=id2, name="deleted2", status="shipped"))
        await repo.soft_delete(id1)
        await repo.soft_delete(id2)

        results = await repo.find_deleted(Query(filters=[Filter.eq("status", "pending")]))
        assert [r.id for r in results] == [id1]

    @pytest.mark.asyncio
    async def test_find_deleted_with_ordering(self, repo: Repo) -> None:
        id1, id2 = uuid4(), uuid4()
        await repo.save(TestModel(id=id1, name="aaa"))
        await repo.save(TestModel(id=id2, name="zzz"))
        await repo.soft_delete(id1)
        await repo.soft_delete(id2)

        results = await repo.find_deleted(Query(order_by="name", order_direction="desc"))
        assert [r.name for r in results] == ["zzz", "aaa"]

    @pytest.mark.asyncio
    async def test_find_deleted_with_pagination(self, repo: Repo) -> None:
        for i in range(5):
            model = TestModel(id=uuid4(), name=f"model_{i:02d}")
            await repo.save(model)
            await repo.soft_delete(model.id)

        results = await repo.find_deleted(Query(order_by="name", limit=2, offset=1))
        assert [r.name for r in results] == ["model_01", "model_02"]

    @pytest.mark.asyncio
    async def test_find_deleted_null_query(self, repo: Repo) -> None:
        model_id = uuid4()
        await repo.save(TestModel(id=model_id, name="deleted"))
        await repo.soft_delete(model_id)
        assert len(await repo.find_deleted(None)) == 1


class TestGetExcludesSoftDeleted:
    @pytest.mark.asyncio
    async def test_get_returns_none_for_soft_deleted(self, repo: Repo) -> None:
        model_id = uuid4()
        await repo.save(TestModel(id=model_id, name="test"))
        assert await repo.get(model_id) is not None
        await repo.soft_delete(model_id)
        assert await repo.get(model_id) is None


class TestGetManyExcludesSoftDeleted:
    @pytest.mark.asyncio
    async def test_get_many_excludes_soft_deleted(self, repo: Repo) -> None:
        id1, id2 = await _seed_active_and_deleted(repo)
        results = await repo.get_many([id1, id2])
        assert [r.id for r in results] == [id1]


class TestFindExcludesSoftDeleted:
    @pytest.mark.asyncio
    async def test_find_excludes_soft_deleted_by_default(self, repo: Repo) -> None:
        active_id, _ = await _seed_active_and_deleted(repo)
        results = await repo.find()
        assert [r.id for r in results] == [active_id]

    @pytest.mark.asyncio
    async def test_find_include_deleted_true(self, repo: Repo) -> None:
        active_id, deleted_id = await _seed_active_and_deleted(repo)
        results = await repo.find(Query(include_deleted=True))
        assert {r.id for r in results} == {active_id, deleted_id}

    @pytest.mark.asyncio
    async def test_find_with_deleted_builder_method(self, repo: Repo) -> None:
        await _seed_active_and_deleted(repo)
        assert len(await repo.find(Query().with_deleted(True))) == 2


class TestCountExcludesSoftDeleted:
    @pytest.mark.asyncio
    async def test_count_excludes_soft_deleted_by_default(self, repo: Repo) -> None:
        await _seed_five_with_two_deleted(repo)
        assert await repo.count() == 3

    @pytest.mark.asyncio
    async def test_count_include_deleted_true(self, repo: Repo) -> None:
        await _seed_five_with_two_deleted(repo)
        assert await repo.count(Query(include_deleted=True)) == 5


class TestExistsExcludesSoftDeleted:
    @pytest.mark.asyncio
    async def test_exists_returns_false_for_soft_deleted(self, repo: Repo) -> None:
        model_id = uuid4()
        await repo.save(TestModel(id=model_id, name="test"))
        assert await repo.exists(model_id) is True
        await repo.soft_delete(model_id)
        assert await repo.exists(model_id) is False


class TestTruncateRemovesSoftDeleted:
    @pytest.mark.asyncio
    async def test_truncate_removes_all_including_soft_deleted(self, repo: Repo) -> None:
        await _seed_active_and_deleted(repo)
        assert await repo.truncate() == 2
        assert len(repo) == 0
        assert await repo.count(Query(include_deleted=True)) == 0
