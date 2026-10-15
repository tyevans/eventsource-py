"""Unit tests for optimistic locking in read model repositories."""

import asyncio
from uuid import UUID, uuid4

import pytest

from eventsource.adapters.memory.readmodels import InMemoryReadModelRepository
from eventsource.ports import readmodels
from eventsource.ports.readmodels import (
    ReadModel,
    ReadModelError,
    ReadModelNotFoundError,
    ReadModelVersionConflictError,
)


class TestModel(ReadModel):
    name: str
    value: int = 0


type Repo = InMemoryReadModelRepository[TestModel]


@pytest.fixture
def repo() -> Repo:
    return InMemoryReadModelRepository(TestModel, enable_tracing=False)


async def _seed(repo: Repo, name: str = "original", value: int = 1) -> tuple[UUID, TestModel]:
    mid = uuid4()
    await repo.save(TestModel(id=mid, name=name, value=value))
    loaded = await repo.get(mid)
    assert loaded is not None
    return mid, loaded


class TestReadModelVersionConflictErrorException:
    def test_optimistic_lock_error_with_actual_version(self) -> None:
        mid = uuid4()
        err = ReadModelVersionConflictError(mid, expected_version=1, actual_version=2)
        assert (err.model_id, err.expected_version, err.actual_version) == (mid, 1, 2)
        assert all(s in str(err) for s in ("expected version 1", "found version 2", str(mid)))

    def test_optimistic_lock_error_without_actual_version(self) -> None:
        mid = uuid4()
        err = ReadModelVersionConflictError(mid, expected_version=1, actual_version=None)
        assert (err.model_id, err.expected_version, err.actual_version) == (mid, 1, None)
        assert "expected version 1" in str(err) and "was not found or was modified" in str(err)

    def test_optimistic_lock_error_is_read_model_error(self) -> None:
        err = ReadModelVersionConflictError(uuid4(), expected_version=1)
        assert isinstance(err, ReadModelError) and isinstance(err, Exception)


class TestReadModelNotFoundErrorException:
    def test_read_model_not_found_error(self) -> None:
        mid = uuid4()
        err = ReadModelNotFoundError(mid)
        assert err.model_id == mid and str(mid) in str(err) and "not found" in str(err)

    def test_read_model_not_found_error_is_read_model_error(self) -> None:
        err = ReadModelNotFoundError(uuid4())
        assert isinstance(err, ReadModelError) and isinstance(err, Exception)


class TestInMemoryOptimisticLocking:
    @pytest.mark.asyncio
    async def test_save_with_correct_version_succeeds(self, repo: Repo) -> None:
        mid, loaded = await _seed(repo)
        loaded.name = "updated"
        await repo.save_with_version_check(loaded)
        result = await repo.get(mid)
        assert result is not None and (result.name, result.version) == ("updated", 2)

    @pytest.mark.asyncio
    async def test_save_with_wrong_version_raises_optimistic_lock_error(self, repo: Repo) -> None:
        mid, reader1 = await _seed(repo)
        reader2 = reader1.model_copy()
        reader1.name = "updated"
        await repo.save_with_version_check(reader1)
        reader2.name = "conflict"
        with pytest.raises(ReadModelVersionConflictError) as exc_info:
            await repo.save_with_version_check(reader2)
        err = exc_info.value
        assert (err.model_id, err.expected_version, err.actual_version) == (mid, 1, 2)

    @pytest.mark.asyncio
    async def test_save_nonexistent_model_raises_not_found_error(self, repo: Repo) -> None:
        mid = uuid4()
        with pytest.raises(ReadModelNotFoundError) as exc_info:
            await repo.save_with_version_check(TestModel(id=mid, name="new", value=1))
        assert exc_info.value.model_id == mid

    @pytest.mark.asyncio
    async def test_version_incremented_after_successful_save(self, repo: Repo) -> None:
        mid, loaded = await _seed(repo)
        assert loaded.version == 1
        loaded.name = "v2"
        await repo.save_with_version_check(loaded)
        reloaded = await repo.get(mid)
        assert reloaded is not None and reloaded.version == 2
        reloaded.name = "v3"
        await repo.save_with_version_check(reloaded)
        final = await repo.get(mid)
        assert final is not None and final.version == 3

    @pytest.mark.asyncio
    async def test_updated_at_changed_on_successful_save(self, repo: Repo) -> None:
        mid, loaded = await _seed(repo)
        original_updated_at = loaded.updated_at
        await asyncio.sleep(0.01)
        loaded.name = "updated"
        await repo.save_with_version_check(loaded)
        result = await repo.get(mid)
        assert result is not None and result.updated_at > original_updated_at

    @pytest.mark.asyncio
    async def test_version_advances_in_storage_not_on_the_callers_object(self, repo: Repo) -> None:
        mid, loaded = await _seed(repo)
        assert loaded.version == 1
        loaded.name = "updated"
        await repo.save_with_version_check(loaded)
        assert loaded.version == 1
        reloaded = await repo.get(mid)
        assert reloaded is not None and reloaded.version == 2

    @pytest.mark.asyncio
    async def test_concurrent_updates_one_succeeds_one_fails(self, repo: Repo) -> None:
        mid, reader1 = await _seed(repo)
        reader2 = reader1.model_copy()
        assert (reader1.version, reader2.version) == (1, 1)
        reader1.name = "update1"
        await repo.save_with_version_check(reader1)
        reader2.name = "update2"
        with pytest.raises(ReadModelVersionConflictError) as exc_info:
            await repo.save_with_version_check(reader2)
        assert (exc_info.value.expected_version, exc_info.value.actual_version) == (1, 2)
        final = await repo.get(mid)
        assert final is not None and (final.name, final.version) == ("update1", 2)

    @pytest.mark.asyncio
    async def test_multiple_sequential_updates_succeed(self, repo: Repo) -> None:
        mid, _ = await _seed(repo, name="v1", value=1)
        for i in range(2, 6):
            loaded = await repo.get(mid)
            assert loaded is not None
            loaded.name, loaded.value = f"v{i}", i
            await repo.save_with_version_check(loaded)
        final = await repo.get(mid)
        assert final is not None and (final.name, final.value, final.version) == ("v5", 5, 5)

    @pytest.mark.asyncio
    async def test_catch_read_model_error_catches_both(self, repo: Repo) -> None:
        mid = uuid4()
        model = TestModel(id=mid, name="original", value=1)
        with pytest.raises(ReadModelError):
            await repo.save_with_version_check(model)
        await repo.save(model)
        loaded = await repo.get(mid)
        assert loaded is not None
        loaded.name = "updated"
        await repo.save_with_version_check(loaded)
        stale = TestModel(id=mid, name="stale", value=1, version=1)
        with pytest.raises(ReadModelError):
            await repo.save_with_version_check(stale)


class TestOptimisticLockingExports:
    def test_exceptions_exported_from_readmodels(self) -> None:
        assert all((ReadModelVersionConflictError, ReadModelError, ReadModelNotFoundError))

    def test_exceptions_in_all(self) -> None:
        expected = ("ReadModelVersionConflictError", "ReadModelError", "ReadModelNotFoundError")
        assert all(name in readmodels.__all__ for name in expected)
