"""Unit tests for optimistic locking in read model repositories."""

import asyncio
from uuid import uuid4

import pytest

from eventsource.adapters.memory.readmodels import InMemoryReadModelRepository
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


class TestReadModelVersionConflictErrorException:
    """Tests for ReadModelVersionConflictError exception."""

    def test_optimistic_lock_error_with_actual_version(self) -> None:
        model_id = uuid4()
        error = ReadModelVersionConflictError(model_id, expected_version=1, actual_version=2)
        assert error.model_id == model_id
        assert error.expected_version == 1
        assert error.actual_version == 2
        assert "expected version 1" in str(error)
        assert "found version 2" in str(error)
        assert str(model_id) in str(error)

    def test_optimistic_lock_error_without_actual_version(self) -> None:
        model_id = uuid4()
        error = ReadModelVersionConflictError(model_id, expected_version=1, actual_version=None)
        assert error.model_id == model_id
        assert error.expected_version == 1
        assert error.actual_version is None
        assert "expected version 1" in str(error)
        assert "was not found or was modified" in str(error)

    def test_optimistic_lock_error_is_read_model_error(self) -> None:
        error = ReadModelVersionConflictError(uuid4(), expected_version=1)
        assert isinstance(error, ReadModelError)
        assert isinstance(error, Exception)


class TestReadModelNotFoundErrorException:
    """Tests for ReadModelNotFoundError exception."""

    def test_read_model_not_found_error(self) -> None:
        model_id = uuid4()
        error = ReadModelNotFoundError(model_id)
        assert error.model_id == model_id
        assert str(model_id) in str(error)
        assert "not found" in str(error)

    def test_read_model_not_found_error_is_read_model_error(self) -> None:
        error = ReadModelNotFoundError(uuid4())
        assert isinstance(error, ReadModelError)
        assert isinstance(error, Exception)


class TestInMemoryOptimisticLocking:
    """Tests for optimistic locking in InMemoryReadModelRepository."""

    @pytest.mark.asyncio
    async def test_save_with_correct_version_succeeds(self, repo: Repo) -> None:
        model_id = uuid4()
        await repo.save(TestModel(id=model_id, name="original", value=1))
        loaded = await repo.get(model_id)
        assert loaded is not None
        loaded.name = "updated"
        await repo.save_with_version_check(loaded)
        result = await repo.get(model_id)
        assert result is not None
        assert result.name == "updated"
        assert result.version == 2

    @pytest.mark.asyncio
    async def test_save_with_wrong_version_raises_optimistic_lock_error(self, repo: Repo) -> None:
        model_id = uuid4()
        await repo.save(TestModel(id=model_id, name="original", value=1))
        reader1 = await repo.get(model_id)
        assert reader1 is not None
        reader2 = reader1.model_copy()
        reader1.name = "updated"
        await repo.save_with_version_check(reader1)
        reader2.name = "conflict"
        with pytest.raises(ReadModelVersionConflictError) as exc_info:
            await repo.save_with_version_check(reader2)
        assert exc_info.value.model_id == model_id
        assert exc_info.value.expected_version == 1
        assert exc_info.value.actual_version == 2

    @pytest.mark.asyncio
    async def test_save_nonexistent_model_raises_not_found_error(self, repo: Repo) -> None:
        model_id = uuid4()
        model = TestModel(id=model_id, name="new", value=1)
        with pytest.raises(ReadModelNotFoundError) as exc_info:
            await repo.save_with_version_check(model)
        assert exc_info.value.model_id == model_id

    @pytest.mark.asyncio
    async def test_version_incremented_after_successful_save(self, repo: Repo) -> None:
        model_id = uuid4()
        await repo.save(TestModel(id=model_id, name="original", value=1))
        loaded = await repo.get(model_id)
        assert loaded is not None
        assert loaded.version == 1
        loaded.name = "v2"
        await repo.save_with_version_check(loaded)
        reloaded = await repo.get(model_id)
        assert reloaded is not None
        assert reloaded.version == 2
        reloaded.name = "v3"
        await repo.save_with_version_check(reloaded)
        final = await repo.get(model_id)
        assert final is not None
        assert final.version == 3

    @pytest.mark.asyncio
    async def test_updated_at_changed_on_successful_save(self, repo: Repo) -> None:
        model_id = uuid4()
        await repo.save(TestModel(id=model_id, name="original", value=1))
        loaded = await repo.get(model_id)
        assert loaded is not None
        original_updated_at = loaded.updated_at
        await asyncio.sleep(0.01)
        loaded.name = "updated"
        await repo.save_with_version_check(loaded)
        result = await repo.get(model_id)
        assert result is not None
        assert result.updated_at > original_updated_at

    @pytest.mark.asyncio
    async def test_version_advances_in_storage_not_on_the_callers_object(self, repo: Repo) -> None:
        """The version bump lands in storage; the writer's object is its own."""
        model_id = uuid4()
        await repo.save(TestModel(id=model_id, name="original", value=1))
        loaded = await repo.get(model_id)
        assert loaded is not None
        assert loaded.version == 1
        loaded.name = "updated"
        await repo.save_with_version_check(loaded)
        assert loaded.version == 1
        reloaded = await repo.get(model_id)
        assert reloaded is not None
        assert reloaded.version == 2

    @pytest.mark.asyncio
    async def test_concurrent_updates_one_succeeds_one_fails(self, repo: Repo) -> None:
        model_id = uuid4()
        await repo.save(TestModel(id=model_id, name="original", value=1))
        reader1 = await repo.get(model_id)
        assert reader1 is not None
        reader2 = reader1.model_copy()
        assert reader1.version == 1
        assert reader2.version == 1
        reader1.name = "update1"
        await repo.save_with_version_check(reader1)
        reader2.name = "update2"
        with pytest.raises(ReadModelVersionConflictError) as exc_info:
            await repo.save_with_version_check(reader2)
        assert exc_info.value.expected_version == 1
        assert exc_info.value.actual_version == 2
        final = await repo.get(model_id)
        assert final is not None
        assert final.name == "update1"
        assert final.version == 2

    @pytest.mark.asyncio
    async def test_multiple_sequential_updates_succeed(self, repo: Repo) -> None:
        model_id = uuid4()
        await repo.save(TestModel(id=model_id, name="v1", value=1))
        for i in range(2, 6):
            loaded = await repo.get(model_id)
            assert loaded is not None
            loaded.name = f"v{i}"
            loaded.value = i
            await repo.save_with_version_check(loaded)
        final = await repo.get(model_id)
        assert final is not None
        assert final.name == "v5"
        assert final.value == 5
        assert final.version == 5

    @pytest.mark.asyncio
    async def test_catch_read_model_error_catches_both(self, repo: Repo) -> None:
        model_id = uuid4()
        model = TestModel(id=model_id, name="original", value=1)
        with pytest.raises(ReadModelError):
            await repo.save_with_version_check(model)
        await repo.save(model)
        loaded = await repo.get(model_id)
        assert loaded is not None
        loaded.name = "updated"
        await repo.save_with_version_check(loaded)
        stale_model = TestModel(id=model_id, name="stale", value=1, version=1)
        with pytest.raises(ReadModelError):
            await repo.save_with_version_check(stale_model)


class TestOptimisticLockingExports:
    """Test that optimistic locking classes are properly exported."""

    def test_exceptions_exported_from_readmodels(self) -> None:
        from eventsource.ports.readmodels import (
            ReadModelError,
            ReadModelNotFoundError,
            ReadModelVersionConflictError,
        )

        assert ReadModelVersionConflictError is not None
        assert ReadModelError is not None
        assert ReadModelNotFoundError is not None

    def test_exceptions_in_all(self) -> None:
        from eventsource.ports import readmodels

        assert "ReadModelVersionConflictError" in readmodels.__all__
        assert "ReadModelError" in readmodels.__all__
        assert "ReadModelNotFoundError" in readmodels.__all__
