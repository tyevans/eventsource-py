"""Unit tests for optimistic locking in read model repositories."""

from uuid import uuid4

import pytest

from eventsource.readmodels import (
    InMemoryReadModelRepository,
    OptimisticLockError,
    ReadModel,
    ReadModelError,
    ReadModelNotFoundError,
)


class TestModel(ReadModel):
    """Test model for optimistic locking tests."""

    name: str
    value: int = 0


@pytest.fixture
def repo() -> InMemoryReadModelRepository[TestModel]:
    """Create a fresh in-memory repository for each test."""
    return InMemoryReadModelRepository(TestModel, enable_tracing=False)


class TestOptimisticLockErrorException:
    """Tests for OptimisticLockError exception."""

    def test_optimistic_lock_error_with_actual_version(self) -> None:
        """Test OptimisticLockError includes model_id and versions."""
        model_id = uuid4()
        error = OptimisticLockError(model_id, expected_version=1, actual_version=2)

        assert error.model_id == model_id
        assert error.expected_version == 1
        assert error.actual_version == 2
        assert "expected version 1" in str(error)
        assert "found version 2" in str(error)
        assert str(model_id) in str(error)

    def test_optimistic_lock_error_without_actual_version(self) -> None:
        """Test OptimisticLockError when actual version is unknown."""
        model_id = uuid4()
        error = OptimisticLockError(model_id, expected_version=1, actual_version=None)

        assert error.model_id == model_id
        assert error.expected_version == 1
        assert error.actual_version is None
        assert "expected version 1" in str(error)
        assert "was not found or was modified" in str(error)

    def test_optimistic_lock_error_is_read_model_error(self) -> None:
        """Test OptimisticLockError inherits from ReadModelError."""
        error = OptimisticLockError(uuid4(), expected_version=1)
        assert isinstance(error, ReadModelError)
        assert isinstance(error, Exception)


class TestReadModelNotFoundErrorException:
    """Tests for ReadModelNotFoundError exception."""

    def test_read_model_not_found_error(self) -> None:
        """Test ReadModelNotFoundError includes model_id."""
        model_id = uuid4()
        error = ReadModelNotFoundError(model_id)

        assert error.model_id == model_id
        assert str(model_id) in str(error)
        assert "not found" in str(error)

    def test_read_model_not_found_error_is_read_model_error(self) -> None:
        """Test ReadModelNotFoundError inherits from ReadModelError."""
        error = ReadModelNotFoundError(uuid4())
        assert isinstance(error, ReadModelError)
        assert isinstance(error, Exception)


class TestInMemoryOptimisticLocking:
    """Tests for optimistic locking in InMemoryReadModelRepository."""

    @pytest.mark.asyncio
    async def test_save_with_correct_version_succeeds(
        self, repo: InMemoryReadModelRepository[TestModel]
    ) -> None:
        """Test save_with_version_check succeeds with correct version."""
        model_id = uuid4()
        model = TestModel(id=model_id, name="original", value=1)
        await repo.save(model)

        # Load and modify
        loaded = await repo.get(model_id)
        assert loaded is not None
        loaded.name = "updated"

        # Should succeed
        await repo.save_with_version_check(loaded)

        result = await repo.get(model_id)
        assert result is not None
        assert result.name == "updated"
        assert result.version == 2

    @pytest.mark.asyncio
    async def test_save_with_wrong_version_raises_optimistic_lock_error(
        self, repo: InMemoryReadModelRepository[TestModel]
    ) -> None:
        """Test save_with_version_check fails with wrong version."""
        model_id = uuid4()
        model = TestModel(id=model_id, name="original", value=1)
        await repo.save(model)

        # Load model and create a copy (simulating separate processes)
        reader1 = await repo.get(model_id)
        assert reader1 is not None
        # Create a separate model instance with same data to simulate separate process
        reader2 = TestModel(
            id=model_id,
            name=reader1.name,
            value=reader1.value,
            version=reader1.version,
            created_at=reader1.created_at,
            updated_at=reader1.updated_at,
        )

        # First reader succeeds and increments version to 2
        reader1.name = "updated"
        await repo.save_with_version_check(reader1)

        # Second reader still has version 1 - should fail
        reader2.name = "conflict"

        with pytest.raises(OptimisticLockError) as exc_info:
            await repo.save_with_version_check(reader2)

        assert exc_info.value.model_id == model_id
        assert exc_info.value.expected_version == 1
        assert exc_info.value.actual_version == 2

    @pytest.mark.asyncio
    async def test_save_nonexistent_model_raises_not_found_error(
        self, repo: InMemoryReadModelRepository[TestModel]
    ) -> None:
        """Test save_with_version_check fails for nonexistent model."""
        model_id = uuid4()
        model = TestModel(id=model_id, name="new", value=1)

        with pytest.raises(ReadModelNotFoundError) as exc_info:
            await repo.save_with_version_check(model)

        assert exc_info.value.model_id == model_id

    @pytest.mark.asyncio
    async def test_version_incremented_after_successful_save(
        self, repo: InMemoryReadModelRepository[TestModel]
    ) -> None:
        """Test version is incremented after successful save_with_version_check."""
        model_id = uuid4()
        model = TestModel(id=model_id, name="original", value=1)
        await repo.save(model)

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
    async def test_updated_at_changed_on_successful_save(
        self, repo: InMemoryReadModelRepository[TestModel]
    ) -> None:
        """Test updated_at is changed on successful save_with_version_check."""
        import asyncio

        model_id = uuid4()
        model = TestModel(id=model_id, name="original", value=1)
        await repo.save(model)

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
    async def test_model_in_memory_is_updated(
        self, repo: InMemoryReadModelRepository[TestModel]
    ) -> None:
        """Test that the model object is updated with new version after save."""
        model_id = uuid4()
        model = TestModel(id=model_id, name="original", value=1)
        await repo.save(model)

        loaded = await repo.get(model_id)
        assert loaded is not None
        assert loaded.version == 1

        loaded.name = "updated"
        await repo.save_with_version_check(loaded)

        # The model object should now have version 2
        assert loaded.version == 2

    @pytest.mark.asyncio
    async def test_concurrent_updates_one_succeeds_one_fails(
        self, repo: InMemoryReadModelRepository[TestModel]
    ) -> None:
        """Test that concurrent updates result in one success and one failure."""
        model_id = uuid4()
        model = TestModel(id=model_id, name="original", value=1)
        await repo.save(model)

        # Simulate two concurrent reads - create copies with the same version
        reader1 = await repo.get(model_id)
        assert reader1 is not None
        # Create a separate model instance with same data to simulate separate process
        reader2 = TestModel(
            id=model_id,
            name=reader1.name,
            value=reader1.value,
            version=reader1.version,
            created_at=reader1.created_at,
            updated_at=reader1.updated_at,
        )

        # Both have version 1
        assert reader1.version == 1
        assert reader2.version == 1

        # First update succeeds
        reader1.name = "update1"
        await repo.save_with_version_check(reader1)

        # Second update should fail - reader2 still has version 1
        reader2.name = "update2"
        with pytest.raises(OptimisticLockError) as exc_info:
            await repo.save_with_version_check(reader2)

        assert exc_info.value.expected_version == 1
        assert exc_info.value.actual_version == 2

        # Verify final state
        final = await repo.get(model_id)
        assert final is not None
        assert final.name == "update1"
        assert final.version == 2

    @pytest.mark.asyncio
    async def test_multiple_sequential_updates_succeed(
        self, repo: InMemoryReadModelRepository[TestModel]
    ) -> None:
        """Test multiple sequential updates with version checks succeed."""
        model_id = uuid4()
        model = TestModel(id=model_id, name="v1", value=1)
        await repo.save(model)

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
    async def test_catch_read_model_error_catches_both(
        self, repo: InMemoryReadModelRepository[TestModel]
    ) -> None:
        """Test catching ReadModelError catches both exception types."""
        model_id = uuid4()
        model = TestModel(id=model_id, name="original", value=1)

        # Test catching ReadModelNotFoundError via ReadModelError
        with pytest.raises(ReadModelError):
            await repo.save_with_version_check(model)

        # Save the model first
        await repo.save(model)

        # Load and save with version check to increment to version 2
        loaded = await repo.get(model_id)
        assert loaded is not None
        loaded.name = "updated"
        await repo.save_with_version_check(loaded)

        # Create model with old version to simulate stale read
        stale_model = TestModel(
            id=model_id,
            name="stale",
            value=1,
            version=1,  # Old version
        )

        # Test catching OptimisticLockError via ReadModelError
        with pytest.raises(ReadModelError):
            await repo.save_with_version_check(stale_model)


class TestOptimisticLockingExports:
    """Test that optimistic locking classes are properly exported."""

    def test_exceptions_exported_from_readmodels(self) -> None:
        """Test exceptions can be imported from eventsource.readmodels."""
        from eventsource.readmodels import (
            OptimisticLockError,
            ReadModelError,
            ReadModelNotFoundError,
        )

        assert OptimisticLockError is not None
        assert ReadModelError is not None
        assert ReadModelNotFoundError is not None

    def test_exceptions_in_all(self) -> None:
        """Test exceptions are in __all__."""
        from eventsource import readmodels

        assert "OptimisticLockError" in readmodels.__all__
        assert "ReadModelError" in readmodels.__all__
        assert "ReadModelNotFoundError" in readmodels.__all__
