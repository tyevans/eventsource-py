"""Integration tests for optimistic locking and read model exceptions."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import pytest

from eventsource.ports.readmodels import (
    ReadModelNotFoundError,
    ReadModelVersionConflictError,
)

from .conftest import EnhancedTestModel

if TYPE_CHECKING:
    pass

pytestmark = [
    pytest.mark.integration,
]


class TestOptimisticLocking:
    """Tests for optimistic locking."""

    @pytest.mark.asyncio
    async def test_save_with_version_check_success(
        self,
        enhanced_repo: Any,
        enhanced_model_factory: Callable[..., EnhancedTestModel],
    ) -> None:
        """Test successful save with version check."""
        model_id = uuid4()
        model = enhanced_model_factory(id=model_id, name="original")
        await enhanced_repo.save(model)

        loaded = await enhanced_repo.get(model_id)
        assert loaded is not None
        loaded.name = "updated"

        await enhanced_repo.save_with_version_check(loaded)

        result = await enhanced_repo.get(model_id)
        assert result is not None
        assert result.name == "updated"
        assert result.version == 2

    @pytest.mark.asyncio
    async def test_save_with_version_check_conflict(
        self,
        enhanced_repo: Any,
        enhanced_model_factory: Callable[..., EnhancedTestModel],
    ) -> None:
        """Test save fails with version conflict."""
        model_id = uuid4()
        model = enhanced_model_factory(id=model_id, name="original")
        await enhanced_repo.save(model)

        # Load twice (simulating two clients)
        client1 = await enhanced_repo.get(model_id)
        assert client1 is not None

        # Create a copy for client2 with same version (simulating separate process)
        client2 = EnhancedTestModel(
            id=model_id,
            name=client1.name,
            category=client1.category,
            value=client1.value,
            amount=client1.amount,
            version=client1.version,
            created_at=client1.created_at,
            updated_at=client1.updated_at,
        )

        # Client 1 saves first
        client1.name = "client1_update"
        await enhanced_repo.save_with_version_check(client1)

        # Client 2 tries to save with stale version
        client2.name = "client2_update"

        with pytest.raises(ReadModelVersionConflictError) as exc_info:
            await enhanced_repo.save_with_version_check(client2)

        error = exc_info.value
        assert error.model_id == model_id
        assert error.expected_version == 1
        assert error.actual_version == 2

    @pytest.mark.asyncio
    async def test_save_with_version_check_not_found(
        self,
        enhanced_repo: Any,
        enhanced_model_factory: Callable[..., EnhancedTestModel],
    ) -> None:
        """Test save fails when model doesn't exist."""
        model = enhanced_model_factory(name="new")

        with pytest.raises(ReadModelNotFoundError) as exc_info:
            await enhanced_repo.save_with_version_check(model)

        assert exc_info.value.model_id == model.id

    @pytest.mark.asyncio
    async def test_optimistic_lock_error_message(
        self,
        enhanced_repo: Any,
        enhanced_model_factory: Callable[..., EnhancedTestModel],
    ) -> None:
        """Test ReadModelVersionConflictError has descriptive message."""
        model_id = uuid4()
        await enhanced_repo.save(enhanced_model_factory(id=model_id, name="test"))

        loaded = await enhanced_repo.get(model_id)
        assert loaded is not None
        await enhanced_repo.save(loaded)  # Increment version to 2

        # Create stale model with version 1
        stale = EnhancedTestModel(
            id=model_id,
            name="conflict",
            version=1,
        )

        with pytest.raises(ReadModelVersionConflictError) as exc_info:
            await enhanced_repo.save_with_version_check(stale)

        error_msg = str(exc_info.value)
        assert str(model_id) in error_msg
        assert "version" in error_msg.lower()

    @pytest.mark.asyncio
    async def test_multiple_sequential_updates_with_version_check(
        self,
        enhanced_repo: Any,
        enhanced_model_factory: Callable[..., EnhancedTestModel],
    ) -> None:
        """Test multiple sequential updates with version checks succeed."""
        model_id = uuid4()
        model = enhanced_model_factory(id=model_id, name="v1", value=1)
        await enhanced_repo.save(model)

        for i in range(2, 6):
            loaded = await enhanced_repo.get(model_id)
            assert loaded is not None
            loaded.name = f"v{i}"
            loaded.value = i
            await enhanced_repo.save_with_version_check(loaded)

        final = await enhanced_repo.get(model_id)
        assert final is not None
        assert final.name == "v5"
        assert final.value == 5
        assert final.version == 5


class TestExceptions:
    """Tests for read model exceptions."""

    def test_optimistic_lock_error_with_actual_version(self) -> None:
        """Test ReadModelVersionConflictError with actual version."""
        model_id = uuid4()
        error = ReadModelVersionConflictError(model_id, expected_version=1, actual_version=2)

        assert error.model_id == model_id
        assert error.expected_version == 1
        assert error.actual_version == 2
        assert "1" in str(error)
        assert "2" in str(error)

    def test_optimistic_lock_error_without_actual_version(self) -> None:
        """Test ReadModelVersionConflictError without actual version."""
        model_id = uuid4()
        error = ReadModelVersionConflictError(model_id, expected_version=1)

        assert error.actual_version is None
        assert "1" in str(error)

    def test_read_model_not_found_error(self) -> None:
        """Test ReadModelNotFoundError."""
        model_id = uuid4()
        error = ReadModelNotFoundError(model_id)

        assert error.model_id == model_id
        assert str(model_id) in str(error)
