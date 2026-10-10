"""Integration tests for combined feature interactions (soft delete + optimistic locking)."""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import pytest

from eventsource.ports.readmodels import ReadModelVersionConflictError

from .conftest import EnhancedTestModel

if TYPE_CHECKING:
    pass

pytestmark = [
    pytest.mark.integration,
]


class TestCombinedFeatures:
    """Tests for combined feature interactions."""

    @pytest.mark.asyncio
    async def test_restore_then_version_check_save(
        self,
        enhanced_repo: Any,
        enhanced_model_factory: Callable[..., EnhancedTestModel],
    ) -> None:
        """Test restore followed by version-checked save."""
        model_id = uuid4()
        model = enhanced_model_factory(id=model_id, name="test")
        await enhanced_repo.save(model)  # v1
        await enhanced_repo.soft_delete(model_id)  # Sets deleted_at

        await enhanced_repo.restore(model_id)  # Clears deleted_at

        loaded = await enhanced_repo.get(model_id)
        assert loaded is not None
        loaded.name = "after_restore"

        # This should work
        await enhanced_repo.save_with_version_check(loaded)

        result = await enhanced_repo.get(model_id)
        assert result is not None
        assert result.name == "after_restore"

    @pytest.mark.asyncio
    async def test_soft_delete_does_not_affect_version_check(
        self,
        enhanced_repo: Any,
        enhanced_model_factory: Callable[..., EnhancedTestModel],
    ) -> None:
        """Test that soft delete does not interfere with version checking."""
        # Create and save model
        model_id = uuid4()
        model = enhanced_model_factory(id=model_id, name="original")
        await enhanced_repo.save(model)

        # Load the model (version 1)
        loaded = await enhanced_repo.get(model_id)
        assert loaded is not None
        assert loaded.version == 1

        # Update with version check (version 2)
        loaded.name = "updated"
        await enhanced_repo.save_with_version_check(loaded)

        # Soft delete
        deleted = await enhanced_repo.soft_delete(model_id)
        assert deleted is True

        # Verify model is soft deleted
        assert await enhanced_repo.get(model_id) is None
        deleted_model = await enhanced_repo.get_deleted(model_id)
        assert deleted_model is not None
        assert deleted_model.name == "updated"

    @pytest.mark.asyncio
    async def test_get_deleted_preserves_version_info(
        self,
        enhanced_repo: Any,
        enhanced_model_factory: Callable[..., EnhancedTestModel],
    ) -> None:
        """Test that get_deleted preserves version information."""
        model_id = uuid4()
        model = enhanced_model_factory(id=model_id, name="v1")
        await enhanced_repo.save(model)

        # Update multiple times
        for i in range(2, 4):
            loaded = await enhanced_repo.get(model_id)
            assert loaded is not None
            loaded.name = f"v{i}"
            await enhanced_repo.save(loaded)

        # Soft delete
        await enhanced_repo.soft_delete(model_id)

        # Check version is preserved in deleted record
        deleted = await enhanced_repo.get_deleted(model_id)
        assert deleted is not None
        assert deleted.version == 3  # v1 -> v2 -> v3
        assert deleted.name == "v3"

    @pytest.mark.asyncio
    async def test_find_deleted_preserves_all_model_data(
        self,
        enhanced_repo: Any,
        enhanced_model_factory: Callable[..., EnhancedTestModel],
    ) -> None:
        """Test that find_deleted returns complete model data."""
        model_id = uuid4()
        model = enhanced_model_factory(
            id=model_id,
            name="complete_data",
            category="special",
            value=42,
            amount=Decimal("99.99"),
        )
        await enhanced_repo.save(model)
        await enhanced_repo.soft_delete(model_id)

        results = await enhanced_repo.find_deleted()

        assert len(results) == 1
        deleted = results[0]
        assert deleted.id == model_id
        assert deleted.name == "complete_data"
        assert deleted.category == "special"
        assert deleted.value == 42
        # Use approximate comparison for decimal to handle float/decimal conversion
        assert abs(float(deleted.amount) - 99.99) < 0.01

    @pytest.mark.asyncio
    async def test_version_conflict_with_soft_deleted_model(
        self,
        enhanced_repo: Any,
        enhanced_model_factory: Callable[..., EnhancedTestModel],
    ) -> None:
        """Test version conflict detection still works after restore."""
        model_id = uuid4()
        model = enhanced_model_factory(id=model_id, name="original")
        await enhanced_repo.save(model)

        # Load twice
        client1 = await enhanced_repo.get(model_id)
        assert client1 is not None
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

        # Client 1 soft deletes
        await enhanced_repo.soft_delete(model_id)

        # Restore the model
        await enhanced_repo.restore(model_id)

        # Client 1 loads fresh and updates
        fresh = await enhanced_repo.get(model_id)
        assert fresh is not None
        fresh.name = "fresh_update"
        await enhanced_repo.save_with_version_check(fresh)

        # Client 2 still has old version - should fail
        client2.name = "stale_update"
        with pytest.raises(ReadModelVersionConflictError):
            await enhanced_repo.save_with_version_check(client2)
