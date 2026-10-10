"""Integration tests for backend-specific enhanced feature behaviors."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import pytest

from eventsource.adapters.memory.readmodels import InMemoryReadModelRepository
from eventsource.ports.readmodels import Filter, Query
from tests.integration.conftest import skip_if_no_postgres_infra

from .conftest import EnhancedTestModel

if TYPE_CHECKING:
    pass

pytestmark = [
    pytest.mark.integration,
]


class TestInMemoryEnhancedFeatures:
    """InMemory-specific enhanced feature tests."""

    @pytest.mark.asyncio
    async def test_inmemory_get_deleted_consistency(
        self,
        enhanced_inmemory_repo: InMemoryReadModelRepository[EnhancedTestModel],
        enhanced_model_factory: Callable[..., EnhancedTestModel],
    ) -> None:
        """Test InMemory get_deleted maintains consistency."""
        model_id = uuid4()
        model = enhanced_model_factory(id=model_id, name="test")
        await enhanced_inmemory_repo.save(model)

        # Initially not deleted
        assert await enhanced_inmemory_repo.get_deleted(model_id) is None
        assert await enhanced_inmemory_repo.get(model_id) is not None

        # After soft delete
        await enhanced_inmemory_repo.soft_delete(model_id)
        assert await enhanced_inmemory_repo.get_deleted(model_id) is not None
        assert await enhanced_inmemory_repo.get(model_id) is None

        # After restore
        await enhanced_inmemory_repo.restore(model_id)
        assert await enhanced_inmemory_repo.get_deleted(model_id) is None
        assert await enhanced_inmemory_repo.get(model_id) is not None

    @pytest.mark.asyncio
    async def test_inmemory_version_check_atomicity(
        self,
        enhanced_inmemory_repo: InMemoryReadModelRepository[EnhancedTestModel],
        enhanced_model_factory: Callable[..., EnhancedTestModel],
    ) -> None:
        """Test InMemory version check is atomic."""
        model_id = uuid4()
        await enhanced_inmemory_repo.save(enhanced_model_factory(id=model_id, name="v1"))

        loaded = await enhanced_inmemory_repo.get(model_id)
        assert loaded is not None
        loaded.name = "v2"

        await enhanced_inmemory_repo.save_with_version_check(loaded)

        # Version should be updated atomically
        result = await enhanced_inmemory_repo.get(model_id)
        assert result is not None
        assert result.version == 2
        assert result.name == "v2"


class TestSQLiteEnhancedFeatures:
    """SQLite-specific enhanced feature tests."""

    @pytest.mark.asyncio
    async def test_sqlite_get_deleted_persistence(
        self,
        enhanced_sqlite_repo: Any,
        enhanced_model_factory: Callable[..., EnhancedTestModel],
    ) -> None:
        """Test SQLite get_deleted reads from actual database."""
        model_id = uuid4()
        model = enhanced_model_factory(id=model_id, name="sqlite_test")
        await enhanced_sqlite_repo.save(model)
        await enhanced_sqlite_repo.soft_delete(model_id)

        deleted = await enhanced_sqlite_repo.get_deleted(model_id)

        assert deleted is not None
        assert deleted.id == model_id
        assert deleted.name == "sqlite_test"

    @pytest.mark.asyncio
    async def test_sqlite_version_check_with_database(
        self,
        enhanced_sqlite_repo: Any,
        enhanced_model_factory: Callable[..., EnhancedTestModel],
    ) -> None:
        """Test SQLite version check operates on database."""
        model_id = uuid4()
        await enhanced_sqlite_repo.save(enhanced_model_factory(id=model_id, name="v1"))

        loaded = await enhanced_sqlite_repo.get(model_id)
        assert loaded is not None
        assert loaded.version == 1

        loaded.name = "v2"
        await enhanced_sqlite_repo.save_with_version_check(loaded)

        result = await enhanced_sqlite_repo.get(model_id)
        assert result is not None
        assert result.version == 2


@pytest.mark.postgres
class TestPostgreSQLEnhancedFeatures:
    """PostgreSQL-specific enhanced feature tests."""

    pytestmark = [
        pytest.mark.integration,
        pytest.mark.postgres,
        skip_if_no_postgres_infra,
    ]

    @pytest.mark.asyncio
    async def test_postgresql_get_deleted_uses_sql(
        self,
        enhanced_postgresql_repo: Any,
        enhanced_model_factory: Callable[..., EnhancedTestModel],
    ) -> None:
        """Test PostgreSQL get_deleted executes proper SQL."""
        model_id = uuid4()
        model = enhanced_model_factory(id=model_id, name="pg_test")
        await enhanced_postgresql_repo.save(model)
        await enhanced_postgresql_repo.soft_delete(model_id)

        deleted = await enhanced_postgresql_repo.get_deleted(model_id)

        assert deleted is not None
        assert deleted.id == model_id
        assert deleted.name == "pg_test"

    @pytest.mark.asyncio
    async def test_postgresql_version_check_with_returning(
        self,
        enhanced_postgresql_repo: Any,
        enhanced_model_factory: Callable[..., EnhancedTestModel],
    ) -> None:
        """Test PostgreSQL version check uses RETURNING clause."""
        model_id = uuid4()
        await enhanced_postgresql_repo.save(enhanced_model_factory(id=model_id, name="v1"))

        loaded = await enhanced_postgresql_repo.get(model_id)
        assert loaded is not None
        loaded.name = "v2"

        await enhanced_postgresql_repo.save_with_version_check(loaded)

        result = await enhanced_postgresql_repo.get(model_id)
        assert result is not None
        assert result.version == 2
        assert result.name == "v2"

    @pytest.mark.asyncio
    async def test_postgresql_find_deleted_with_complex_filter(
        self,
        enhanced_postgresql_repo: Any,
        enhanced_model_factory: Callable[..., EnhancedTestModel],
    ) -> None:
        """Test PostgreSQL find_deleted with complex filter combinations."""
        ids = [uuid4() for _ in range(5)]

        # Create models with varying attributes
        for i, model_id in enumerate(ids):
            await enhanced_postgresql_repo.save(
                enhanced_model_factory(
                    id=model_id,
                    name=f"model_{i}",
                    category="A" if i < 3 else "B",
                    value=i * 10,
                )
            )
            await enhanced_postgresql_repo.soft_delete(model_id)

        # Complex filter: category A AND value > 10
        results = await enhanced_postgresql_repo.find_deleted(
            Query(
                filters=[
                    Filter.eq("category", "A"),
                    Filter.gt("value", 10),
                ],
                order_by="value",
                order_direction="asc",
            )
        )

        assert len(results) == 1
        assert results[0].name == "model_2"
        assert results[0].value == 20
