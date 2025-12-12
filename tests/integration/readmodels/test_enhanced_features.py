"""
Integration tests for enhanced read model features.

Tests soft delete helpers and optimistic locking across all backends:
- InMemory (always available)
- SQLite (always available)
- PostgreSQL (requires testcontainers/Docker)

Task: P4-003 - Create Enhanced Features Tests
"""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import pytest
import pytest_asyncio

from eventsource.readmodels import (
    Filter,
    InMemoryReadModelRepository,
    OptimisticLockError,
    Query,
    ReadModel,
    ReadModelNotFoundError,
    generate_schema,
)

from ..conftest import skip_if_no_postgres_infra

if TYPE_CHECKING:
    from uuid import UUID


pytestmark = [
    pytest.mark.integration,
]


# ============================================================================
# Test Read Model
# ============================================================================


class EnhancedTestModel(ReadModel):
    """Test model for enhanced feature integration tests."""

    name: str
    category: str = "default"
    value: int = 0
    amount: Decimal = Decimal("0.00")


# ============================================================================
# Fixtures
# ============================================================================


@pytest_asyncio.fixture
async def enhanced_inmemory_repo() -> InMemoryReadModelRepository[EnhancedTestModel]:
    """In-memory repository for enhanced feature tests."""
    repo: InMemoryReadModelRepository[EnhancedTestModel] = InMemoryReadModelRepository(
        EnhancedTestModel, enable_tracing=False
    )
    yield repo
    await repo.clear()


@pytest_asyncio.fixture
async def enhanced_sqlite_repo(tmp_path: Any) -> Any:
    """SQLite repository for enhanced feature tests."""
    try:
        import aiosqlite
    except ImportError:
        pytest.skip("aiosqlite not installed")

    from eventsource.readmodels import SQLiteReadModelRepository

    db_path = tmp_path / "test_enhanced_features.db"
    async with aiosqlite.connect(db_path) as db:
        # Create table using schema generation utility
        schema_sql = generate_schema(EnhancedTestModel, dialect="sqlite", if_not_exists=False)
        await db.execute(schema_sql)
        await db.commit()

        repo: SQLiteReadModelRepository[EnhancedTestModel] = SQLiteReadModelRepository(
            db, EnhancedTestModel, enable_tracing=False
        )
        yield repo
        await repo.truncate()


@pytest_asyncio.fixture
async def enhanced_postgresql_repo(postgres_engine: Any) -> Any:
    """PostgreSQL repository for enhanced feature tests."""
    from sqlalchemy import text

    from eventsource.readmodels import PostgreSQLReadModelRepository

    # Create table using schema generation utility
    async with postgres_engine.begin() as conn:
        await conn.execute(text(f"DROP TABLE IF EXISTS {EnhancedTestModel.table_name()}"))
        schema_sql = generate_schema(EnhancedTestModel, dialect="postgresql", if_not_exists=False)
        await conn.execute(text(schema_sql))

    # Create repository with engine
    repo: PostgreSQLReadModelRepository[EnhancedTestModel] = PostgreSQLReadModelRepository(
        postgres_engine, EnhancedTestModel, enable_tracing=False
    )
    yield repo
    await repo.truncate()


@pytest.fixture
def enhanced_model_factory() -> Callable[..., EnhancedTestModel]:
    """Factory for creating test EnhancedTestModel instances."""

    def _create(
        id: UUID | None = None,
        name: str = "test_model",
        category: str = "default",
        value: int = 0,
        amount: Decimal = Decimal("0.00"),
    ) -> EnhancedTestModel:
        return EnhancedTestModel(
            id=id or uuid4(),
            name=name,
            category=category,
            value=value,
            amount=amount,
        )

    return _create


@pytest.fixture(params=["inmemory", "sqlite", "postgresql"])
def enhanced_repo_type(request: pytest.FixtureRequest) -> str:
    """Return the repository type for parametrization."""
    return request.param


@pytest_asyncio.fixture
async def enhanced_repo(
    enhanced_repo_type: str,
    enhanced_inmemory_repo: InMemoryReadModelRepository[EnhancedTestModel],
    enhanced_sqlite_repo: Any,
    enhanced_postgresql_repo: Any,
    request: pytest.FixtureRequest,
) -> Any:
    """Parametrized fixture providing all repository implementations."""
    if enhanced_repo_type == "inmemory":
        yield enhanced_inmemory_repo
    elif enhanced_repo_type == "sqlite":
        yield enhanced_sqlite_repo
    elif enhanced_repo_type == "postgresql":
        try:
            yield enhanced_postgresql_repo
        except pytest.skip.Exception:
            pytest.skip("PostgreSQL test infrastructure not available")
    else:
        pytest.fail(f"Unknown repository type: {enhanced_repo_type}")


# ============================================================================
# Soft Delete Helper Tests
# ============================================================================


class TestSoftDeleteHelpers:
    """Tests for soft delete helper methods."""

    @pytest.mark.asyncio
    async def test_get_deleted_returns_deleted_record(
        self,
        enhanced_repo: Any,
        enhanced_model_factory: Callable[..., EnhancedTestModel],
    ) -> None:
        """Test get_deleted returns soft-deleted records."""
        model_id = uuid4()
        model = enhanced_model_factory(id=model_id, name="will_delete")
        await enhanced_repo.save(model)
        await enhanced_repo.soft_delete(model_id)

        deleted = await enhanced_repo.get_deleted(model_id)

        assert deleted is not None
        assert deleted.id == model_id
        assert deleted.name == "will_delete"
        assert deleted.deleted_at is not None

    @pytest.mark.asyncio
    async def test_get_deleted_returns_none_for_active(
        self,
        enhanced_repo: Any,
        enhanced_model_factory: Callable[..., EnhancedTestModel],
    ) -> None:
        """Test get_deleted returns None for active records."""
        model_id = uuid4()
        model = enhanced_model_factory(id=model_id, name="active")
        await enhanced_repo.save(model)

        deleted = await enhanced_repo.get_deleted(model_id)

        assert deleted is None

    @pytest.mark.asyncio
    async def test_get_deleted_returns_none_for_nonexistent(
        self,
        enhanced_repo: Any,
    ) -> None:
        """Test get_deleted returns None for nonexistent records."""
        deleted = await enhanced_repo.get_deleted(uuid4())
        assert deleted is None

    @pytest.mark.asyncio
    async def test_find_deleted_returns_only_deleted(
        self,
        enhanced_repo: Any,
        enhanced_model_factory: Callable[..., EnhancedTestModel],
    ) -> None:
        """Test find_deleted returns only soft-deleted records."""
        active_ids = [uuid4() for _ in range(2)]
        deleted_ids = [uuid4() for _ in range(3)]

        for i, model_id in enumerate(active_ids):
            await enhanced_repo.save(enhanced_model_factory(id=model_id, name=f"active_{i}"))

        for i, model_id in enumerate(deleted_ids):
            await enhanced_repo.save(enhanced_model_factory(id=model_id, name=f"deleted_{i}"))
            await enhanced_repo.soft_delete(model_id)

        results = await enhanced_repo.find_deleted()

        assert len(results) == 3
        result_ids = {r.id for r in results}
        assert result_ids == set(deleted_ids)

    @pytest.mark.asyncio
    async def test_find_deleted_with_filter(
        self,
        enhanced_repo: Any,
        enhanced_model_factory: Callable[..., EnhancedTestModel],
    ) -> None:
        """Test find_deleted with filters."""
        model1_id = uuid4()
        model2_id = uuid4()

        await enhanced_repo.save(enhanced_model_factory(id=model1_id, name="del1", category="A"))
        await enhanced_repo.save(enhanced_model_factory(id=model2_id, name="del2", category="B"))
        await enhanced_repo.soft_delete(model1_id)
        await enhanced_repo.soft_delete(model2_id)

        results = await enhanced_repo.find_deleted(Query(filters=[Filter.eq("category", "A")]))

        assert len(results) == 1
        assert results[0].category == "A"

    @pytest.mark.asyncio
    async def test_find_deleted_with_pagination(
        self,
        enhanced_repo: Any,
        enhanced_model_factory: Callable[..., EnhancedTestModel],
    ) -> None:
        """Test find_deleted with pagination."""
        for i in range(5):
            model_id = uuid4()
            await enhanced_repo.save(enhanced_model_factory(id=model_id, name=f"del_{i}"))
            await enhanced_repo.soft_delete(model_id)

        results = await enhanced_repo.find_deleted(Query(limit=2))

        assert len(results) == 2

    @pytest.mark.asyncio
    async def test_find_deleted_empty_when_none_deleted(
        self,
        enhanced_repo: Any,
        enhanced_model_factory: Callable[..., EnhancedTestModel],
    ) -> None:
        """Test find_deleted returns empty list when no deletions."""
        await enhanced_repo.save(enhanced_model_factory(name="active"))

        results = await enhanced_repo.find_deleted()

        assert results == []

    @pytest.mark.asyncio
    async def test_find_deleted_with_ordering(
        self,
        enhanced_repo: Any,
        enhanced_model_factory: Callable[..., EnhancedTestModel],
    ) -> None:
        """Test find_deleted with ordering."""
        id1 = uuid4()
        id2 = uuid4()
        id3 = uuid4()

        await enhanced_repo.save(enhanced_model_factory(id=id1, name="alpha", value=30))
        await enhanced_repo.save(enhanced_model_factory(id=id2, name="gamma", value=10))
        await enhanced_repo.save(enhanced_model_factory(id=id3, name="beta", value=20))

        await enhanced_repo.soft_delete(id1)
        await enhanced_repo.soft_delete(id2)
        await enhanced_repo.soft_delete(id3)

        # Test ascending order by name
        results_asc = await enhanced_repo.find_deleted(
            Query(order_by="name", order_direction="asc")
        )

        assert len(results_asc) == 3
        assert results_asc[0].name == "alpha"
        assert results_asc[1].name == "beta"
        assert results_asc[2].name == "gamma"

        # Test descending order by value
        results_desc = await enhanced_repo.find_deleted(
            Query(order_by="value", order_direction="desc")
        )

        assert len(results_desc) == 3
        assert results_desc[0].value == 30
        assert results_desc[1].value == 20
        assert results_desc[2].value == 10


# ============================================================================
# Optimistic Locking Tests
# ============================================================================


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

        with pytest.raises(OptimisticLockError) as exc_info:
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
        """Test OptimisticLockError has descriptive message."""
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

        with pytest.raises(OptimisticLockError) as exc_info:
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


# ============================================================================
# Exception Tests
# ============================================================================


class TestExceptions:
    """Tests for read model exceptions."""

    def test_optimistic_lock_error_with_actual_version(self) -> None:
        """Test OptimisticLockError with actual version."""
        model_id = uuid4()
        error = OptimisticLockError(model_id, expected_version=1, actual_version=2)

        assert error.model_id == model_id
        assert error.expected_version == 1
        assert error.actual_version == 2
        assert "1" in str(error)
        assert "2" in str(error)

    def test_optimistic_lock_error_without_actual_version(self) -> None:
        """Test OptimisticLockError without actual version."""
        model_id = uuid4()
        error = OptimisticLockError(model_id, expected_version=1)

        assert error.actual_version is None
        assert "1" in str(error)

    def test_read_model_not_found_error(self) -> None:
        """Test ReadModelNotFoundError."""
        model_id = uuid4()
        error = ReadModelNotFoundError(model_id)

        assert error.model_id == model_id
        assert str(model_id) in str(error)


# ============================================================================
# Combined Feature Tests
# ============================================================================


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
        with pytest.raises(OptimisticLockError):
            await enhanced_repo.save_with_version_check(client2)


# ============================================================================
# Backend-Specific Tests
# ============================================================================


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
