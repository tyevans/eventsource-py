"""Integration tests specific to individual repository backends (PostgreSQL, SQLite, InMemory)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from uuid import uuid4

import pytest

from tests.integration.conftest import skip_if_no_postgres_infra

if TYPE_CHECKING:
    from collections.abc import Callable

    from .conftest import OrderSummary

pytestmark = [
    pytest.mark.integration,
]


@pytest.mark.postgres
class TestPostgreSQLReadModelRepository:
    """Tests specific to PostgreSQL repository."""

    pytestmark = [
        pytest.mark.integration,
        pytest.mark.postgres,
        skip_if_no_postgres_infra,
    ]

    @pytest.mark.asyncio
    async def test_postgresql_basic_save_and_get(
        self,
        postgresql_repo: Any,
        order_summary_factory: Callable[..., OrderSummary],
    ) -> None:
        """Test basic PostgreSQL operations work correctly."""
        order = order_summary_factory(order_number="PG-TEST-001")

        await postgresql_repo.save(order)
        result = await postgresql_repo.get(order.id)

        assert result is not None
        assert result.order_number == "PG-TEST-001"


class TestSQLiteReadModelRepository:
    """Tests specific to SQLite repository."""

    @pytest.mark.asyncio
    async def test_sqlite_basic_save_and_get(
        self,
        sqlite_repo: Any,
        order_summary_factory: Callable[..., OrderSummary],
    ) -> None:
        """Test basic SQLite operations work correctly."""
        order = order_summary_factory(order_number="SQLITE-TEST-001")

        await sqlite_repo.save(order)
        result = await sqlite_repo.get(order.id)

        assert result is not None
        assert result.order_number == "SQLITE-TEST-001"

    @pytest.mark.asyncio
    async def test_sqlite_uuid_conversion(
        self,
        sqlite_repo: Any,
        order_summary_factory: Callable[..., OrderSummary],
    ) -> None:
        """Test that UUIDs are properly stored and retrieved in SQLite (as TEXT)."""
        specific_id = uuid4()
        order = order_summary_factory(id=specific_id)

        await sqlite_repo.save(order)
        result = await sqlite_repo.get(specific_id)

        assert result is not None
        assert result.id == specific_id
        assert isinstance(result.id, type(specific_id))


class TestInMemoryReadModelRepository:
    """Tests specific to InMemory repository."""

    @pytest.mark.asyncio
    async def test_inmemory_basic_save_and_get(
        self,
        inmemory_repo: Any,
        order_summary_factory: Callable[..., OrderSummary],
    ) -> None:
        """Test basic InMemory operations work correctly."""
        order = order_summary_factory(order_number="MEM-TEST-001")

        await inmemory_repo.save(order)
        result = await inmemory_repo.get(order.id)

        assert result is not None
        assert result.order_number == "MEM-TEST-001"

    @pytest.mark.asyncio
    async def test_inmemory_clear(
        self,
        inmemory_repo: Any,
        order_summary_factory: Callable[..., OrderSummary],
    ) -> None:
        """Test the clear() convenience method."""
        for i in range(3):
            await inmemory_repo.save(order_summary_factory(order_number=f"ORD-{i}"))

        await inmemory_repo.clear()

        assert await inmemory_repo.count() == 0
