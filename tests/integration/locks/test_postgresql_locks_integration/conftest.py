"""Fixtures for PostgreSQL advisory lock integration tests."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest_asyncio

from eventsource.adapters.postgresql.locks import PostgreSQLLockManager

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker


@pytest_asyncio.fixture
async def lock_manager(
    postgres_session_factory: async_sessionmaker[AsyncSession],
) -> PostgreSQLLockManager:
    """Provide a PostgreSQLLockManager for testing."""
    return PostgreSQLLockManager(
        postgres_session_factory,
        holder_id="test-worker",
        enable_tracing=False,
    )


@pytest_asyncio.fixture
async def second_lock_manager(
    postgres_session_factory: async_sessionmaker[AsyncSession],
) -> PostgreSQLLockManager:
    """Provide a second PostgreSQLLockManager for concurrent testing."""
    return PostgreSQLLockManager(
        postgres_session_factory,
        holder_id="test-worker-2",
        enable_tracing=False,
    )
