"""Conformance tests for PostgreSQLLockManager against the DistributedLock port suite."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

import pytest
import pytest_asyncio

from eventsource.adapters.postgresql.locks import PostgreSQLLockManager
from eventsource.testing.conformance_ports import DistributedLockConformance

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

pytestmark = [pytest.mark.integration, pytest.mark.postgres]


class TestPostgreSQLLockManager(DistributedLockConformance):
    @pytest_asyncio.fixture
    async def store(
        self, postgres_session_factory: async_sessionmaker[AsyncSession]
    ) -> AsyncIterator[PostgreSQLLockManager]:
        manager = PostgreSQLLockManager(
            postgres_session_factory,
            holder_id="conformance",
            enable_tracing=False,
        )
        yield manager
        await manager.release_all()
