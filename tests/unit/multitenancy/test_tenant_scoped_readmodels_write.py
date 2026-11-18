"""Write path of TenantScopedReadModelRepository (ADR-0114 T2), memory and SQLite."""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager
from uuid import UUID, uuid4

import aiosqlite
import pytest

from eventsource import (
    TenantContextNotSetError,
    TenantMismatchError,
    clear_tenant_context,
    tenant_scope,
)
from eventsource.adapters.memory.readmodels import InMemoryReadModelRepository
from eventsource.adapters.sql.readmodel_schema import generate_schema
from eventsource.adapters.sqlite.readmodels import SQLiteReadModelRepository
from eventsource.application.readmodels import TenantScopedReadModelRepository
from eventsource.domain.exceptions.tenant import TenantScopeOperationError
from eventsource.ports.readmodels import ReadModel, ReadModelRepository

BACKENDS = ["memory", pytest.param("sqlite", marks=pytest.mark.sqlite)]
TENANT_A, TENANT_B = uuid4(), uuid4()


class TenantNote(ReadModel):
    __table_name__ = "tenant_notes"
    tenant_id: UUID | None = None
    name: str = ""


@pytest.fixture(autouse=True)
def _clear_tenant() -> Iterator[None]:
    clear_tenant_context()
    yield
    clear_tenant_context()


@asynccontextmanager
async def _bare(backend: str) -> AsyncIterator[ReadModelRepository[TenantNote]]:
    """Yield an unwrapped repository of the named backend."""
    if backend == "memory":
        yield InMemoryReadModelRepository(TenantNote, enable_tracing=False)
        return
    async with aiosqlite.connect(":memory:") as conn:
        await conn.executescript(generate_schema(TenantNote, dialect="sqlite"))
        await conn.commit()
        yield SQLiteReadModelRepository(conn, TenantNote, enable_tracing=False)


@asynccontextmanager
async def _open(backend: str) -> AsyncIterator[TenantScopedReadModelRepository[TenantNote]]:
    """Yield the scoped wrapper over a fresh repository of the named backend."""
    async with _bare(backend) as repo:
        yield TenantScopedReadModelRepository(repo)


async def _seed_a(repo: TenantScopedReadModelRepository[TenantNote]) -> TenantNote:
    row = TenantNote(id=uuid4(), tenant_id=TENANT_A, name="original")
    async with tenant_scope(TENANT_A):
        await repo.save(row)
    return row


async def _unscoped_name(repo: TenantScopedReadModelRepository[TenantNote], id: UUID) -> str:
    found = await repo.get(id)
    return found.name if found is not None else ""


@pytest.mark.parametrize("backend", BACKENDS)
async def test_foreign_save_is_refused_and_row_is_unchanged(backend: str) -> None:
    async with _open(backend) as repo:
        row = await _seed_a(repo)
        intruder = TenantNote(id=row.id, tenant_id=TENANT_B, name="overwritten")
        async with tenant_scope(TENANT_B):
            with pytest.raises(TenantMismatchError):
                await repo.save(intruder)
        assert await _unscoped_name(repo, row.id) == "original"


@pytest.mark.parametrize("backend", BACKENDS)
async def test_foreign_save_is_refused_for_soft_deleted_row(backend: str) -> None:
    async with _open(backend) as repo:
        row = await _seed_a(repo)
        async with tenant_scope(TENANT_A):
            assert await repo.soft_delete(row.id)
        intruder = TenantNote(id=row.id, tenant_id=TENANT_B, name="overwritten")
        async with tenant_scope(TENANT_B):
            with pytest.raises(TenantMismatchError):
                await repo.save(intruder)
        assert await repo.get_deleted(row.id) is not None
        async with tenant_scope(TENANT_A):
            assert (found := await repo.get_deleted(row.id)) is not None
            assert found.name == "original"


@pytest.mark.parametrize("backend", BACKENDS)
async def test_owner_update_is_visible_to_owner(backend: str) -> None:
    async with _open(backend) as repo:
        row = await _seed_a(repo)
        async with tenant_scope(TENANT_A):
            await repo.save(row.model_copy(update={"name": "edited"}))
            assert (found := await repo.get(row.id)) is not None
            assert found.name == "edited"
        assert await _unscoped_name(repo, row.id) == "edited"


@pytest.mark.parametrize("backend", BACKENDS)
async def test_model_tenant_other_than_scope_is_refused(backend: str) -> None:
    async with _open(backend) as repo:
        stray = TenantNote(id=uuid4(), tenant_id=TENANT_B, name="stray")
        async with tenant_scope(TENANT_A):
            with pytest.raises(TenantMismatchError):
                await repo.save(stray)
            with pytest.raises(TenantMismatchError):
                await repo.save_many([stray])
        assert await repo.get(stray.id) is None


@pytest.mark.parametrize("backend", BACKENDS)
async def test_save_many_with_one_foreign_model_writes_nothing(backend: str) -> None:
    async with _open(backend) as repo:
        row = await _seed_a(repo)
        own = TenantNote(id=uuid4(), tenant_id=TENANT_A, name="new")
        foreign = TenantNote(id=row.id, tenant_id=TENANT_B, name="overwritten")
        async with tenant_scope(TENANT_A):
            with pytest.raises(TenantMismatchError):
                await repo.save_many([own, foreign])
        assert await repo.get(own.id) is None
        assert await _unscoped_name(repo, row.id) == "original"


@pytest.mark.parametrize("backend", BACKENDS)
async def test_foreign_row_cannot_be_deleted_soft_deleted_or_restored(backend: str) -> None:
    async with _open(backend) as repo:
        row = await _seed_a(repo)
        async with tenant_scope(TENANT_B):
            assert await repo.delete(row.id) is False
            assert await repo.soft_delete(row.id) is False
            assert await repo.restore(row.id) is False
        assert await _unscoped_name(repo, row.id) == "original"
        async with tenant_scope(TENANT_A):
            assert await repo.soft_delete(row.id) is True
        async with tenant_scope(TENANT_B):
            assert await repo.restore(row.id) is False
        async with tenant_scope(TENANT_A):
            assert await repo.get_deleted(row.id) is not None
            assert await repo.restore(row.id) is True


@pytest.mark.parametrize("backend", BACKENDS)
async def test_truncate_is_refused_under_any_scope(backend: str) -> None:
    async with _open(backend) as repo:
        row = await _seed_a(repo)
        for tenant in (TENANT_A, TENANT_B):
            async with tenant_scope(tenant):
                with pytest.raises(TenantScopeOperationError):
                    await repo.truncate()
        assert await _unscoped_name(repo, row.id) == "original"
        assert await repo.truncate() == 1


@pytest.mark.parametrize("backend", BACKENDS)
async def test_version_check_save_refuses_foreign_id(backend: str) -> None:
    async with _open(backend) as repo:
        row = await _seed_a(repo)
        intruder = TenantNote(id=row.id, tenant_id=TENANT_B, name="overwritten", version=1)
        async with tenant_scope(TENANT_B):
            with pytest.raises(TenantMismatchError):
                await repo.save_with_version_check(intruder)
        assert await _unscoped_name(repo, row.id) == "original"


@pytest.mark.parametrize("backend", BACKENDS)
async def test_required_context_refuses_write_without_scope(backend: str) -> None:
    async with _bare(backend) as bare:
        repo = TenantScopedReadModelRepository(bare, require_tenant_context=True)
        with pytest.raises(TenantContextNotSetError):
            await repo.save(TenantNote(id=uuid4(), tenant_id=TENANT_A))
        with pytest.raises(TenantContextNotSetError):
            await repo.truncate()


@pytest.mark.parametrize("backend", BACKENDS)
async def test_unscoped_writes_pass_through_unchanged(backend: str) -> None:
    async with _open(backend) as repo:
        row = await _seed_a(repo)
        other = TenantNote(id=row.id, tenant_id=TENANT_B, name="unscoped-overwrite")
        await repo.save(other)
        assert await _unscoped_name(repo, row.id) == "unscoped-overwrite"
        assert await repo.soft_delete(row.id) is True
        assert await repo.truncate() == 1
