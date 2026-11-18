"""Read path of TenantScopedReadModelRepository (ADR-0114 T1), memory and SQLite."""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager
from uuid import UUID, uuid4

import aiosqlite
import pytest

from eventsource import TenantContextNotSetError, clear_tenant_context, tenant_scope
from eventsource.adapters.memory.readmodels import InMemoryReadModelRepository
from eventsource.adapters.sql.readmodel_schema import generate_schema
from eventsource.adapters.sqlite.readmodels import SQLiteReadModelRepository
from eventsource.application.readmodels import TenantScopedReadModelRepository
from eventsource.ports.readmodels import ReadModel


class TenantNote(ReadModel):
    __table_name__ = "tenant_notes"
    tenant_id: UUID | None = None
    name: str = ""


class UntenantedNote(ReadModel):
    __table_name__ = "untenanted_notes"
    name: str = ""


BACKENDS = ["memory", pytest.param("sqlite", marks=pytest.mark.sqlite)]
TENANT_A, TENANT_B = uuid4(), uuid4()


@pytest.fixture(autouse=True)
def _clear_tenant() -> Iterator[None]:
    clear_tenant_context()
    yield
    clear_tenant_context()


@asynccontextmanager
async def _open[M: ReadModel](
    backend: str, model: type[M]
) -> AsyncIterator[TenantScopedReadModelRepository[M]]:
    """Yield a scoped wrapper over a fresh repository of the named backend."""
    if backend == "memory":
        yield TenantScopedReadModelRepository(
            InMemoryReadModelRepository(model, enable_tracing=False)
        )
        return
    async with aiosqlite.connect(":memory:") as conn:
        await conn.executescript(generate_schema(model, dialect="sqlite"))
        await conn.commit()
        sqlite_repo = SQLiteReadModelRepository(conn, model, enable_tracing=False)
        yield TenantScopedReadModelRepository(sqlite_repo)


async def _seed(repo: TenantScopedReadModelRepository[TenantNote]) -> dict[str, TenantNote]:
    """Save two rows per tenant and one row with no tenant, all unscoped."""
    rows = {
        "a1": TenantNote(id=uuid4(), tenant_id=TENANT_A, name="a1"),
        "a2": TenantNote(id=uuid4(), tenant_id=TENANT_A, name="a2"),
        "b1": TenantNote(id=uuid4(), tenant_id=TENANT_B, name="b1"),
        "none": TenantNote(id=uuid4(), tenant_id=None, name="none"),
    }
    inner = getattr(repo, "repository", repo)  # seed through the bare repository
    for row in rows.values():
        await inner.save(row)
    return rows


@pytest.mark.parametrize("backend", BACKENDS)
async def test_get_returns_own_row_and_none_for_other_tenant(backend: str) -> None:
    async with _open(backend, TenantNote) as repo:
        rows = await _seed(repo)
        async with tenant_scope(TENANT_A):
            assert (found := await repo.get(rows["a1"].id)) is not None and found.name == "a1"
        async with tenant_scope(TENANT_B):
            assert await repo.get(rows["a1"].id) is None


@pytest.mark.parametrize("backend", BACKENDS)
async def test_collection_reads_exclude_other_tenant(backend: str) -> None:
    async with _open(backend, TenantNote) as repo:
        rows = await _seed(repo)
        ids = [rows["a1"].id, rows["b1"].id]
        async with tenant_scope(TENANT_B):
            assert not await repo.exists(rows["a1"].id)
            assert await repo.exists(rows["b1"].id)
            assert await repo.count() == 1
            assert [m.name for m in await repo.find()] == ["b1"]
            assert [m.name for m in await repo.get_many(ids)] == ["b1"]
            assert await repo.find_one(id=rows["a1"].id) is None
            assert await repo.find_one(id=rows["b1"].id) is not None
        async with tenant_scope(TENANT_A):
            assert await repo.count(name="a2") == 1
            assert [m.name for m in await repo.find(name="a2")] == ["a2"]
            assert await repo.count() == 2


@pytest.mark.parametrize("backend", BACKENDS)
async def test_row_without_tenant_is_invisible_under_any_scope(backend: str) -> None:
    async with _open(backend, TenantNote) as repo:
        rows = await _seed(repo)
        for tenant in (TENANT_A, TENANT_B):
            async with tenant_scope(tenant):
                assert await repo.get(rows["none"].id) is None
                assert not await repo.exists(rows["none"].id)
                assert await repo.count(name="none") == 0


@pytest.mark.parametrize("backend", BACKENDS)
async def test_deleted_reads_are_scoped(backend: str) -> None:
    async with _open(backend, TenantNote) as repo:
        rows = await _seed(repo)
        assert await repo.soft_delete(rows["a1"].id)
        async with tenant_scope(TENANT_B):
            assert await repo.get_deleted(rows["a1"].id) is None
            assert await repo.find_deleted() == []
        async with tenant_scope(TENANT_A):
            assert (
                found := await repo.get_deleted(rows["a1"].id)
            ) is not None and found.name == "a1"
            assert [m.name for m in await repo.find_deleted()] == ["a1"]


@pytest.mark.parametrize("backend", BACKENDS)
async def test_unscoped_reads_pass_through_unchanged(backend: str) -> None:
    async with _open(backend, TenantNote) as repo:
        rows = await _seed(repo)
        assert (found := await repo.get(rows["a1"].id)) is not None and found.name == "a1"
        assert await repo.get(rows["none"].id) is not None
        assert await repo.exists(rows["b1"].id)
        assert await repo.count() == 4
        assert len(await repo.get_many([rows["a1"].id, rows["none"].id])) == 2


@pytest.mark.parametrize("backend", BACKENDS)
async def test_required_context_without_scope_raises(backend: str) -> None:
    async with _open(backend, TenantNote) as scoped:
        repo = TenantScopedReadModelRepository(scoped.repository, require_tenant_context=True)
        rows = await _seed(repo)
        with pytest.raises(TenantContextNotSetError):
            await repo.get(rows["a1"].id)
        async with tenant_scope(TENANT_A):
            assert (found := await repo.get(rows["a1"].id)) is not None and found.name == "a1"


@pytest.mark.parametrize("backend", BACKENDS)
async def test_model_without_tenant_field_raises_under_scope(backend: str) -> None:
    async with _open(backend, UntenantedNote) as repo:
        row = UntenantedNote(id=uuid4(), name="plain")
        await repo.save(row)
        async with tenant_scope(TENANT_A):
            with pytest.raises(ValueError):
                await repo.find(name="plain")
            with pytest.raises(ValueError):
                await repo.count()


@pytest.mark.parametrize("backend", BACKENDS)
async def test_caller_tenant_filter_is_refused_under_scope(backend: str) -> None:
    async with _open(backend, TenantNote) as repo:
        await _seed(repo)
        async with tenant_scope(TENANT_A):
            with pytest.raises(ValueError, match="tenant_id"):
                await repo.find(tenant_id=TENANT_B)
