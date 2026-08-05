"""Conformance suite for the `ReadModelRepository` port.

Subclass and provide a `store` fixture yielding a repository already bound
to `ConformanceReadModel` with its table provisioned -- bindings own
provisioning so this module stays adapter-free and dialect-free.

**Not pinned here**, deliberately: the ordering of `get_many` results (the
Protocol does not guarantee one), the resolution of `updated_at`, and any
dialect-specific type coercion. Those stay in the per-backend test modules.

**Pinned here:** the repository owns its stored models. A read returns an
object the caller may mutate freely without reaching storage, and a write
takes a copy rather than adopting -- or mutating -- the caller's object.
This was an in-memory-versus-SQL divergence before it was a contract: the
SQL adapters hydrate a fresh instance per read and bump `version` in the
row, while the in-memory adapter handed out its live dict entry and bumped
`version` on the caller's object. The two cases below pin both directions
for every backend.
"""

import asyncio
from abc import ABC, abstractmethod
from uuid import uuid4

import pytest

from eventsource.ports.readmodels import (
    Filter,
    OptimisticLockError,
    Query,
    ReadModelNotFoundError,
    ReadModelRepository,
)
from eventsource.testing.conformance_ports._fixtures import ConformanceReadModel

Repo = ReadModelRepository[ConformanceReadModel]


def _model(name: str = "conformance", count: int = 0) -> ConformanceReadModel:
    return ConformanceReadModel(id=uuid4(), name=name, count=count)


class ReadModelRepositoryConformance(ABC):
    """Conformance suite for `ReadModelRepository` implementations."""

    @abstractmethod
    @pytest.fixture
    def store(self) -> object:
        """Yield a repository bound to `ConformanceReadModel`, table provisioned."""
        raise NotImplementedError

    async def test_save_then_get_round_trips(self, store: Repo) -> None:
        model = _model(name="alpha", count=3)
        await store.save(model)
        loaded = await store.get(model.id)
        assert loaded is not None
        assert loaded.id == model.id
        assert loaded.name == "alpha"
        assert loaded.count == 3

    async def test_get_of_an_absent_id_returns_none(self, store: Repo) -> None:
        assert await store.get(uuid4()) is None

    async def test_save_upserts_and_advances_updated_at(self, store: Repo) -> None:
        model = _model(name="before")
        await store.save(model)
        first = await store.get(model.id)
        assert first is not None

        await asyncio.sleep(0.01)
        first.name = "after"
        await store.save(first)

        second = await store.get(model.id)
        assert second is not None
        assert second.name == "after"
        assert second.updated_at >= first.updated_at
        assert await store.count() == 1

    async def test_get_many_skips_missing_ids(self, store: Repo) -> None:
        present = _model(name="present")
        await store.save(present)
        found = await store.get_many([present.id, uuid4()])
        assert [m.id for m in found] == [present.id]

    async def test_save_many_persists_every_model(self, store: Repo) -> None:
        models = [_model(name=f"m{i}", count=i) for i in range(3)]
        await store.save_many(models)
        for model in models:
            assert await store.get(model.id) is not None

    async def test_exists_reflects_presence(self, store: Repo) -> None:
        model = _model()
        assert await store.exists(model.id) is False
        await store.save(model)
        assert await store.exists(model.id) is True

    async def test_delete_returns_whether_a_row_was_removed(self, store: Repo) -> None:
        model = _model()
        await store.save(model)
        assert await store.delete(model.id) is True
        assert await store.get(model.id) is None
        assert await store.delete(model.id) is False

    async def test_soft_delete_hides_from_get_but_not_from_get_deleted(self, store: Repo) -> None:
        model = _model()
        await store.save(model)
        assert await store.soft_delete(model.id) is True
        assert await store.get(model.id) is None
        recovered = await store.get_deleted(model.id)
        assert recovered is not None
        assert recovered.id == model.id

    async def test_restore_makes_a_soft_deleted_model_visible_again(self, store: Repo) -> None:
        model = _model()
        await store.save(model)
        await store.soft_delete(model.id)
        assert await store.restore(model.id) is True
        assert await store.get(model.id) is not None

    async def test_find_deleted_returns_only_soft_deleted_models(self, store: Repo) -> None:
        live = _model(name="live")
        gone = _model(name="gone")
        await store.save_many([live, gone])
        await store.soft_delete(gone.id)
        assert [m.id for m in await store.find_deleted()] == [gone.id]

    async def test_find_filters_on_equality(self, store: Repo) -> None:
        alpha = _model(name="alpha")
        beta = _model(name="beta")
        await store.save_many([alpha, beta])
        found = await store.find(Query(filters=[Filter.eq("name", "alpha")]))
        assert [m.id for m in found] == [alpha.id]

    async def test_find_orders_and_limits(self, store: Repo) -> None:
        await store.save_many([_model(name=f"m{i}", count=i) for i in range(3)])
        found = await store.find(Query(order_by="count", order_direction="desc", limit=2))
        assert [m.count for m in found] == [2, 1]

    async def test_find_excludes_soft_deleted_models(self, store: Repo) -> None:
        live = _model(name="live")
        gone = _model(name="gone")
        await store.save_many([live, gone])
        await store.soft_delete(gone.id)
        assert [m.id for m in await store.find()] == [live.id]

    async def test_count_with_and_without_filters(self, store: Repo) -> None:
        await store.save_many([_model(name="alpha"), _model(name="beta")])
        assert await store.count() == 2
        assert await store.count(Query(filters=[Filter.eq("name", "alpha")])) == 1

    async def test_truncate_returns_the_count_and_empties_the_table(self, store: Repo) -> None:
        live = _model(name="live")
        gone = _model(name="gone")
        await store.save_many([live, gone])
        await store.soft_delete(gone.id)
        assert await store.truncate() == 2
        assert await store.count() == 0
        assert await store.get_deleted(gone.id) is None

    async def test_save_with_version_check_increments_version(self, store: Repo) -> None:
        model = _model()
        await store.save(model)
        loaded = await store.get(model.id)
        assert loaded is not None
        before = loaded.version
        loaded.name = "bumped"
        await store.save_with_version_check(loaded)
        after = await store.get(model.id)
        assert after is not None
        assert after.version == before + 1

    async def test_save_with_version_check_rejects_a_stale_version(self, store: Repo) -> None:
        model = _model()
        await store.save(model)
        first = await store.get(model.id)
        assert first is not None
        # Two independent readers of the same version, both from get() --
        # which is only a valid way to build this scenario because reads
        # are pinned below to return unaliased objects.
        second = await store.get(model.id)
        assert second is not None

        first.name = "winner"
        await store.save_with_version_check(first)

        second.name = "loser"
        with pytest.raises(OptimisticLockError):
            await store.save_with_version_check(second)

    async def test_save_with_version_check_rejects_an_absent_model(self, store: Repo) -> None:
        with pytest.raises(ReadModelNotFoundError):
            await store.save_with_version_check(_model())

    async def test_reads_do_not_alias_stored_state(self, store: Repo) -> None:
        """Mutating a read result must not reach storage."""
        model = _model(name="stored")
        await store.save(model)

        loaded = await store.get(model.id)
        assert loaded is not None
        loaded.name = "mutated locally"

        again = await store.get(model.id)
        assert again is not None
        assert again.name == "stored"

        [from_many] = await store.get_many([model.id])
        assert from_many.name == "stored"
        [from_find] = await store.find()
        assert from_find.name == "stored"

    async def test_writes_do_not_mutate_the_callers_model(self, store: Repo) -> None:
        """A saved object stays the caller's, unchanged by later writes."""
        model = _model(name="first")
        await store.save(model)
        version_after_save = model.version

        # A second, unrelated write against the same id.
        update = await store.get(model.id)
        assert update is not None
        update.name = "second"
        await store.save(update)

        # The caller's original object is untouched by both writes.
        assert model.name == "first"
        assert model.version == version_after_save


__all__ = ["ReadModelRepositoryConformance"]
