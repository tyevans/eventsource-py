"""Conformance suite for the `ReadModelRepository` port."""

import asyncio
from abc import ABC, abstractmethod
from uuid import uuid4

import pytest

from eventsource.ports import readmodels as _rm
from eventsource.ports.readmodels import ReadModelRepository
from eventsource.testing.conformance_ports._fixtures import ConformanceReadModel
from eventsource.testing.conformance_ports.readmodels_query import (
    ReadModelQueryConformance,
    _model,
)

Repo = ReadModelRepository[ConformanceReadModel]


class ReadModelRepositoryConformance(ReadModelQueryConformance, ABC):
    """Conformance suite for `ReadModelRepository` implementations."""

    @abstractmethod
    @pytest.fixture
    def store(self) -> object:
        raise NotImplementedError  # fmt: skip

    async def test_save_then_get_round_trips(self, store: Repo) -> None:
        await store.save(model := _model(name="alpha", count=3))
        assert (loaded := await store.get(model.id)) is not None and (loaded.id, loaded.name, loaded.count) == (model.id, "alpha", 3)  # fmt: skip

    async def test_get_of_an_absent_id_returns_none(self, store: Repo) -> None:
        assert await store.get(uuid4()) is None  # fmt: skip

    async def test_save_upserts_and_advances_updated_at(self, store: Repo) -> None:
        await store.save(model := _model(name="before"))
        assert (first := await store.get(model.id)) is not None and await asyncio.sleep(0.01, True)
        first.name = "after"
        await store.save(first)
        assert (sec := await store.get(model.id)) is not None and (sec.name, await store.count()) == ("after", 1) and sec.updated_at >= first.updated_at  # fmt: skip

    async def test_get_many_skips_missing_ids(self, store: Repo) -> None:
        await store.save(present := _model(name="present"))
        assert [m.id for m in await store.get_many([present.id, uuid4()])] == [present.id]

    async def test_save_many_persists_every_model(self, store: Repo) -> None:
        await store.save_many(models := [_model(name=f"m{i}", count=i) for i in range(3)])
        assert [await store.get(m.id) is not None for m in models] == [True, True, True]

    async def test_exists_reflects_presence(self, store: Repo) -> None:
        assert await store.exists((model := _model()).id) is False
        await store.save(model)
        assert await store.exists(model.id) is True

    async def test_delete_returns_whether_a_row_was_removed(self, store: Repo) -> None:
        await store.save(model := _model())
        assert await store.delete(model.id) is True and await store.get(model.id) is None and await store.delete(model.id) is False  # fmt: skip

    async def test_soft_delete_hides_from_get_but_not_from_get_deleted(self, store: Repo) -> None:
        await store.save(model := _model())
        assert await store.soft_delete(model.id) is True and await store.get(model.id) is None and (rec := await store.get_deleted(model.id)) is not None and rec.id == model.id  # fmt: skip

    async def test_restore_makes_a_soft_deleted_model_visible_again(self, store: Repo) -> None:
        await store.save(model := _model())
        assert await store.soft_delete(model.id) and await store.restore(model.id) and await store.get(model.id) is not None  # fmt: skip

    async def test_find_deleted_returns_only_soft_deleted_models(self, store: Repo) -> None:
        await store.save_many([_model(name="live"), gone := _model(name="gone")])
        assert await store.soft_delete(gone.id) and [m.id for m in await store.find_deleted()] == [gone.id]  # fmt: skip

    async def test_truncate_returns_the_count_and_empties_the_table(self, store: Repo) -> None:
        await store.save_many([_model(name="live"), gone := _model(name="gone")])
        assert await store.soft_delete(gone.id) and await store.truncate() == 2 and await store.count() == 0 and await store.get_deleted(gone.id) is None  # fmt: skip

    async def test_save_with_version_check_increments_version(self, store: Repo) -> None:
        await store.save(model := _model())
        assert (loaded := await store.get(model.id)) is not None
        before, loaded.name = loaded.version, "bumped"
        await store.save_with_version_check(loaded)
        assert (after := await store.get(model.id)) is not None and after.version == before + 1

    async def test_save_with_version_check_rejects_a_stale_version(self, store: Repo) -> None:
        await store.save(model := _model())
        assert (first := await store.get(model.id)) is not None and (second := await store.get(model.id)) is not None  # fmt: skip
        first.name, second.name = "winner", "loser"
        await store.save_with_version_check(first)
        with pytest.raises(_rm.ReadModelVersionConflictError): await store.save_with_version_check(second)  # fmt: skip  # noqa: E701

    async def test_save_with_version_check_rejects_an_absent_model(self, store: Repo) -> None:
        with pytest.raises(_rm.ReadModelNotFoundError): await store.save_with_version_check(_model())  # fmt: skip  # noqa: E701

    async def test_reads_do_not_alias_stored_state(self, store: Repo) -> None:
        await store.save(model := _model(name="stored"))
        assert (loaded := await store.get(model.id)) is not None
        loaded.name = "mutated locally"
        assert (again := await store.get(model.id)) is not None and again.name == "stored" and (await store.get_many([model.id]))[0].name == (await store.find())[0].name == "stored"  # fmt: skip

    async def test_writes_do_not_mutate_the_callers_model(self, store: Repo) -> None:
        await store.save(model := _model(name="first"))
        assert (update := await store.get(model.id)) is not None
        v_saved, update.name = model.version, "second"
        await store.save(update)
        assert (model.name, model.version) == ("first", v_saved)


__all__ = ["ReadModelRepositoryConformance"]
