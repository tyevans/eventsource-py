"""Conformance suite for the `ReadModelRepository` port."""

import asyncio
from abc import ABC, abstractmethod
from uuid import uuid4

import pytest

from eventsource.ports.readmodels import (
    Filter,
    Query,
    ReadModelNotFoundError,
    ReadModelRepository,
    ReadModelVersionConflictError,
)
from eventsource.testing.conformance_ports._fixtures import ConformanceReadModel

Repo = ReadModelRepository[ConformanceReadModel]


def _model(
    name: str = "conformance", count: int = 0, note: str | None = None
) -> ConformanceReadModel:
    return ConformanceReadModel(id=uuid4(), name=name, count=count, note=note)


def _bad_filter(field: str, operator: str, value: object) -> Filter:
    return Filter(field=field, operator=operator, value=value)  # type: ignore[arg-type]


class ReadModelRepositoryConformance(ABC):
    """Conformance suite for `ReadModelRepository` implementations."""

    @abstractmethod
    @pytest.fixture
    def store(self) -> object:
        raise NotImplementedError

    async def test_save_then_get_round_trips(self, store: Repo) -> None:
        await store.save(model := _model(name="alpha", count=3))
        assert (loaded := await store.get(model.id)) is not None
        assert (loaded.id, loaded.name, loaded.count) == (model.id, "alpha", 3)

    async def test_get_of_an_absent_id_returns_none(self, store: Repo) -> None:
        assert await store.get(uuid4()) is None

    async def test_save_upserts_and_advances_updated_at(self, store: Repo) -> None:
        await store.save(model := _model(name="before"))
        assert (first := await store.get(model.id)) is not None
        await asyncio.sleep(0.01)
        first.name = "after"
        await store.save(first)
        assert (sec := await store.get(model.id)) is not None and sec.name == "after"
        assert sec.updated_at >= first.updated_at and await store.count() == 1

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
        assert await store.delete(model.id) is True
        assert await store.get(model.id) is None and await store.delete(model.id) is False

    async def test_soft_delete_hides_from_get_but_not_from_get_deleted(self, store: Repo) -> None:
        await store.save(model := _model())
        assert await store.soft_delete(model.id) is True and await store.get(model.id) is None
        assert (rec := await store.get_deleted(model.id)) is not None and rec.id == model.id

    async def test_restore_makes_a_soft_deleted_model_visible_again(self, store: Repo) -> None:
        await store.save(model := _model())
        await store.soft_delete(model.id)
        assert await store.restore(model.id) is True and await store.get(model.id) is not None

    async def test_find_deleted_returns_only_soft_deleted_models(self, store: Repo) -> None:
        await store.save_many([_model(name="live"), gone := _model(name="gone")])
        await store.soft_delete(gone.id)
        assert [m.id for m in await store.find_deleted()] == [gone.id]

    async def test_find_filters_on_equality(self, store: Repo) -> None:
        await store.save_many([alpha := _model(name="alpha"), _model(name="beta")])
        assert [m.id for m in await store.find(Query([Filter.eq("name", "alpha")]))] == [alpha.id]

    async def test_find_supports_every_comparison_operator(self, store: Repo) -> None:
        await store.save_many([_model(name=f"m{i}", count=i) for i in (1, 2, 3)])

        async def counts(op: str, val: object) -> list[int]:
            return sorted(m.count for m in await store.find(Query([_bad_filter("count", op, val)])))

        assert await counts("eq", 2) == [2] and await counts("ne", 2) == [1, 3]
        assert await counts("gt", 2) == [3] and await counts("gte", 2) == [2, 3]
        assert await counts("lt", 2) == [1] and await counts("lte", 2) == [1, 2]
        assert await counts("in", [1, 3]) == [1, 3] and await counts("not_in", [1, 3]) == [2]

    async def test_a_null_field_is_distinct_from_every_value(self, store: Repo) -> None:
        await store.save_many([_model(name="present", note="set"), _model(name="absent")])
        f_eq, f_in = Filter.eq("note", "set"), Filter.in_("note", ["set"])
        f_gt, f_gte = Filter.gt("note", "a"), Filter.gte("note", "a")
        f_lt, f_lte = Filter.lt("note", "zzz"), Filter.lte("note", "zzz")
        for f in (f_eq, f_in, f_gt, f_gte, f_lt, f_lte):
            assert [m.name for m in await store.find(Query([f]))] == ["present"]
        for f in (Filter.ne("note", "set"), Filter.not_in("note", ["set"])):
            assert [m.name for m in await store.find(Query([f]))] == ["absent"]

    async def test_empty_in_matches_nothing_and_empty_not_in_matches_everything(
        self, store: Repo
    ) -> None:
        await store.save_many([_model(name="a"), _model(name="b")])
        q_in, q_out = Query([Filter.in_("name", [])]), Query([Filter.not_in("name", [])])
        assert await store.find(q_in) == [] and await store.count(q_in) == 0
        assert len(await store.find(q_out)) == 2 and await store.count(q_out) == 2

    async def test_filtering_on_an_unknown_field_raises(self, store: Repo) -> None:
        await store.save(_model())
        for op in (store.find, store.count, store.find_deleted):
            with pytest.raises(ValueError, match="no_such_field"):
                await op(Query(filters=[Filter.eq("no_such_field", "x")]))

    async def test_an_unknown_operator_raises(self, store: Repo) -> None:
        await store.save(_model())
        for op in (store.find, store.count, store.find_deleted):
            with pytest.raises(ValueError, match="like"):
                await op(Query(filters=[_bad_filter("name", "like", "x")]))

    async def test_find_filters_on_uuid_values(self, store: Repo) -> None:
        await store.save_many([wanted := _model(name="wanted"), other := _model(name="other")])
        assert [m.id for m in await store.find(Query([Filter.eq("id", wanted.id)]))] == [wanted.id]
        both = await store.find(Query([Filter.in_("id", [wanted.id, other.id])]))
        assert sorted(m.name for m in both) == ["other", "wanted"]

    async def test_find_offsets_results(self, store: Repo) -> None:
        await store.save_many([_model(name=f"m{i}", count=i) for i in range(3)])
        assert [m.count for m in await store.find(Query(order_by="count", offset=1))] == [1, 2]
        q_one = Query(order_by="count", offset=1, limit=1)
        assert [m.count for m in await store.find(q_one)] == [1]

    async def test_include_deleted_shows_soft_deleted_rows(self, store: Repo) -> None:
        await store.save_many([_model(name="live"), gone := _model(name="gone")])
        await store.soft_delete(gone.id)
        q_all = Query(include_deleted=True)
        assert sorted(m.name for m in await store.find(q_all)) == ["gone", "live"]
        assert await store.count(q_all) == 2
        q_gone = Query(filters=[Filter.eq("name", "gone")], include_deleted=True)
        assert [m.id for m in await store.find(q_gone)] == [gone.id]

    async def test_find_orders_and_limits(self, store: Repo) -> None:
        await store.save_many([_model(name=f"m{i}", count=i) for i in range(3)])
        q = Query(order_by="count", order_direction="desc", limit=2)
        assert [m.count for m in await store.find(q)] == [2, 1]

    async def test_find_excludes_soft_deleted_models(self, store: Repo) -> None:
        await store.save_many([live := _model(name="live"), gone := _model(name="gone")])
        await store.soft_delete(gone.id)
        assert [m.id for m in await store.find()] == [live.id]

    async def test_count_with_and_without_filters(self, store: Repo) -> None:
        await store.save_many([_model(name="alpha"), _model(name="beta")])
        q_a = Query([Filter.eq("name", "alpha")])
        assert await store.count() == 2 and await store.count(q_a) == 1

    async def test_truncate_returns_the_count_and_empties_the_table(self, store: Repo) -> None:
        await store.save_many([_model(name="live"), gone := _model(name="gone")])
        await store.soft_delete(gone.id)
        assert await store.truncate() == 2
        assert await store.count() == 0 and await store.get_deleted(gone.id) is None

    async def test_save_with_version_check_increments_version(self, store: Repo) -> None:
        await store.save(model := _model())
        assert (loaded := await store.get(model.id)) is not None
        before, loaded.name = loaded.version, "bumped"
        await store.save_with_version_check(loaded)
        assert (after := await store.get(model.id)) is not None and after.version == before + 1

    async def test_save_with_version_check_rejects_a_stale_version(self, store: Repo) -> None:
        await store.save(model := _model())
        first, second = await store.get(model.id), await store.get(model.id)
        assert first is not None and second is not None
        first.name, second.name = "winner", "loser"
        await store.save_with_version_check(first)
        with pytest.raises(ReadModelVersionConflictError):
            await store.save_with_version_check(second)

    async def test_save_with_version_check_rejects_an_absent_model(self, store: Repo) -> None:
        with pytest.raises(ReadModelNotFoundError):
            await store.save_with_version_check(_model())

    async def test_reads_do_not_alias_stored_state(self, store: Repo) -> None:
        await store.save(model := _model(name="stored"))
        assert (loaded := await store.get(model.id)) is not None
        loaded.name = "mutated locally"
        assert (again := await store.get(model.id)) is not None and again.name == "stored"
        assert (
            (await store.get_many([model.id]))[0].name == (await store.find())[0].name == "stored"
        )

    async def test_writes_do_not_mutate_the_callers_model(self, store: Repo) -> None:
        await store.save(model := _model(name="first"))
        assert (update := await store.get(model.id)) is not None
        v_saved, update.name = model.version, "second"
        await store.save(update)
        assert (model.name, model.version) == ("first", v_saved)


__all__ = ["ReadModelRepositoryConformance"]
