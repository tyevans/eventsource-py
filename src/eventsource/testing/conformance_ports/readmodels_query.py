"""Query-path conformance cases for the `ReadModelRepository` port (filters, paging, ordering)."""

from collections.abc import Callable
from uuid import uuid4

import pytest

from eventsource.ports.readmodels import Filter, Query, ReadModelRepository
from eventsource.testing.conformance_ports._fixtures import ConformanceReadModel

Repo = ReadModelRepository[ConformanceReadModel]


def _model(name: str = "conformance", count: int = 0, note: str | None = None) -> ConformanceReadModel:  # fmt: skip
    return ConformanceReadModel(id=uuid4(), name=name, count=count, note=note)


def _mutated(**attrs: object) -> Query:
    """Build a default query, then set fields directly (bypassing __post_init__)."""
    query = Query()
    for name, value in attrs.items():
        setattr(query, name, value)
    return query


# Sort and paging values that are SQL, not plain values. Each must be rejected
# before any adapter renders it, on every read path. The last entries reach the
# adapter through a field set after construction.
_INJECTED_QUERIES: tuple[Callable[[], Query], ...] = (
    lambda: Query(order_by="(SELECT 1)"),
    lambda: Query(order_by="(CASE WHEN (SELECT 1) = 1 THEN count ELSE -count END)"),
    lambda: Query(order_by="count", order_direction="ASC, (SELECT 1)"),  # type: ignore[arg-type]
    lambda: Query(limit="(SELECT 1)"),  # type: ignore[arg-type]
    lambda: Query(offset="(SELECT 1)"),  # type: ignore[arg-type]
    lambda: Query(order_by="no_such_field"),
    lambda: _mutated(order_by="(SELECT 1)"),
    lambda: _mutated(order_direction="ASC, (SELECT 1)"),
    lambda: _mutated(limit="(SELECT 1)"),
)


class ReadModelQueryConformance:
    """Query cases mixed into `ReadModelRepositoryConformance`; `store` comes from the host class."""

    async def test_find_filters_on_equality(self, store: Repo) -> None:
        await store.save_many([alpha := _model(name="alpha"), _model(name="beta")])
        assert [m.id for m in await store.find(Query([Filter.eq("name", "alpha")]))] == [alpha.id]

    async def test_find_supports_every_comparison_operator(self, store: Repo) -> None:
        await store.save_many([_model(name=f"m{i}", count=i) for i in (1, 2, 3)])
        assert {op: sorted(m.count for m in await store.find(Query([Filter("count", op, val)]))) for op, val in (("eq", 2), ("ne", 2), ("gt", 2), ("gte", 2), ("lt", 2), ("lte", 2), ("in", [1, 3]), ("not_in", [1, 3]))} == {"eq": [2], "ne": [1, 3], "gt": [3], "gte": [2, 3], "lt": [1], "lte": [1, 2], "in": [1, 3], "not_in": [2]}  # type: ignore[arg-type]  # fmt: skip

    async def test_a_null_field_is_distinct_from_every_value(self, store: Repo) -> None:
        await store.save_many([_model(name="present", note="set"), _model(name="absent")])
        assert all([[m.name for m in await store.find(Query([f]))] == ["present"] for f in (Filter.eq("note", "set"), Filter.in_("note", ["set"]), Filter.gt("note", "a"), Filter.gte("note", "a"), Filter.lt("note", "zzz"), Filter.lte("note", "zzz"))]) and all([[m.name for m in await store.find(Query([f]))] == ["absent"] for f in (Filter.ne("note", "set"), Filter.not_in("note", ["set"]))])  # fmt: skip

    async def test_empty_in_matches_nothing_and_empty_not_in_matches_everything(self, store: Repo) -> None:  # fmt: skip
        await store.save_many([_model(name="a"), _model(name="b")])
        assert (q_in := Query([Filter.in_("name", [])])) and (q_out := Query([Filter.not_in("name", [])])) and await store.find(q_in) == [] and await store.count(q_in) == 0 and len(await store.find(q_out)) == 2 and await store.count(q_out) == 2  # fmt: skip

    async def test_filtering_on_an_unknown_field_raises(self, store: Repo) -> None:
        await store.save(_model())
        for op in (store.find, store.count, store.find_deleted):
            with pytest.raises(ValueError, match="no_such_field"): await op(Query(filters=[Filter.eq("no_such_field", "x")]))  # fmt: skip  # noqa: E701

    async def test_an_unknown_operator_raises(self, store: Repo) -> None:
        await store.save(_model())
        for op in (store.find, store.count, store.find_deleted):
            with pytest.raises(ValueError, match="like"): await op(Query(filters=[Filter("name", "like", "x")]))  # type: ignore[arg-type]  # fmt: skip  # noqa: E701

    async def test_find_filters_on_uuid_values(self, store: Repo) -> None:
        await store.save_many([wanted := _model(name="wanted"), other := _model(name="other")])
        assert [m.id for m in await store.find(Query([Filter.eq("id", wanted.id)]))] == [wanted.id] and sorted(m.name for m in await store.find(Query([Filter.in_("id", [wanted.id, other.id])]))) == ["other", "wanted"]  # fmt: skip

    async def test_find_offsets_results(self, store: Repo) -> None:
        await store.save_many([_model(name=f"m{i}", count=i) for i in range(3)])
        assert [m.count for m in await store.find(Query(order_by="count", offset=1))] == [1, 2] and [m.count for m in await store.find(Query(order_by="count", offset=1, limit=1))] == [1]  # fmt: skip

    async def test_include_deleted_shows_soft_deleted_rows(self, store: Repo) -> None:
        await store.save_many([_model(name="live"), gone := _model(name="gone")])
        assert await store.soft_delete(gone.id) and (q_all := Query(include_deleted=True)) and sorted(m.name for m in await store.find(q_all)) == ["gone", "live"] and await store.count(q_all) == 2 and [m.id for m in await store.find(Query([Filter.eq("name", "gone")], include_deleted=True))] == [gone.id]  # fmt: skip

    async def test_find_orders_and_limits(self, store: Repo) -> None:
        await store.save_many([_model(name=f"m{i}", count=i) for i in range(3)])
        assert [m.count for m in await store.find(Query(order_by="count", order_direction="desc", limit=2))] == [2, 1]  # fmt: skip

    async def test_order_direction_accepts_any_case(self, store: Repo) -> None:
        await store.save_many([_model(name="a", count=1), _model(name="b", count=2)])
        assert [m.count for m in await store.find(Query(order_by="count", order_direction="DESC"))] == [2, 1]  # type: ignore[arg-type]  # fmt: skip

    async def test_injected_sort_or_paging_values_are_rejected(self, store: Repo) -> None:
        await store.save_many([_model(name="a", count=1), _model(name="b", count=2)])
        for build in _INJECTED_QUERIES:
            for op in (store.find, store.find_one, store.find_deleted):
                with pytest.raises(ValueError):
                    await op(build())

    async def test_find_excludes_soft_deleted_models(self, store: Repo) -> None:
        await store.save_many([live := _model(name="live"), gone := _model(name="gone")])
        assert await store.soft_delete(gone.id) and [m.id for m in await store.find()] == [live.id]

    async def test_count_with_and_without_filters(self, store: Repo) -> None:
        await store.save_many([_model(name="alpha"), _model(name="beta")])
        assert await store.count() == 2 and await store.count(Query([Filter.eq("name", "alpha")])) == 1  # fmt: skip
