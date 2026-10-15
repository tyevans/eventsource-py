"""Unit tests for Query and Filter classes."""

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from eventsource.ports.readmodels.query import Filter, Query


class TestFilter:
    """Tests for Filter dataclass."""

    def test_eq_filter(self) -> None:
        f = Filter.eq("status", "active")
        assert (f.field, f.operator, f.value) == ("status", "eq", "active")

    def test_ne_filter(self) -> None:
        f = Filter.ne("status", "cancelled")
        assert (f.field, f.operator, f.value) == ("status", "ne", "cancelled")

    def test_gt_filter(self) -> None:
        f = Filter.gt("amount", Decimal("100"))
        assert (f.field, f.operator, f.value) == ("amount", "gt", Decimal("100"))

    def test_gte_filter(self) -> None:
        f = Filter.gte("version", 2)
        assert (f.field, f.operator, f.value) == ("version", "gte", 2)

    def test_lt_filter(self) -> None:
        f = Filter.lt("priority", 5)
        assert (f.field, f.operator, f.value) == ("priority", "lt", 5)

    def test_lte_filter(self) -> None:
        f = Filter.lte("count", 10)
        assert (f.field, f.operator, f.value) == ("count", "lte", 10)

    def test_in_filter(self) -> None:
        f = Filter.in_("status", ["pending", "processing"])
        assert (f.field, f.operator, f.value) == ("status", "in", ["pending", "processing"])

    def test_in_filter_accepts_iterables(self) -> None:
        assert Filter.in_("status", ("a", "b")).value == ["a", "b"]
        assert sorted(Filter.in_("status", {"a", "b"}).value) == ["a", "b"]
        assert Filter.in_("num", (x for x in range(3))).value == [0, 1, 2]

    def test_not_in_filter(self) -> None:
        f = Filter.not_in("status", ["cancelled", "refunded"])
        assert (f.field, f.operator, f.value) == ("status", "not_in", ["cancelled", "refunded"])

    def test_not_in_filter_accepts_iterables(self) -> None:
        assert Filter.not_in("status", ("x", "y")).value == ["x", "y"]
        assert sorted(Filter.not_in("status", {"x", "y"}).value) == ["x", "y"]
        assert Filter.not_in("num", (x for x in range(2))).value == [0, 1]

    def test_filter_is_frozen(self) -> None:
        f = Filter.eq("status", "active")
        with pytest.raises(AttributeError):
            f.value = "inactive"  # type: ignore[misc]

    def test_filter_str_representations(self) -> None:
        assert str(Filter.eq("status", "active")) == "status = 'active'"
        assert str(Filter.ne("status", "cancelled")) == "status != 'cancelled'"
        assert str(Filter.gt("amount", 100)) == "amount > 100"
        assert str(Filter.gte("amount", 100)) == "amount >= 100"
        assert str(Filter.lt("amount", 100)) == "amount < 100"
        assert str(Filter.lte("amount", 100)) == "amount <= 100"
        assert str(Filter.in_("status", ["a", "b"])) == "status IN ['a', 'b']"
        assert str(Filter.not_in("status", ["a", "b"])) == "status NOT IN ['a', 'b']"

    def test_filter_with_datetime_and_none(self) -> None:
        now = datetime.now(UTC)
        assert Filter.lt("created_at", now) == Filter("created_at", "lt", now)
        assert Filter.eq("deleted_at", None).value is None


class TestQuery:
    """Tests for Query dataclass."""

    def test_default_query(self) -> None:
        q = Query()
        assert (q.filters, q.order_by, q.order_direction) == ([], None, "asc")
        assert (q.limit, q.offset, q.include_deleted) == (None, 0, False)

    def test_query_init_fields(self) -> None:
        q = Query(
            filters=[Filter.eq("status", "active"), Filter.gt("amount", 100)],
            order_by="created_at",
            order_direction="desc",
            limit=20,
            offset=40,
            include_deleted=True,
        )
        assert [f.field for f in q.filters] == ["status", "amount"]
        assert (q.order_by, q.order_direction) == ("created_at", "desc")
        assert (q.limit, q.offset, q.include_deleted) == (20, 40, True)

    def test_with_filter_builder(self) -> None:
        q = Query()
        q2 = q.with_filter(Filter.eq("status", "active"))
        assert q.filters == []
        assert q2.filters == [Filter.eq("status", "active")]

    def test_with_filter_variadic_and_kwargs(self) -> None:
        q = Query().with_filter(
            Filter.gt("amount", 100),
            Filter.lt("amount", 500),
            status="active",
            tier="gold",
        )
        assert q.filters == [
            Filter.gt("amount", 100),
            Filter.lt("amount", 500),
            Filter.eq("status", "active"),
            Filter.eq("tier", "gold"),
        ]

    def test_where_classmethod(self) -> None:
        q1 = Query.where(status="active", customer_id="c-1")
        assert q1.filters == [Filter.eq("status", "active"), Filter.eq("customer_id", "c-1")]
        q2 = Query.where(Filter.gt("amount", 100), status="active")
        assert q2.filters == [Filter.gt("amount", 100), Filter.eq("status", "active")]

    def test_with_filter_preserves_other_fields(self) -> None:
        q = Query(order_by="name", limit=10, include_deleted=True)
        q2 = q.with_filter(Filter.eq("status", "active"))
        assert (q2.order_by, q2.limit, q2.include_deleted) == ("name", 10, True)

    def test_with_order_builder(self) -> None:
        q = Query()
        q2 = q.with_order("created_at", "desc")
        assert q.order_by is None
        assert (q2.order_by, q2.order_direction) == ("created_at", "desc")
        assert (
            Query().with_order("name").order_by,
            Query().with_order("name").order_direction,
        ) == (
            "name",
            "asc",
        )

    def test_with_pagination_builder(self) -> None:
        q = Query()
        q2 = q.with_pagination(limit=50, offset=100)
        assert q.limit is None
        assert (q2.limit, q2.offset) == (50, 100)
        assert (
            Query().with_pagination(limit=50).limit,
            Query().with_pagination(limit=50).offset,
        ) == (
            50,
            0,
        )

    def test_with_deleted_builder(self) -> None:
        q = Query()
        assert q.with_deleted(True).include_deleted is True
        assert q.include_deleted is False
        assert Query().with_deleted().include_deleted is True

    def test_builder_chaining(self) -> None:
        q = (
            Query()
            .with_filter(Filter.eq("status", "active"))
            .with_filter(Filter.gt("amount", 100))
            .with_order("created_at", "desc")
            .with_pagination(limit=20, offset=0)
            .with_deleted(False)
        )
        assert len(q.filters) == 2
        assert (q.order_by, q.order_direction) == ("created_at", "desc")
        assert (q.limit, q.offset, q.include_deleted) == (20, 0, False)

    def test_query_str_representations(self) -> None:
        assert str(Query()) == "(all records)"
        assert "ORDER BY created_at DESC" in str(
            Query(order_by="created_at", order_direction="desc")
        )
        assert "LIMIT 20" in str(Query(limit=20))
        assert "OFFSET 40" in str(Query(offset=40))
        assert "(including deleted)" in str(Query(include_deleted=True))

    def test_query_str_full(self) -> None:
        q = Query(
            filters=[Filter.eq("status", "active"), Filter.gt("amount", 100)],
            order_by="created_at",
            order_direction="desc",
            limit=20,
            offset=40,
            include_deleted=True,
        )
        result = str(q)
        for expected in (
            "WHERE",
            "AND",
            "ORDER BY created_at DESC",
            "LIMIT 20",
            "OFFSET 40",
            "(including deleted)",
        ):
            assert expected in result
