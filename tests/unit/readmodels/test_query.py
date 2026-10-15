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

    def test_not_in_filter(self) -> None:
        f = Filter.not_in("status", ["cancelled", "refunded"])
        assert (f.field, f.operator, f.value) == ("status", "not_in", ["cancelled", "refunded"])

    def test_filter_is_frozen(self) -> None:
        f = Filter.eq("status", "active")
        with pytest.raises(AttributeError):
            f.value = "inactive"  # type: ignore[misc]

    def test_filter_str_eq(self) -> None:
        assert str(Filter.eq("status", "active")) == "status = 'active'"

    def test_filter_str_ne(self) -> None:
        assert str(Filter.ne("status", "cancelled")) == "status != 'cancelled'"

    def test_filter_str_gt(self) -> None:
        assert str(Filter.gt("amount", 100)) == "amount > 100"

    def test_filter_str_gte(self) -> None:
        assert str(Filter.gte("amount", 100)) == "amount >= 100"

    def test_filter_str_lt(self) -> None:
        assert str(Filter.lt("amount", 100)) == "amount < 100"

    def test_filter_str_lte(self) -> None:
        assert str(Filter.lte("amount", 100)) == "amount <= 100"

    def test_filter_str_in(self) -> None:
        result = str(Filter.in_("status", ["a", "b"]))
        assert "status" in result and "IN" in result and "['a', 'b']" in result

    def test_filter_str_not_in(self) -> None:
        result = str(Filter.not_in("status", ["a", "b"]))
        assert "status" in result and "NOT IN" in result

    def test_filter_with_datetime(self) -> None:
        now = datetime.now(UTC)
        f = Filter.lt("created_at", now)
        assert (f.field, f.operator, f.value) == ("created_at", "lt", now)

    def test_filter_with_none_value(self) -> None:
        assert Filter.eq("deleted_at", None).value is None


class TestQuery:
    """Tests for Query dataclass."""

    def test_default_query(self) -> None:
        q = Query()
        assert q.filters == []
        assert q.order_by is None
        assert q.order_direction == "asc"
        assert q.limit is None
        assert q.offset == 0
        assert q.include_deleted is False

    def test_query_with_filters(self) -> None:
        q = Query(filters=[Filter.eq("status", "active"), Filter.gt("amount", 100)])
        assert len(q.filters) == 2
        assert q.filters[0].field == "status"
        assert q.filters[1].field == "amount"

    def test_query_with_order(self) -> None:
        q = Query(order_by="created_at", order_direction="desc")
        assert (q.order_by, q.order_direction) == ("created_at", "desc")

    def test_query_with_pagination(self) -> None:
        q = Query(limit=20, offset=40)
        assert (q.limit, q.offset) == (20, 40)

    def test_query_include_deleted(self) -> None:
        assert Query(include_deleted=True).include_deleted is True

    def test_with_filter_builder(self) -> None:
        q = Query()
        q2 = q.with_filter(Filter.eq("status", "active"))
        assert len(q.filters) == 0
        assert len(q2.filters) == 1
        assert q2.filters[0].field == "status"

    def test_with_filter_preserves_other_fields(self) -> None:
        q = Query(order_by="name", limit=10, include_deleted=True)
        q2 = q.with_filter(Filter.eq("status", "active"))
        assert (q2.order_by, q2.limit, q2.include_deleted) == ("name", 10, True)

    def test_with_order_builder(self) -> None:
        q = Query()
        q2 = q.with_order("created_at", "desc")
        assert q.order_by is None
        assert (q2.order_by, q2.order_direction) == ("created_at", "desc")

    def test_with_order_default_direction(self) -> None:
        q2 = Query().with_order("name")
        assert (q2.order_by, q2.order_direction) == ("name", "asc")

    def test_with_pagination_builder(self) -> None:
        q = Query()
        q2 = q.with_pagination(limit=50, offset=100)
        assert q.limit is None
        assert (q2.limit, q2.offset) == (50, 100)

    def test_with_pagination_default_offset(self) -> None:
        q2 = Query().with_pagination(limit=50)
        assert (q2.limit, q2.offset) == (50, 0)

    def test_with_deleted_builder(self) -> None:
        q = Query()
        q2 = q.with_deleted(True)
        assert q.include_deleted is False
        assert q2.include_deleted is True

    def test_with_deleted_default_value(self) -> None:
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

    def test_query_str_empty(self) -> None:
        assert str(Query()) == "(all records)"

    def test_query_str_with_filters(self) -> None:
        q = Query(filters=[Filter.eq("status", "active"), Filter.gt("amount", 100)])
        result = str(q)
        assert "WHERE" in result and "AND" in result
        assert "status" in result and "amount" in result

    def test_query_str_with_order(self) -> None:
        result = str(Query(order_by="created_at", order_direction="desc"))
        assert "ORDER BY created_at DESC" in result

    def test_query_str_with_limit(self) -> None:
        assert "LIMIT 20" in str(Query(limit=20))

    def test_query_str_with_offset(self) -> None:
        assert "OFFSET 40" in str(Query(offset=40))

    def test_query_str_with_include_deleted(self) -> None:
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
        assert "WHERE" in result
        assert "ORDER BY created_at DESC" in result
        assert "LIMIT 20" in result
        assert "OFFSET 40" in result
        assert "(including deleted)" in result
