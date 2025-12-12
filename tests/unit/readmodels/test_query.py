"""Unit tests for Query and Filter classes."""

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from eventsource.readmodels.query import Filter, Query


class TestFilter:
    """Tests for Filter dataclass."""

    def test_eq_filter(self) -> None:
        """Test equality filter creation."""
        f = Filter.eq("status", "active")
        assert f.field == "status"
        assert f.operator == "eq"
        assert f.value == "active"

    def test_ne_filter(self) -> None:
        """Test not-equal filter creation."""
        f = Filter.ne("status", "cancelled")
        assert f.field == "status"
        assert f.operator == "ne"
        assert f.value == "cancelled"

    def test_gt_filter(self) -> None:
        """Test greater-than filter creation."""
        f = Filter.gt("amount", Decimal("100"))
        assert f.field == "amount"
        assert f.operator == "gt"
        assert f.value == Decimal("100")

    def test_gte_filter(self) -> None:
        """Test greater-than-or-equal filter creation."""
        f = Filter.gte("version", 2)
        assert f.field == "version"
        assert f.operator == "gte"
        assert f.value == 2

    def test_lt_filter(self) -> None:
        """Test less-than filter creation."""
        f = Filter.lt("priority", 5)
        assert f.field == "priority"
        assert f.operator == "lt"
        assert f.value == 5

    def test_lte_filter(self) -> None:
        """Test less-than-or-equal filter creation."""
        f = Filter.lte("count", 10)
        assert f.field == "count"
        assert f.operator == "lte"
        assert f.value == 10

    def test_in_filter(self) -> None:
        """Test in-list filter creation."""
        f = Filter.in_("status", ["pending", "processing"])
        assert f.field == "status"
        assert f.operator == "in"
        assert f.value == ["pending", "processing"]

    def test_not_in_filter(self) -> None:
        """Test not-in-list filter creation."""
        f = Filter.not_in("status", ["cancelled", "refunded"])
        assert f.field == "status"
        assert f.operator == "not_in"
        assert f.value == ["cancelled", "refunded"]

    def test_filter_is_frozen(self) -> None:
        """Test that filters are immutable."""
        f = Filter.eq("status", "active")
        with pytest.raises(AttributeError):
            f.value = "inactive"  # type: ignore[misc]

    def test_filter_str_eq(self) -> None:
        """Test string representation for eq operator."""
        f = Filter.eq("status", "active")
        assert str(f) == "status = 'active'"

    def test_filter_str_ne(self) -> None:
        """Test string representation for ne operator."""
        f = Filter.ne("status", "cancelled")
        assert str(f) == "status != 'cancelled'"

    def test_filter_str_gt(self) -> None:
        """Test string representation for gt operator."""
        f = Filter.gt("amount", 100)
        assert str(f) == "amount > 100"

    def test_filter_str_gte(self) -> None:
        """Test string representation for gte operator."""
        f = Filter.gte("amount", 100)
        assert str(f) == "amount >= 100"

    def test_filter_str_lt(self) -> None:
        """Test string representation for lt operator."""
        f = Filter.lt("amount", 100)
        assert str(f) == "amount < 100"

    def test_filter_str_lte(self) -> None:
        """Test string representation for lte operator."""
        f = Filter.lte("amount", 100)
        assert str(f) == "amount <= 100"

    def test_filter_str_in(self) -> None:
        """Test string representation for in operator."""
        f = Filter.in_("status", ["a", "b"])
        result = str(f)
        assert "status" in result
        assert "IN" in result
        assert "['a', 'b']" in result

    def test_filter_str_not_in(self) -> None:
        """Test string representation for not_in operator."""
        f = Filter.not_in("status", ["a", "b"])
        result = str(f)
        assert "status" in result
        assert "NOT IN" in result

    def test_filter_with_datetime(self) -> None:
        """Test filter with datetime value."""
        now = datetime.now(UTC)
        f = Filter.lt("created_at", now)
        assert f.field == "created_at"
        assert f.operator == "lt"
        assert f.value == now

    def test_filter_with_none_value(self) -> None:
        """Test filter with None value."""
        f = Filter.eq("deleted_at", None)
        assert f.value is None


class TestQuery:
    """Tests for Query dataclass."""

    def test_default_query(self) -> None:
        """Test default query values."""
        q = Query()
        assert q.filters == []
        assert q.order_by is None
        assert q.order_direction == "asc"
        assert q.limit is None
        assert q.offset == 0
        assert q.include_deleted is False

    def test_query_with_filters(self) -> None:
        """Test query with filters."""
        q = Query(
            filters=[
                Filter.eq("status", "active"),
                Filter.gt("amount", 100),
            ]
        )
        assert len(q.filters) == 2
        assert q.filters[0].field == "status"
        assert q.filters[1].field == "amount"

    def test_query_with_order(self) -> None:
        """Test query with ordering."""
        q = Query(order_by="created_at", order_direction="desc")
        assert q.order_by == "created_at"
        assert q.order_direction == "desc"

    def test_query_with_pagination(self) -> None:
        """Test query with pagination."""
        q = Query(limit=20, offset=40)
        assert q.limit == 20
        assert q.offset == 40

    def test_query_include_deleted(self) -> None:
        """Test query with include_deleted."""
        q = Query(include_deleted=True)
        assert q.include_deleted is True

    def test_with_filter_builder(self) -> None:
        """Test with_filter builder method."""
        q = Query()
        q2 = q.with_filter(Filter.eq("status", "active"))

        assert len(q.filters) == 0  # Original unchanged
        assert len(q2.filters) == 1
        assert q2.filters[0].field == "status"

    def test_with_filter_preserves_other_fields(self) -> None:
        """Test that with_filter preserves other query fields."""
        q = Query(order_by="name", limit=10, include_deleted=True)
        q2 = q.with_filter(Filter.eq("status", "active"))

        assert q2.order_by == "name"
        assert q2.limit == 10
        assert q2.include_deleted is True

    def test_with_order_builder(self) -> None:
        """Test with_order builder method."""
        q = Query()
        q2 = q.with_order("created_at", "desc")

        assert q.order_by is None  # Original unchanged
        assert q2.order_by == "created_at"
        assert q2.order_direction == "desc"

    def test_with_order_default_direction(self) -> None:
        """Test with_order default direction is asc."""
        q = Query()
        q2 = q.with_order("name")

        assert q2.order_by == "name"
        assert q2.order_direction == "asc"

    def test_with_pagination_builder(self) -> None:
        """Test with_pagination builder method."""
        q = Query()
        q2 = q.with_pagination(limit=50, offset=100)

        assert q.limit is None  # Original unchanged
        assert q2.limit == 50
        assert q2.offset == 100

    def test_with_pagination_default_offset(self) -> None:
        """Test with_pagination default offset is 0."""
        q = Query()
        q2 = q.with_pagination(limit=50)

        assert q2.limit == 50
        assert q2.offset == 0

    def test_with_deleted_builder(self) -> None:
        """Test with_deleted builder method."""
        q = Query()
        q2 = q.with_deleted(True)

        assert q.include_deleted is False  # Original unchanged
        assert q2.include_deleted is True

    def test_with_deleted_default_value(self) -> None:
        """Test with_deleted default value is True."""
        q = Query()
        q2 = q.with_deleted()

        assert q2.include_deleted is True

    def test_builder_chaining(self) -> None:
        """Test that builders can be chained."""
        q = (
            Query()
            .with_filter(Filter.eq("status", "active"))
            .with_filter(Filter.gt("amount", 100))
            .with_order("created_at", "desc")
            .with_pagination(limit=20, offset=0)
            .with_deleted(False)
        )

        assert len(q.filters) == 2
        assert q.order_by == "created_at"
        assert q.order_direction == "desc"
        assert q.limit == 20
        assert q.offset == 0
        assert q.include_deleted is False

    def test_query_str_empty(self) -> None:
        """Test string representation of empty query."""
        q = Query()
        assert str(q) == "(all records)"

    def test_query_str_with_filters(self) -> None:
        """Test string representation with filters."""
        q = Query(
            filters=[Filter.eq("status", "active"), Filter.gt("amount", 100)],
        )
        result = str(q)
        assert "WHERE" in result
        assert "AND" in result
        assert "status" in result
        assert "amount" in result

    def test_query_str_with_order(self) -> None:
        """Test string representation with order."""
        q = Query(order_by="created_at", order_direction="desc")
        result = str(q)
        assert "ORDER BY created_at DESC" in result

    def test_query_str_with_limit(self) -> None:
        """Test string representation with limit."""
        q = Query(limit=20)
        result = str(q)
        assert "LIMIT 20" in result

    def test_query_str_with_offset(self) -> None:
        """Test string representation with offset."""
        q = Query(offset=40)
        result = str(q)
        assert "OFFSET 40" in result

    def test_query_str_with_include_deleted(self) -> None:
        """Test string representation with include_deleted."""
        q = Query(include_deleted=True)
        result = str(q)
        assert "(including deleted)" in result

    def test_query_str_full(self) -> None:
        """Test string representation with all options."""
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
