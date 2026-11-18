"""Construction-time validation of Query sort and paging values (TASK-0461)."""

import pytest

from eventsource.ports.readmodels import Query


class TestOrderDirection:
    @pytest.mark.parametrize("direction", ["asc", "desc", "ASC", "Desc"])
    def test_accepts_asc_or_desc_in_any_case(self, direction: str) -> None:
        query = Query(order_by="name", order_direction=direction)  # type: ignore[arg-type]
        assert query.order_direction == direction.lower()

    @pytest.mark.parametrize("direction", ["ASC, (SELECT 1)", "up", "", "asc desc"])
    def test_rejects_anything_else(self, direction: str) -> None:
        with pytest.raises(ValueError, match="order_direction"):
            Query(order_by="name", order_direction=direction)  # type: ignore[arg-type]

    def test_with_order_applies_the_same_check(self) -> None:
        with pytest.raises(ValueError, match="order_direction"):
            Query().with_order("name", "sideways")  # type: ignore[arg-type]


class TestOrderBy:
    def test_rejects_a_non_string(self) -> None:
        with pytest.raises(ValueError, match="order_by"):
            Query(order_by=3)  # type: ignore[arg-type]


class TestPaging:
    @pytest.mark.parametrize("limit", [0, 1, 500])
    def test_accepts_non_negative_int_limit(self, limit: int) -> None:
        assert Query(limit=limit).limit == limit

    @pytest.mark.parametrize("value", ["(SELECT 1)", "5", -1, 2.0, True])
    def test_rejects_non_int_or_negative_limit(self, value: object) -> None:
        with pytest.raises(ValueError, match="limit"):
            Query(limit=value)  # type: ignore[arg-type]

    @pytest.mark.parametrize("value", ["(SELECT 1)", -1, None, False])
    def test_rejects_non_int_or_negative_offset(self, value: object) -> None:
        with pytest.raises(ValueError, match="offset"):
            Query(offset=value)  # type: ignore[arg-type]

    def test_with_pagination_applies_the_same_check(self) -> None:
        with pytest.raises(ValueError, match="limit"):
            Query().with_pagination(limit="(SELECT 1)")  # type: ignore[arg-type]
