"""Unit tests for PostgreSQLReadModelRepository model handling, construction, and basic operations."""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from eventsource.adapters.postgresql.readmodels import PostgreSQLReadModelRepository
from eventsource.ports.readmodels.model import ReadModel
from eventsource.ports.readmodels.query import Filter


class OrderSummary(ReadModel):
    """Test model for repository tests."""

    order_number: str
    status: str
    total_amount: Decimal


class CustomTableModel(ReadModel):
    """Test model with custom table name."""

    __table_name__ = "custom_orders"
    name: str


class TestPostgreSQLReadModelRepositoryConstruction:
    """Tests for repository construction and initialization."""

    def test_constructor_with_model_class(self) -> None:
        repo = PostgreSQLReadModelRepository(MagicMock(), OrderSummary, enable_tracing=False)
        assert repo._model_class == OrderSummary
        assert repo.model_class == OrderSummary

    def test_constructor_derives_table_name(self) -> None:
        repo = PostgreSQLReadModelRepository(MagicMock(), OrderSummary, enable_tracing=False)
        assert repo._table_name == "order_summaries"

    def test_constructor_custom_table_name(self) -> None:
        repo = PostgreSQLReadModelRepository(MagicMock(), CustomTableModel, enable_tracing=False)
        assert repo._table_name == "custom_orders"

    def test_constructor_stores_field_names(self) -> None:
        repo = PostgreSQLReadModelRepository(MagicMock(), OrderSummary, enable_tracing=False)
        expected_fields = [
            "id",
            "created_at",
            "updated_at",
            "version",
            "deleted_at",
            "order_number",
            "status",
            "total_amount",
        ]
        assert repo._field_names == expected_fields

    def test_repr(self) -> None:
        repo = PostgreSQLReadModelRepository(MagicMock(), OrderSummary, enable_tracing=False)
        repr_str = repr(repo)
        assert "PostgreSQLReadModelRepository" in repr_str
        assert "OrderSummary" in repr_str
        assert "order_summaries" in repr_str
        assert "disabled" in repr_str

    def test_repr_with_tracing(self) -> None:
        repo = PostgreSQLReadModelRepository(MagicMock(), OrderSummary, enable_tracing=True)
        assert "enabled" in repr(repo)


class TestFilterToSQL:
    """PostgreSQL's rendering of a filter (named parameters and ANY/ALL over a bound array)."""

    @pytest.fixture
    def repo(self) -> PostgreSQLReadModelRepository[OrderSummary]:
        return PostgreSQLReadModelRepository(MagicMock(), OrderSummary, enable_tracing=False)

    def test_scalar_operators_bind_named_parameters(
        self, repo: PostgreSQLReadModelRepository[OrderSummary]
    ) -> None:
        clause, params = repo._filter_to_sql(Filter.eq("status", "active"), 0)
        assert clause == "status = :p0"
        assert params == {"p0": "active"}

        clause, params = repo._filter_to_sql(Filter.gt("total_amount", 100), 2)
        assert clause == "total_amount > :p2"
        assert params == {"p2": 100}

    def test_null_tolerant_operators_say_so_explicitly(
        self, repo: PostgreSQLReadModelRepository[OrderSummary]
    ) -> None:
        clause, params = repo._filter_to_sql(Filter.ne("status", "cancelled"), 1)
        assert clause == "(status IS NULL OR status != :p1)"
        assert params == {"p1": "cancelled"}

    def test_list_operators_bind_one_array_parameter(
        self, repo: PostgreSQLReadModelRepository[OrderSummary]
    ) -> None:
        clause, params = repo._filter_to_sql(Filter.in_("status", ["a", "b"]), 6)
        assert clause == "status = ANY(:p6)"
        assert params == {"p6": ["a", "b"]}

        clause, params = repo._filter_to_sql(Filter.not_in("status", ["c", "d"]), 7)
        assert clause == "(status IS NULL OR status != ALL(:p7))"
        assert params == {"p7": ["c", "d"]}

    def test_empty_list_operators_render_without_parameters(
        self, repo: PostgreSQLReadModelRepository[OrderSummary]
    ) -> None:
        assert repo._filter_to_sql(Filter.in_("status", []), 0) == ("1 = 0", {})
        assert repo._filter_to_sql(Filter.not_in("status", []), 0) == ("1 = 1", {})


class TestRowToModel:
    """Tests for row to model conversion."""

    @pytest.fixture
    def repo(self) -> PostgreSQLReadModelRepository[OrderSummary]:
        return PostgreSQLReadModelRepository(MagicMock(), OrderSummary, enable_tracing=False)

    def test_row_to_model(self, repo: PostgreSQLReadModelRepository[OrderSummary]) -> None:
        model_id = uuid4()
        now = datetime.now(UTC)
        row = (model_id, now, now, 1, None, "ORD-001", "pending", Decimal("99.99"))
        model = repo._row_to_model(row)

        assert model.id == model_id
        assert model.created_at == now
        assert model.updated_at == now
        assert model.version == 1
        assert model.deleted_at is None
        assert model.order_number == "ORD-001"
        assert model.status == "pending"
        assert model.total_amount == Decimal("99.99")


class TestAsyncOperations:
    """Tests for async operations early returns and edge cases."""

    @pytest.fixture
    def repo(self) -> PostgreSQLReadModelRepository[OrderSummary]:
        return PostgreSQLReadModelRepository(MagicMock(), OrderSummary, enable_tracing=False)

    @pytest.mark.asyncio
    async def test_get_many_empty_list_returns_empty(
        self, repo: PostgreSQLReadModelRepository[OrderSummary]
    ) -> None:
        assert await repo.get_many([]) == []

    @pytest.mark.asyncio
    async def test_save_many_empty_list_does_nothing(
        self, repo: PostgreSQLReadModelRepository[OrderSummary]
    ) -> None:
        await repo.save_many([])


class TestModelClassProperty:
    """Tests for model_class property."""

    def test_model_class_returns_correct_class(self) -> None:
        repo = PostgreSQLReadModelRepository(MagicMock(), OrderSummary, enable_tracing=False)
        assert repo.model_class is OrderSummary

    def test_model_class_custom_model(self) -> None:
        repo = PostgreSQLReadModelRepository(MagicMock(), CustomTableModel, enable_tracing=False)
        assert repo.model_class is CustomTableModel


class TestTracingConfiguration:
    """Tests for tracing configuration."""

    def test_tracing_disabled_by_flag(self) -> None:
        repo = PostgreSQLReadModelRepository(MagicMock(), OrderSummary, enable_tracing=False)
        assert repo._enable_tracing is False

    def test_tracing_enabled_by_default(self) -> None:
        repo = PostgreSQLReadModelRepository(MagicMock(), OrderSummary)
        assert repo._enable_tracing is True

    def test_tracing_can_be_explicitly_enabled(self) -> None:
        repo = PostgreSQLReadModelRepository(MagicMock(), OrderSummary, enable_tracing=True)
        assert repo._enable_tracing is True


class TestSQLSecurityAnnotations:
    """Tests to verify SQL security annotations are present."""

    def test_table_name_from_trusted_source(self) -> None:
        repo = PostgreSQLReadModelRepository(MagicMock(), OrderSummary, enable_tracing=False)
        assert repo._table_name == OrderSummary.table_name()
        assert repo._table_name == "order_summaries"

    def test_custom_table_name_from_class(self) -> None:
        repo = PostgreSQLReadModelRepository(MagicMock(), CustomTableModel, enable_tracing=False)
        assert repo._table_name == "custom_orders"
        assert repo._table_name == CustomTableModel.table_name()
