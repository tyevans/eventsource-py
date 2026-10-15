"""Tests for SQLite read model repository construction and model conversion."""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from eventsource.adapters.sqlite.readmodels import SQLiteReadModelRepository
from eventsource.ports.readmodels.query import Filter
from tests.unit.readmodels.test_sqlite._fixtures import (
    CustomTableModel,
    OrderSummary,
)


class TestSQLiteReadModelRepositoryConstruction:
    """Tests for repository construction and initialization."""

    def test_constructor_with_model_class(self) -> None:
        repo = SQLiteReadModelRepository(MagicMock(), OrderSummary, enable_tracing=False)
        assert repo._model_class == OrderSummary
        assert repo.model_class == OrderSummary

    def test_constructor_derives_table_name(self) -> None:
        repo = SQLiteReadModelRepository(MagicMock(), OrderSummary, enable_tracing=False)
        assert repo._table_name == "order_summaries"

    def test_constructor_custom_table_name(self) -> None:
        repo = SQLiteReadModelRepository(MagicMock(), CustomTableModel, enable_tracing=False)
        assert repo._table_name == "custom_orders"

    def test_constructor_stores_field_names(self) -> None:
        repo = SQLiteReadModelRepository(MagicMock(), OrderSummary, enable_tracing=False)
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
        repo = SQLiteReadModelRepository(MagicMock(), OrderSummary, enable_tracing=False)
        repr_str = repr(repo)
        assert "SQLiteReadModelRepository" in repr_str
        assert "OrderSummary" in repr_str
        assert "order_summaries" in repr_str
        assert "disabled" in repr_str

    def test_repr_with_tracing(self) -> None:
        repo = SQLiteReadModelRepository(MagicMock(), OrderSummary, enable_tracing=True)
        assert "enabled" in repr(repo)


class TestFilterToSQL:
    """SQLite's rendering of a filter (dialect-specific placeholders and UUID coercion)."""

    @pytest.fixture
    def repo(self) -> SQLiteReadModelRepository[OrderSummary]:
        return SQLiteReadModelRepository(MagicMock(), OrderSummary, enable_tracing=False)

    def test_scalar_operators_bind_positional_parameters(
        self, repo: SQLiteReadModelRepository[OrderSummary]
    ) -> None:
        clause, params = repo._filter_to_sql(Filter.eq("status", "active"))
        assert clause == "status = ?"
        assert params == ["active"]

        clause, params = repo._filter_to_sql(Filter.gte("total_amount", 100))
        assert clause == "total_amount >= ?"
        assert params == [100]

    def test_null_tolerant_operators_say_so_explicitly(
        self, repo: SQLiteReadModelRepository[OrderSummary]
    ) -> None:
        clause, params = repo._filter_to_sql(Filter.ne("status", "cancelled"))
        assert clause == "(status IS NULL OR status != ?)"
        assert params == ["cancelled"]

    def test_list_operators_expand_to_one_placeholder_per_value(
        self, repo: SQLiteReadModelRepository[OrderSummary]
    ) -> None:
        clause, params = repo._filter_to_sql(Filter.in_("status", ["a", "b", "c"]))
        assert clause == "status IN (?,?,?)"
        assert params == ["a", "b", "c"]

        clause, params = repo._filter_to_sql(Filter.not_in("status", ["c", "d"]))
        assert clause == "(status IS NULL OR status NOT IN (?,?))"
        assert params == ["c", "d"]

    def test_empty_list_operators_render_without_parameters(
        self, repo: SQLiteReadModelRepository[OrderSummary]
    ) -> None:
        assert repo._filter_to_sql(Filter.in_("status", [])) == ("1 = 0", [])
        assert repo._filter_to_sql(Filter.not_in("status", [])) == ("1 = 1", [])

    def test_uuid_values_are_coerced_to_text(
        self, repo: SQLiteReadModelRepository[OrderSummary]
    ) -> None:
        test_uuid = uuid4()
        clause, params = repo._filter_to_sql(Filter.eq("id", test_uuid))
        assert clause == "id = ?"
        assert params == [str(test_uuid)]

        uuid1, uuid2 = uuid4(), uuid4()
        clause, params = repo._filter_to_sql(Filter.in_("id", [uuid1, uuid2]))
        assert clause == "id IN (?,?)"
        assert params == [str(uuid1), str(uuid2)]


class TestRowToModel:
    """Tests for row to model conversion with SQLite type handling."""

    @pytest.fixture
    def repo(self) -> SQLiteReadModelRepository[OrderSummary]:
        return SQLiteReadModelRepository(MagicMock(), OrderSummary, enable_tracing=False)

    def test_row_to_model_with_string_types(
        self, repo: SQLiteReadModelRepository[OrderSummary]
    ) -> None:
        model_id, now = uuid4(), datetime.now(UTC)
        row = (
            str(model_id),
            now.isoformat(),
            now.isoformat(),
            1,
            None,
            "ORD-001",
            "pending",
            Decimal("99.99"),
        )
        model = repo._row_to_model(row)
        assert model.id == model_id
        assert model.created_at.replace(microsecond=0) == now.replace(microsecond=0)
        assert model.updated_at.replace(microsecond=0) == now.replace(microsecond=0)
        assert model.version == 1
        assert model.deleted_at is None
        assert model.order_number == "ORD-001"
        assert model.status == "pending"
        assert abs(model.total_amount - Decimal("99.99")) < Decimal("0.01")

    def test_row_to_model_with_deleted_at(
        self, repo: SQLiteReadModelRepository[OrderSummary]
    ) -> None:
        model_id, now, deleted = uuid4(), datetime.now(UTC), datetime.now(UTC)
        row = (
            str(model_id),
            now.isoformat(),
            now.isoformat(),
            1,
            deleted.isoformat(),
            "ORD-001",
            "cancelled",
            Decimal("99.99"),
        )
        model = repo._row_to_model(row)
        assert model.deleted_at is not None
        assert model.is_deleted()

    def test_row_to_model_with_z_suffix(
        self, repo: SQLiteReadModelRepository[OrderSummary]
    ) -> None:
        model_id = uuid4()
        row = (
            str(model_id),
            "2024-01-15T10:30:00Z",
            "2024-01-15T10:30:00Z",
            1,
            None,
            "ORD-001",
            "pending",
            Decimal("50.00"),
        )
        model = repo._row_to_model(row)
        assert model.created_at is not None
        assert model.updated_at is not None


class TestModelToValues:
    """Tests for model to SQL values conversion."""

    @pytest.fixture
    def repo(self) -> SQLiteReadModelRepository[OrderSummary]:
        return SQLiteReadModelRepository(MagicMock(), OrderSummary, enable_tracing=False)

    def test_model_to_values(self, repo: SQLiteReadModelRepository[OrderSummary]) -> None:
        model_id = uuid4()
        model = OrderSummary(
            id=model_id,
            order_number="ORD-001",
            status="pending",
            total_amount=Decimal("99.99"),
        )
        now = datetime.now(UTC)
        values = repo._model_to_values(model, now)
        assert isinstance(values, tuple)
        assert len(values) == len(repo._field_names)
        assert values[0] == str(model_id)
        assert values[2] == now.isoformat()

    def test_model_to_values_preserves_order(
        self, repo: SQLiteReadModelRepository[OrderSummary]
    ) -> None:
        model = OrderSummary(
            id=uuid4(),
            order_number="ORD-001",
            status="pending",
            total_amount=Decimal("99.99"),
        )
        values = repo._model_to_values(model, datetime.now(UTC))
        assert values[5] == "ORD-001"
        assert values[6] == "pending"


class TestModelClassProperty:
    """Tests for model_class property."""

    def test_model_class_returns_correct_class(self) -> None:
        repo = SQLiteReadModelRepository(MagicMock(), OrderSummary, enable_tracing=False)
        assert repo.model_class is OrderSummary

    def test_model_class_custom_model(self) -> None:
        repo = SQLiteReadModelRepository(MagicMock(), CustomTableModel, enable_tracing=False)
        assert repo.model_class is CustomTableModel


__all__ = [
    "CustomTableModel",  # pragma: allowlist secret
    "OrderSummary",  # pragma: allowlist secret
    "TestFilterToSQL",  # pragma: allowlist secret
    "TestModelClassProperty",  # pragma: allowlist secret
    "TestModelToValues",  # pragma: allowlist secret
    "TestRowToModel",  # pragma: allowlist secret
    "TestSQLiteReadModelRepositoryConstruction",  # pragma: allowlist secret
]
