"""Unit tests for SQLiteReadModelRepository.

These tests use mocking to verify correct SQL generation and behavior
without requiring a real SQLite database connection. Integration tests
with actual SQLite databases are in P2-004.
"""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from eventsource.readmodels import ReadModel
from eventsource.readmodels.query import Filter, Query
from eventsource.readmodels.sqlite import SQLiteReadModelRepository


class OrderSummary(ReadModel):
    """Test model for repository tests."""

    order_number: str
    status: str
    total_amount: Decimal


class CustomTableModel(ReadModel):
    """Test model with custom table name."""

    __table_name__ = "custom_orders"
    name: str


class TestSQLiteReadModelRepositoryConstruction:
    """Tests for repository construction and initialization."""

    def test_constructor_with_model_class(self) -> None:
        """Test repository construction stores model class."""
        mock_conn = MagicMock()
        repo = SQLiteReadModelRepository(mock_conn, OrderSummary, enable_tracing=False)

        assert repo._model_class == OrderSummary
        assert repo.model_class == OrderSummary

    def test_constructor_derives_table_name(self) -> None:
        """Test table name is derived from model class."""
        mock_conn = MagicMock()
        repo = SQLiteReadModelRepository(mock_conn, OrderSummary, enable_tracing=False)

        assert repo._table_name == "order_summaries"

    def test_constructor_custom_table_name(self) -> None:
        """Test custom table name is respected."""
        mock_conn = MagicMock()
        repo = SQLiteReadModelRepository(mock_conn, CustomTableModel, enable_tracing=False)

        assert repo._table_name == "custom_orders"

    def test_constructor_stores_field_names(self) -> None:
        """Test field names are extracted from model."""
        mock_conn = MagicMock()
        repo = SQLiteReadModelRepository(mock_conn, OrderSummary, enable_tracing=False)

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
        """Test string representation."""
        mock_conn = MagicMock()
        repo = SQLiteReadModelRepository(mock_conn, OrderSummary, enable_tracing=False)

        repr_str = repr(repo)
        assert "SQLiteReadModelRepository" in repr_str
        assert "OrderSummary" in repr_str
        assert "order_summaries" in repr_str
        assert "disabled" in repr_str

    def test_repr_with_tracing(self) -> None:
        """Test string representation with tracing enabled."""
        mock_conn = MagicMock()
        repo = SQLiteReadModelRepository(mock_conn, OrderSummary, enable_tracing=True)

        repr_str = repr(repo)
        assert "enabled" in repr_str


class TestFilterToSQL:
    """Tests for filter to SQL conversion with SQLite syntax."""

    @pytest.fixture
    def repo(self) -> SQLiteReadModelRepository[OrderSummary]:
        """Create repository for testing."""
        mock_conn = MagicMock()
        return SQLiteReadModelRepository(mock_conn, OrderSummary, enable_tracing=False)

    def test_filter_eq(self, repo: SQLiteReadModelRepository[OrderSummary]) -> None:
        """Test equality filter SQL generation with positional parameter."""
        clause, params = repo._filter_to_sql(Filter.eq("status", "active"))

        assert clause == "status = ?"
        assert params == ["active"]

    def test_filter_ne(self, repo: SQLiteReadModelRepository[OrderSummary]) -> None:
        """Test not-equal filter SQL generation."""
        clause, params = repo._filter_to_sql(Filter.ne("status", "cancelled"))

        assert clause == "status != ?"
        assert params == ["cancelled"]

    def test_filter_gt(self, repo: SQLiteReadModelRepository[OrderSummary]) -> None:
        """Test greater-than filter SQL generation."""
        clause, params = repo._filter_to_sql(Filter.gt("total_amount", 100))

        assert clause == "total_amount > ?"
        assert params == [100]

    def test_filter_gte(self, repo: SQLiteReadModelRepository[OrderSummary]) -> None:
        """Test greater-than-or-equal filter SQL generation."""
        clause, params = repo._filter_to_sql(Filter.gte("total_amount", 100))

        assert clause == "total_amount >= ?"
        assert params == [100]

    def test_filter_lt(self, repo: SQLiteReadModelRepository[OrderSummary]) -> None:
        """Test less-than filter SQL generation."""
        clause, params = repo._filter_to_sql(Filter.lt("total_amount", 50))

        assert clause == "total_amount < ?"
        assert params == [50]

    def test_filter_lte(self, repo: SQLiteReadModelRepository[OrderSummary]) -> None:
        """Test less-than-or-equal filter SQL generation."""
        clause, params = repo._filter_to_sql(Filter.lte("total_amount", 50))

        assert clause == "total_amount <= ?"
        assert params == [50]

    def test_filter_in(self, repo: SQLiteReadModelRepository[OrderSummary]) -> None:
        """Test IN filter SQL generation with multiple placeholders."""
        clause, params = repo._filter_to_sql(Filter.in_("status", ["a", "b", "c"]))

        assert clause == "status IN (?,?,?)"
        assert params == ["a", "b", "c"]

    def test_filter_not_in(self, repo: SQLiteReadModelRepository[OrderSummary]) -> None:
        """Test NOT IN filter SQL generation."""
        clause, params = repo._filter_to_sql(Filter.not_in("status", ["c", "d"]))

        assert clause == "status NOT IN (?,?)"
        assert params == ["c", "d"]

    def test_filter_unknown_operator(self, repo: SQLiteReadModelRepository[OrderSummary]) -> None:
        """Test unknown operator raises ValueError."""
        # Create a filter with an invalid operator by directly constructing
        invalid_filter = Filter(field="status", operator="invalid", value="x")  # type: ignore[arg-type]

        with pytest.raises(ValueError, match="Unknown operator: invalid"):
            repo._filter_to_sql(invalid_filter)

    def test_filter_with_uuid_value(self, repo: SQLiteReadModelRepository[OrderSummary]) -> None:
        """Test UUID values are converted to strings."""
        test_uuid = uuid4()
        clause, params = repo._filter_to_sql(Filter.eq("id", test_uuid))

        assert clause == "id = ?"
        assert params == [str(test_uuid)]

    def test_filter_in_with_uuid_values(
        self, repo: SQLiteReadModelRepository[OrderSummary]
    ) -> None:
        """Test UUID values in list are converted to strings."""
        uuid1, uuid2 = uuid4(), uuid4()
        clause, params = repo._filter_to_sql(Filter.in_("id", [uuid1, uuid2]))

        assert clause == "id IN (?,?)"
        assert params == [str(uuid1), str(uuid2)]


class TestBuildSelectQuery:
    """Tests for SELECT query building with SQLite syntax."""

    @pytest.fixture
    def repo(self) -> SQLiteReadModelRepository[OrderSummary]:
        """Create repository for testing."""
        mock_conn = MagicMock()
        return SQLiteReadModelRepository(mock_conn, OrderSummary, enable_tracing=False)

    def test_basic_select(self, repo: SQLiteReadModelRepository[OrderSummary]) -> None:
        """Test basic SELECT query building."""
        query = Query()
        sql, params = repo._build_select_query(query)

        assert "SELECT" in sql
        assert "FROM order_summaries" in sql
        assert "WHERE" in sql
        assert "deleted_at IS NULL" in sql
        assert params == ()

    def test_select_with_filter(self, repo: SQLiteReadModelRepository[OrderSummary]) -> None:
        """Test SELECT with filter using positional parameters."""
        query = Query(filters=[Filter.eq("status", "active")])
        sql, params = repo._build_select_query(query)

        assert "WHERE" in sql
        assert "deleted_at IS NULL" in sql
        assert "status = ?" in sql
        assert params == ("active",)

    def test_select_with_multiple_filters(
        self, repo: SQLiteReadModelRepository[OrderSummary]
    ) -> None:
        """Test SELECT with multiple filters."""
        query = Query(
            filters=[
                Filter.eq("status", "active"),
                Filter.gt("total_amount", Decimal("100")),
            ]
        )
        sql, params = repo._build_select_query(query)

        assert "status = ?" in sql
        assert "total_amount > ?" in sql
        assert " AND " in sql
        assert "active" in params
        assert Decimal("100") in params

    def test_select_with_ordering(self, repo: SQLiteReadModelRepository[OrderSummary]) -> None:
        """Test SELECT with ordering."""
        query = Query(order_by="created_at", order_direction="desc")
        sql, params = repo._build_select_query(query)

        assert "ORDER BY created_at DESC" in sql

    def test_select_with_ordering_asc(self, repo: SQLiteReadModelRepository[OrderSummary]) -> None:
        """Test SELECT with ascending ordering."""
        query = Query(order_by="order_number", order_direction="asc")
        sql, params = repo._build_select_query(query)

        assert "ORDER BY order_number ASC" in sql

    def test_select_with_limit(self, repo: SQLiteReadModelRepository[OrderSummary]) -> None:
        """Test SELECT with limit."""
        query = Query(limit=10)
        sql, params = repo._build_select_query(query)

        assert "LIMIT 10" in sql

    def test_select_with_offset(self, repo: SQLiteReadModelRepository[OrderSummary]) -> None:
        """Test SELECT with offset."""
        query = Query(offset=5)
        sql, params = repo._build_select_query(query)

        assert "OFFSET 5" in sql

    def test_select_with_pagination(self, repo: SQLiteReadModelRepository[OrderSummary]) -> None:
        """Test SELECT with pagination (limit and offset)."""
        query = Query(limit=10, offset=20)
        sql, params = repo._build_select_query(query)

        assert "LIMIT 10" in sql
        assert "OFFSET 20" in sql

    def test_select_include_deleted(self, repo: SQLiteReadModelRepository[OrderSummary]) -> None:
        """Test SELECT with include_deleted excludes soft-delete filter."""
        query = Query(include_deleted=True)
        sql, params = repo._build_select_query(query)

        # Should not have deleted_at filter since include_deleted=True
        assert "deleted_at IS NULL" not in sql

    def test_select_full_query(self, repo: SQLiteReadModelRepository[OrderSummary]) -> None:
        """Test SELECT with all query options."""
        query = Query(
            filters=[Filter.eq("status", "active")],
            order_by="created_at",
            order_direction="desc",
            limit=10,
            offset=0,
        )
        sql, params = repo._build_select_query(query)

        assert "SELECT" in sql
        assert "FROM order_summaries" in sql
        assert "WHERE" in sql
        assert "deleted_at IS NULL" in sql
        assert "status = ?" in sql
        assert "ORDER BY created_at DESC" in sql
        assert "LIMIT 10" in sql
        assert "active" in params


class TestBuildCountQuery:
    """Tests for COUNT query building."""

    @pytest.fixture
    def repo(self) -> SQLiteReadModelRepository[OrderSummary]:
        """Create repository for testing."""
        mock_conn = MagicMock()
        return SQLiteReadModelRepository(mock_conn, OrderSummary, enable_tracing=False)

    def test_basic_count(self, repo: SQLiteReadModelRepository[OrderSummary]) -> None:
        """Test basic COUNT query building."""
        query = Query()
        sql, params = repo._build_count_query(query)

        assert "SELECT COUNT(*)" in sql
        assert "FROM order_summaries" in sql
        assert "deleted_at IS NULL" in sql
        assert params == ()

    def test_count_with_filter(self, repo: SQLiteReadModelRepository[OrderSummary]) -> None:
        """Test COUNT with filter."""
        query = Query(filters=[Filter.eq("status", "shipped")])
        sql, params = repo._build_count_query(query)

        assert "SELECT COUNT(*)" in sql
        assert "status = ?" in sql
        assert "shipped" in params

    def test_count_include_deleted(self, repo: SQLiteReadModelRepository[OrderSummary]) -> None:
        """Test COUNT with include_deleted."""
        query = Query(include_deleted=True)
        sql, params = repo._build_count_query(query)

        assert "deleted_at IS NULL" not in sql


class TestRowToModel:
    """Tests for row to model conversion with SQLite type handling."""

    @pytest.fixture
    def repo(self) -> SQLiteReadModelRepository[OrderSummary]:
        """Create repository for testing."""
        mock_conn = MagicMock()
        return SQLiteReadModelRepository(mock_conn, OrderSummary, enable_tracing=False)

    def test_row_to_model_with_string_types(
        self, repo: SQLiteReadModelRepository[OrderSummary]
    ) -> None:
        """Test converting database row with SQLite TEXT types to model."""
        model_id = uuid4()
        now = datetime.now(UTC)

        # SQLite stores UUID and datetime as TEXT
        row = (
            str(model_id),  # id as TEXT
            now.isoformat(),  # created_at as TEXT
            now.isoformat(),  # updated_at as TEXT
            1,  # version
            None,  # deleted_at
            "ORD-001",  # order_number
            "pending",  # status
            Decimal("99.99"),  # total_amount (stored as REAL/NUMERIC)
        )

        model = repo._row_to_model(row)

        assert model.id == model_id
        assert model.created_at.replace(microsecond=0) == now.replace(microsecond=0)
        assert model.updated_at.replace(microsecond=0) == now.replace(microsecond=0)
        assert model.version == 1
        assert model.deleted_at is None
        assert model.order_number == "ORD-001"
        assert model.status == "pending"
        # Decimal comparison - SQLite may store as float
        assert abs(model.total_amount - Decimal("99.99")) < Decimal("0.01")

    def test_row_to_model_with_deleted_at(
        self, repo: SQLiteReadModelRepository[OrderSummary]
    ) -> None:
        """Test converting row with deleted_at timestamp."""
        model_id = uuid4()
        now = datetime.now(UTC)
        deleted = datetime.now(UTC)

        row = (
            str(model_id),
            now.isoformat(),
            now.isoformat(),
            1,
            deleted.isoformat(),  # deleted_at as TEXT
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
        """Test converting timestamps with Z suffix (common in SQLite)."""
        model_id = uuid4()

        row = (
            str(model_id),
            "2024-01-15T10:30:00Z",  # Z suffix instead of +00:00
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
        """Create repository for testing."""
        mock_conn = MagicMock()
        return SQLiteReadModelRepository(mock_conn, OrderSummary, enable_tracing=False)

    def test_model_to_values(self, repo: SQLiteReadModelRepository[OrderSummary]) -> None:
        """Test converting model to SQL values tuple."""
        model_id = uuid4()
        model = OrderSummary(
            id=model_id,
            order_number="ORD-001",
            status="pending",
            total_amount=Decimal("99.99"),
        )

        now = datetime.now(UTC)
        values = repo._model_to_values(model, now)

        # Should be tuple with values in field_names order
        assert isinstance(values, tuple)
        assert len(values) == len(repo._field_names)

        # Check that id is converted to string
        assert values[0] == str(model_id)

        # Check that updated_at is overridden
        assert values[2] == now.isoformat()

    def test_model_to_values_preserves_order(
        self, repo: SQLiteReadModelRepository[OrderSummary]
    ) -> None:
        """Test that values are in correct field order."""
        model = OrderSummary(
            id=uuid4(),
            order_number="ORD-001",
            status="pending",
            total_amount=Decimal("99.99"),
        )

        now = datetime.now(UTC)
        values = repo._model_to_values(model, now)

        # Values should be in field_names order
        # ['id', 'created_at', 'updated_at', 'version', 'deleted_at',
        #  'order_number', 'status', 'total_amount']
        assert values[5] == "ORD-001"  # order_number
        assert values[6] == "pending"  # status


class TestAsyncOperations:
    """Tests for async operations - primarily testing early returns and edge cases.

    Note: Full database integration tests are in P2-004. These unit tests
    focus on early return conditions that don't require database access.
    """

    @pytest.fixture
    def repo(self) -> SQLiteReadModelRepository[OrderSummary]:
        """Create repository with mock connection for testing."""
        mock_conn = MagicMock()
        return SQLiteReadModelRepository(mock_conn, OrderSummary, enable_tracing=False)

    @pytest.mark.asyncio
    async def test_get_many_empty_list_returns_empty(
        self, repo: SQLiteReadModelRepository[OrderSummary]
    ) -> None:
        """Test get_many with empty list returns empty list immediately."""
        result = await repo.get_many([])
        assert result == []

    @pytest.mark.asyncio
    async def test_save_many_empty_list_does_nothing(
        self, repo: SQLiteReadModelRepository[OrderSummary]
    ) -> None:
        """Test save_many with empty list returns immediately."""
        # Should not raise and should not call database
        await repo.save_many([])


class TestModelClassProperty:
    """Tests for model_class property."""

    def test_model_class_returns_correct_class(self) -> None:
        """Test model_class property returns the model class."""
        mock_conn = MagicMock()
        repo = SQLiteReadModelRepository(mock_conn, OrderSummary, enable_tracing=False)

        assert repo.model_class is OrderSummary

    def test_model_class_custom_model(self) -> None:
        """Test model_class property with custom model."""
        mock_conn = MagicMock()
        repo = SQLiteReadModelRepository(mock_conn, CustomTableModel, enable_tracing=False)

        assert repo.model_class is CustomTableModel


class TestTracingConfiguration:
    """Tests for tracing configuration."""

    def test_tracing_disabled_by_flag(self) -> None:
        """Test tracing can be disabled via constructor flag."""
        mock_conn = MagicMock()
        repo = SQLiteReadModelRepository(mock_conn, OrderSummary, enable_tracing=False)

        assert repo._enable_tracing is False

    def test_tracing_enabled_by_default(self) -> None:
        """Test tracing is enabled by default."""
        mock_conn = MagicMock()
        repo = SQLiteReadModelRepository(mock_conn, OrderSummary)

        assert repo._enable_tracing is True

    def test_tracing_can_be_explicitly_enabled(self) -> None:
        """Test tracing can be explicitly enabled."""
        mock_conn = MagicMock()
        repo = SQLiteReadModelRepository(mock_conn, OrderSummary, enable_tracing=True)

        assert repo._enable_tracing is True


class TestSQLSecurityAnnotations:
    """Tests to verify SQL security annotations are present."""

    def test_table_name_from_trusted_source(self) -> None:
        """Test that table names come from trusted class definitions."""
        mock_conn = MagicMock()
        repo = SQLiteReadModelRepository(mock_conn, OrderSummary, enable_tracing=False)

        # Table name should be derived from model class, not user input
        assert repo._table_name == OrderSummary.table_name()
        assert repo._table_name == "order_summaries"

    def test_custom_table_name_from_class(self) -> None:
        """Test custom table names come from class definition."""
        mock_conn = MagicMock()
        repo = SQLiteReadModelRepository(mock_conn, CustomTableModel, enable_tracing=False)

        assert repo._table_name == "custom_orders"
        assert repo._table_name == CustomTableModel.table_name()


class TestQueryBuilderIntegration:
    """Tests for Query builder integration."""

    @pytest.fixture
    def repo(self) -> SQLiteReadModelRepository[OrderSummary]:
        """Create repository for testing."""
        mock_conn = MagicMock()
        return SQLiteReadModelRepository(mock_conn, OrderSummary, enable_tracing=False)

    def test_query_with_filter_method(self, repo: SQLiteReadModelRepository[OrderSummary]) -> None:
        """Test Query with_filter method integration."""
        base_query = Query(limit=100)
        filtered = base_query.with_filter(Filter.eq("status", "active"))

        sql, params = repo._build_select_query(filtered)

        assert "status = ?" in sql
        assert "LIMIT 100" in sql
        assert "active" in params

    def test_query_with_order_method(self, repo: SQLiteReadModelRepository[OrderSummary]) -> None:
        """Test Query with_order method integration."""
        query = Query().with_order("created_at", "desc")

        sql, params = repo._build_select_query(query)

        assert "ORDER BY created_at DESC" in sql

    def test_query_with_pagination_method(
        self, repo: SQLiteReadModelRepository[OrderSummary]
    ) -> None:
        """Test Query with_pagination method integration."""
        query = Query().with_pagination(limit=20, offset=40)

        sql, params = repo._build_select_query(query)

        assert "LIMIT 20" in sql
        assert "OFFSET 40" in sql

    def test_query_with_deleted_method(self, repo: SQLiteReadModelRepository[OrderSummary]) -> None:
        """Test Query with_deleted method integration."""
        query = Query().with_deleted(True)

        sql, params = repo._build_select_query(query)

        assert "deleted_at IS NULL" not in sql

    def test_chained_query_methods(self, repo: SQLiteReadModelRepository[OrderSummary]) -> None:
        """Test chained Query methods integration."""
        query = (
            Query()
            .with_filter(Filter.eq("status", "shipped"))
            .with_filter(Filter.gt("total_amount", Decimal("50")))
            .with_order("created_at", "desc")
            .with_pagination(limit=10, offset=0)
        )

        sql, params = repo._build_select_query(query)

        assert "status = ?" in sql
        assert "total_amount > ?" in sql
        assert "ORDER BY created_at DESC" in sql
        assert "LIMIT 10" in sql
        assert "shipped" in params
        assert Decimal("50") in params


class TestSQLiteSyntaxDifferences:
    """Tests specific to SQLite syntax differences from PostgreSQL."""

    @pytest.fixture
    def repo(self) -> SQLiteReadModelRepository[OrderSummary]:
        """Create repository for testing."""
        mock_conn = MagicMock()
        return SQLiteReadModelRepository(mock_conn, OrderSummary, enable_tracing=False)

    def test_uses_positional_parameters(
        self, repo: SQLiteReadModelRepository[OrderSummary]
    ) -> None:
        """Test that SQLite uses ? positional parameters, not :named."""
        query = Query(filters=[Filter.eq("status", "active")])
        sql, params = repo._build_select_query(query)

        # Should use ? not :param
        assert "?" in sql
        assert ":p0" not in sql
        assert ":status" not in sql

    def test_in_operator_uses_multiple_placeholders(
        self, repo: SQLiteReadModelRepository[OrderSummary]
    ) -> None:
        """Test IN operator uses multiple ? placeholders, not ANY()."""
        query = Query(filters=[Filter.in_("status", ["a", "b", "c"])])
        sql, params = repo._build_select_query(query)

        # SQLite uses IN (?,?,?) not ANY(:param)
        assert "IN (?,?,?)" in sql
        assert "ANY" not in sql
        assert params == ("a", "b", "c")

    def test_not_in_operator_uses_multiple_placeholders(
        self, repo: SQLiteReadModelRepository[OrderSummary]
    ) -> None:
        """Test NOT IN operator uses multiple ? placeholders, not ALL()."""
        query = Query(filters=[Filter.not_in("status", ["x", "y"])])
        sql, params = repo._build_select_query(query)

        # SQLite uses NOT IN (?,?) not != ALL(:param)
        assert "NOT IN (?,?)" in sql
        assert "ALL" not in sql
        assert params == ("x", "y")


class TestExportFromModule:
    """Tests that SQLiteReadModelRepository is properly exported."""

    def test_import_from_readmodels(self) -> None:
        """Test SQLiteReadModelRepository can be imported from eventsource.readmodels."""
        from eventsource.readmodels import SQLiteReadModelRepository as ImportedRepo

        assert ImportedRepo is SQLiteReadModelRepository

    def test_in_all_exports(self) -> None:
        """Test SQLiteReadModelRepository is in __all__."""
        from eventsource import readmodels

        assert "SQLiteReadModelRepository" in readmodels.__all__
