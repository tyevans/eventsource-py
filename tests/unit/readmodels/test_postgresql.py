"""Unit tests for PostgreSQLReadModelRepository.

These tests use mocking to verify correct SQL generation and behavior
without requiring a real PostgreSQL database connection.
"""

from decimal import Decimal
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from eventsource.readmodels import ReadModel
from eventsource.readmodels.postgresql import PostgreSQLReadModelRepository
from eventsource.readmodels.query import Filter, Query


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
        """Test repository construction stores model class."""
        mock_conn = MagicMock()
        repo = PostgreSQLReadModelRepository(mock_conn, OrderSummary, enable_tracing=False)

        assert repo._model_class == OrderSummary
        assert repo.model_class == OrderSummary

    def test_constructor_derives_table_name(self) -> None:
        """Test table name is derived from model class."""
        mock_conn = MagicMock()
        repo = PostgreSQLReadModelRepository(mock_conn, OrderSummary, enable_tracing=False)

        assert repo._table_name == "order_summaries"

    def test_constructor_custom_table_name(self) -> None:
        """Test custom table name is respected."""
        mock_conn = MagicMock()
        repo = PostgreSQLReadModelRepository(mock_conn, CustomTableModel, enable_tracing=False)

        assert repo._table_name == "custom_orders"

    def test_constructor_stores_field_names(self) -> None:
        """Test field names are extracted from model."""
        mock_conn = MagicMock()
        repo = PostgreSQLReadModelRepository(mock_conn, OrderSummary, enable_tracing=False)

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
        repo = PostgreSQLReadModelRepository(mock_conn, OrderSummary, enable_tracing=False)

        repr_str = repr(repo)
        assert "PostgreSQLReadModelRepository" in repr_str
        assert "OrderSummary" in repr_str
        assert "order_summaries" in repr_str
        assert "disabled" in repr_str

    def test_repr_with_tracing(self) -> None:
        """Test string representation with tracing enabled."""
        mock_conn = MagicMock()
        repo = PostgreSQLReadModelRepository(mock_conn, OrderSummary, enable_tracing=True)

        repr_str = repr(repo)
        assert "enabled" in repr_str


class TestFilterToSQL:
    """Tests for filter to SQL conversion."""

    @pytest.fixture
    def repo(self) -> PostgreSQLReadModelRepository[OrderSummary]:
        """Create repository for testing."""
        mock_conn = MagicMock()
        return PostgreSQLReadModelRepository(mock_conn, OrderSummary, enable_tracing=False)

    def test_filter_eq(self, repo: PostgreSQLReadModelRepository[OrderSummary]) -> None:
        """Test equality filter SQL generation."""
        clause, param = repo._filter_to_sql(Filter.eq("status", "active"), 0)

        assert clause == "status = :p0"
        assert param == "p0"

    def test_filter_ne(self, repo: PostgreSQLReadModelRepository[OrderSummary]) -> None:
        """Test not-equal filter SQL generation."""
        clause, param = repo._filter_to_sql(Filter.ne("status", "cancelled"), 1)

        assert clause == "status != :p1"
        assert param == "p1"

    def test_filter_gt(self, repo: PostgreSQLReadModelRepository[OrderSummary]) -> None:
        """Test greater-than filter SQL generation."""
        clause, param = repo._filter_to_sql(Filter.gt("total_amount", 100), 2)

        assert clause == "total_amount > :p2"
        assert param == "p2"

    def test_filter_gte(self, repo: PostgreSQLReadModelRepository[OrderSummary]) -> None:
        """Test greater-than-or-equal filter SQL generation."""
        clause, param = repo._filter_to_sql(Filter.gte("total_amount", 100), 3)

        assert clause == "total_amount >= :p3"
        assert param == "p3"

    def test_filter_lt(self, repo: PostgreSQLReadModelRepository[OrderSummary]) -> None:
        """Test less-than filter SQL generation."""
        clause, param = repo._filter_to_sql(Filter.lt("total_amount", 50), 4)

        assert clause == "total_amount < :p4"
        assert param == "p4"

    def test_filter_lte(self, repo: PostgreSQLReadModelRepository[OrderSummary]) -> None:
        """Test less-than-or-equal filter SQL generation."""
        clause, param = repo._filter_to_sql(Filter.lte("total_amount", 50), 5)

        assert clause == "total_amount <= :p5"
        assert param == "p5"

    def test_filter_in(self, repo: PostgreSQLReadModelRepository[OrderSummary]) -> None:
        """Test IN filter SQL generation."""
        clause, param = repo._filter_to_sql(Filter.in_("status", ["a", "b"]), 6)

        assert clause == "status = ANY(:p6)"
        assert param == "p6"

    def test_filter_not_in(self, repo: PostgreSQLReadModelRepository[OrderSummary]) -> None:
        """Test NOT IN filter SQL generation."""
        clause, param = repo._filter_to_sql(Filter.not_in("status", ["c", "d"]), 7)

        assert clause == "status != ALL(:p7)"
        assert param == "p7"

    def test_filter_unknown_operator(
        self, repo: PostgreSQLReadModelRepository[OrderSummary]
    ) -> None:
        """Test unknown operator raises ValueError."""
        # Create a filter with an invalid operator by directly constructing
        invalid_filter = Filter(field="status", operator="invalid", value="x")  # type: ignore[arg-type]

        with pytest.raises(ValueError, match="Unknown operator: invalid"):
            repo._filter_to_sql(invalid_filter, 0)


class TestBuildSelectQuery:
    """Tests for SELECT query building."""

    @pytest.fixture
    def repo(self) -> PostgreSQLReadModelRepository[OrderSummary]:
        """Create repository for testing."""
        mock_conn = MagicMock()
        return PostgreSQLReadModelRepository(mock_conn, OrderSummary, enable_tracing=False)

    def test_basic_select(self, repo: PostgreSQLReadModelRepository[OrderSummary]) -> None:
        """Test basic SELECT query building."""
        query = Query()
        sql, params = repo._build_select_query(query)

        assert "SELECT" in sql
        assert "FROM order_summaries" in sql
        assert "WHERE" in sql
        assert "deleted_at IS NULL" in sql
        assert params == {}

    def test_select_with_filter(self, repo: PostgreSQLReadModelRepository[OrderSummary]) -> None:
        """Test SELECT with filter."""
        query = Query(filters=[Filter.eq("status", "active")])
        sql, params = repo._build_select_query(query)

        assert "WHERE" in sql
        assert "deleted_at IS NULL" in sql
        assert "status = :p0" in sql
        assert params == {"p0": "active"}

    def test_select_with_multiple_filters(
        self, repo: PostgreSQLReadModelRepository[OrderSummary]
    ) -> None:
        """Test SELECT with multiple filters."""
        query = Query(
            filters=[
                Filter.eq("status", "active"),
                Filter.gt("total_amount", Decimal("100")),
            ]
        )
        sql, params = repo._build_select_query(query)

        assert "status = :p0" in sql
        assert "total_amount > :p1" in sql
        assert " AND " in sql
        assert params["p0"] == "active"
        assert params["p1"] == Decimal("100")

    def test_select_with_ordering(self, repo: PostgreSQLReadModelRepository[OrderSummary]) -> None:
        """Test SELECT with ordering."""
        query = Query(order_by="created_at", order_direction="desc")
        sql, params = repo._build_select_query(query)

        assert "ORDER BY created_at DESC" in sql

    def test_select_with_ordering_asc(
        self, repo: PostgreSQLReadModelRepository[OrderSummary]
    ) -> None:
        """Test SELECT with ascending ordering."""
        query = Query(order_by="order_number", order_direction="asc")
        sql, params = repo._build_select_query(query)

        assert "ORDER BY order_number ASC" in sql

    def test_select_with_limit(self, repo: PostgreSQLReadModelRepository[OrderSummary]) -> None:
        """Test SELECT with limit."""
        query = Query(limit=10)
        sql, params = repo._build_select_query(query)

        assert "LIMIT 10" in sql

    def test_select_with_offset(self, repo: PostgreSQLReadModelRepository[OrderSummary]) -> None:
        """Test SELECT with offset."""
        query = Query(offset=5)
        sql, params = repo._build_select_query(query)

        assert "OFFSET 5" in sql

    def test_select_with_pagination(
        self, repo: PostgreSQLReadModelRepository[OrderSummary]
    ) -> None:
        """Test SELECT with pagination (limit and offset)."""
        query = Query(limit=10, offset=20)
        sql, params = repo._build_select_query(query)

        assert "LIMIT 10" in sql
        assert "OFFSET 20" in sql

    def test_select_include_deleted(
        self, repo: PostgreSQLReadModelRepository[OrderSummary]
    ) -> None:
        """Test SELECT with include_deleted excludes soft-delete filter."""
        query = Query(include_deleted=True)
        sql, params = repo._build_select_query(query)

        # Should not have WHERE clause since there are no filters
        # and include_deleted=True
        assert "deleted_at IS NULL" not in sql

    def test_select_full_query(self, repo: PostgreSQLReadModelRepository[OrderSummary]) -> None:
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
        assert "status = :p0" in sql
        assert "ORDER BY created_at DESC" in sql
        assert "LIMIT 10" in sql
        assert params["p0"] == "active"


class TestBuildCountQuery:
    """Tests for COUNT query building."""

    @pytest.fixture
    def repo(self) -> PostgreSQLReadModelRepository[OrderSummary]:
        """Create repository for testing."""
        mock_conn = MagicMock()
        return PostgreSQLReadModelRepository(mock_conn, OrderSummary, enable_tracing=False)

    def test_basic_count(self, repo: PostgreSQLReadModelRepository[OrderSummary]) -> None:
        """Test basic COUNT query building."""
        query = Query()
        sql, params = repo._build_count_query(query)

        assert "SELECT COUNT(*)" in sql
        assert "FROM order_summaries" in sql
        assert "deleted_at IS NULL" in sql
        assert params == {}

    def test_count_with_filter(self, repo: PostgreSQLReadModelRepository[OrderSummary]) -> None:
        """Test COUNT with filter."""
        query = Query(filters=[Filter.eq("status", "shipped")])
        sql, params = repo._build_count_query(query)

        assert "SELECT COUNT(*)" in sql
        assert "status = :p0" in sql
        assert params["p0"] == "shipped"

    def test_count_include_deleted(self, repo: PostgreSQLReadModelRepository[OrderSummary]) -> None:
        """Test COUNT with include_deleted."""
        query = Query(include_deleted=True)
        sql, params = repo._build_count_query(query)

        assert "deleted_at IS NULL" not in sql


class TestRowToModel:
    """Tests for row to model conversion."""

    @pytest.fixture
    def repo(self) -> PostgreSQLReadModelRepository[OrderSummary]:
        """Create repository for testing."""
        mock_conn = MagicMock()
        return PostgreSQLReadModelRepository(mock_conn, OrderSummary, enable_tracing=False)

    def test_row_to_model(self, repo: PostgreSQLReadModelRepository[OrderSummary]) -> None:
        """Test converting database row to model."""
        from datetime import UTC, datetime

        model_id = uuid4()
        now = datetime.now(UTC)

        # Row values in field_names order
        row = (
            model_id,  # id
            now,  # created_at
            now,  # updated_at
            1,  # version
            None,  # deleted_at
            "ORD-001",  # order_number
            "pending",  # status
            Decimal("99.99"),  # total_amount
        )

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
    """Tests for async operations - primarily testing early returns and edge cases.

    Note: Full database integration tests are in P2-004. These unit tests
    focus on early return conditions that don't require database access.
    """

    @pytest.fixture
    def repo(self) -> PostgreSQLReadModelRepository[OrderSummary]:
        """Create repository with mock connection for testing."""
        mock_conn = MagicMock()
        return PostgreSQLReadModelRepository(mock_conn, OrderSummary, enable_tracing=False)

    @pytest.mark.asyncio
    async def test_get_many_empty_list_returns_empty(
        self, repo: PostgreSQLReadModelRepository[OrderSummary]
    ) -> None:
        """Test get_many with empty list returns empty list immediately."""
        result = await repo.get_many([])
        assert result == []

    @pytest.mark.asyncio
    async def test_save_many_empty_list_does_nothing(
        self, repo: PostgreSQLReadModelRepository[OrderSummary]
    ) -> None:
        """Test save_many with empty list returns immediately."""
        # Should not raise and should not call database
        await repo.save_many([])


class TestModelClassProperty:
    """Tests for model_class property."""

    def test_model_class_returns_correct_class(self) -> None:
        """Test model_class property returns the model class."""
        mock_conn = MagicMock()
        repo = PostgreSQLReadModelRepository(mock_conn, OrderSummary, enable_tracing=False)

        assert repo.model_class is OrderSummary

    def test_model_class_custom_model(self) -> None:
        """Test model_class property with custom model."""
        mock_conn = MagicMock()
        repo = PostgreSQLReadModelRepository(mock_conn, CustomTableModel, enable_tracing=False)

        assert repo.model_class is CustomTableModel


class TestTracingConfiguration:
    """Tests for tracing configuration."""

    def test_tracing_disabled_by_flag(self) -> None:
        """Test tracing can be disabled via constructor flag."""
        mock_conn = MagicMock()
        repo = PostgreSQLReadModelRepository(mock_conn, OrderSummary, enable_tracing=False)

        assert repo._enable_tracing is False

    def test_tracing_enabled_by_default(self) -> None:
        """Test tracing is enabled by default."""
        mock_conn = MagicMock()
        repo = PostgreSQLReadModelRepository(mock_conn, OrderSummary)

        assert repo._enable_tracing is True

    def test_tracing_can_be_explicitly_enabled(self) -> None:
        """Test tracing can be explicitly enabled."""
        mock_conn = MagicMock()
        repo = PostgreSQLReadModelRepository(mock_conn, OrderSummary, enable_tracing=True)

        assert repo._enable_tracing is True


class TestSQLSecurityAnnotations:
    """Tests to verify SQL security annotations are present."""

    def test_table_name_from_trusted_source(self) -> None:
        """Test that table names come from trusted class definitions."""
        mock_conn = MagicMock()
        repo = PostgreSQLReadModelRepository(mock_conn, OrderSummary, enable_tracing=False)

        # Table name should be derived from model class, not user input
        assert repo._table_name == OrderSummary.table_name()
        assert repo._table_name == "order_summaries"

    def test_custom_table_name_from_class(self) -> None:
        """Test custom table names come from class definition."""
        mock_conn = MagicMock()
        repo = PostgreSQLReadModelRepository(mock_conn, CustomTableModel, enable_tracing=False)

        assert repo._table_name == "custom_orders"
        assert repo._table_name == CustomTableModel.table_name()


class TestQueryBuilderIntegration:
    """Tests for Query builder integration."""

    @pytest.fixture
    def repo(self) -> PostgreSQLReadModelRepository[OrderSummary]:
        """Create repository for testing."""
        mock_conn = MagicMock()
        return PostgreSQLReadModelRepository(mock_conn, OrderSummary, enable_tracing=False)

    def test_query_with_filter_method(
        self, repo: PostgreSQLReadModelRepository[OrderSummary]
    ) -> None:
        """Test Query with_filter method integration."""
        base_query = Query(limit=100)
        filtered = base_query.with_filter(Filter.eq("status", "active"))

        sql, params = repo._build_select_query(filtered)

        assert "status = :p0" in sql
        assert "LIMIT 100" in sql
        assert params["p0"] == "active"

    def test_query_with_order_method(
        self, repo: PostgreSQLReadModelRepository[OrderSummary]
    ) -> None:
        """Test Query with_order method integration."""
        query = Query().with_order("created_at", "desc")

        sql, params = repo._build_select_query(query)

        assert "ORDER BY created_at DESC" in sql

    def test_query_with_pagination_method(
        self, repo: PostgreSQLReadModelRepository[OrderSummary]
    ) -> None:
        """Test Query with_pagination method integration."""
        query = Query().with_pagination(limit=20, offset=40)

        sql, params = repo._build_select_query(query)

        assert "LIMIT 20" in sql
        assert "OFFSET 40" in sql

    def test_query_with_deleted_method(
        self, repo: PostgreSQLReadModelRepository[OrderSummary]
    ) -> None:
        """Test Query with_deleted method integration."""
        query = Query().with_deleted(True)

        sql, params = repo._build_select_query(query)

        assert "deleted_at IS NULL" not in sql

    def test_chained_query_methods(self, repo: PostgreSQLReadModelRepository[OrderSummary]) -> None:
        """Test chained Query methods integration."""
        query = (
            Query()
            .with_filter(Filter.eq("status", "shipped"))
            .with_filter(Filter.gt("total_amount", Decimal("50")))
            .with_order("created_at", "desc")
            .with_pagination(limit=10, offset=0)
        )

        sql, params = repo._build_select_query(query)

        assert "status = :p0" in sql
        assert "total_amount > :p1" in sql
        assert "ORDER BY created_at DESC" in sql
        assert "LIMIT 10" in sql
        assert params["p0"] == "shipped"
        assert params["p1"] == Decimal("50")
