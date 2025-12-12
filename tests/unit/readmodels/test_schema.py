"""Unit tests for schema generation utilities."""

from datetime import date, datetime
from decimal import Decimal
from typing import Any, ClassVar
from uuid import UUID

from pydantic import Field

from eventsource.readmodels import ReadModel
from eventsource.readmodels.schema import (
    POSTGRESQL_TYPE_MAP,
    SQLITE_TYPE_MAP,
    _extract_type,
    _format_default,
    _get_custom_sql_type,
    _is_optional,
    generate_full_schema,
    generate_indexes,
    generate_schema,
)


# Test models
class SimpleModel(ReadModel):
    """A simple test model with basic fields."""

    name: str
    count: int = 0


class ComplexModel(ReadModel):
    """A complex test model with various types."""

    order_number: str
    status: str
    total_amount: Decimal
    is_priority: bool = False
    tags: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] | None = None


class AllTypesModel(ReadModel):
    """A model testing all supported types."""

    string_field: str
    int_field: int
    float_field: float
    decimal_field: Decimal
    bool_field: bool
    datetime_field: datetime
    date_field: date
    uuid_field: UUID
    bytes_field: bytes
    list_field: list[str]
    dict_field: dict[str, Any]


class CustomTableModel(ReadModel):
    """A model with custom table name."""

    __table_name__ = "my_custom_table"
    value: int


class IndexedModel(ReadModel):
    """A model with custom indexes."""

    __indexes__: ClassVar[list[dict[str, Any]]] = [
        {"fields": ["status"], "where": "deleted_at IS NULL"},
        {"fields": ["customer_id", "created_at"]},
        {"fields": ["priority"], "name": "idx_priority_custom"},
    ]
    status: str
    customer_id: UUID
    priority: int = 0


class ModelWithDefaults(ReadModel):
    """A model testing various default values."""

    string_with_default: str = "default_value"
    int_with_default: int = 42
    float_with_default: float = 3.14
    bool_true_default: bool = True
    bool_false_default: bool = False
    decimal_default: Decimal = Decimal("99.99")
    optional_string: str | None = None


class ModelWithCustomSqlType(ReadModel):
    """A model using custom SQL type specification."""

    description: str = Field(
        ..., json_schema_extra={"sql_type": {"postgresql": "TEXT", "sqlite": "TEXT"}}
    )
    short_code: str = Field(..., json_schema_extra={"sql_type": "CHAR(10)"})


class TestTypeMaps:
    """Tests for type mapping constants."""

    def test_postgresql_type_map_has_expected_types(self) -> None:
        """Test that PostgreSQL type map contains all expected types."""
        assert UUID in POSTGRESQL_TYPE_MAP
        assert str in POSTGRESQL_TYPE_MAP
        assert int in POSTGRESQL_TYPE_MAP
        assert float in POSTGRESQL_TYPE_MAP
        assert Decimal in POSTGRESQL_TYPE_MAP
        assert bool in POSTGRESQL_TYPE_MAP
        assert datetime in POSTGRESQL_TYPE_MAP
        assert date in POSTGRESQL_TYPE_MAP
        assert dict in POSTGRESQL_TYPE_MAP
        assert list in POSTGRESQL_TYPE_MAP
        assert bytes in POSTGRESQL_TYPE_MAP

    def test_sqlite_type_map_has_expected_types(self) -> None:
        """Test that SQLite type map contains all expected types."""
        assert UUID in SQLITE_TYPE_MAP
        assert str in SQLITE_TYPE_MAP
        assert int in SQLITE_TYPE_MAP
        assert float in SQLITE_TYPE_MAP
        assert Decimal in SQLITE_TYPE_MAP
        assert bool in SQLITE_TYPE_MAP
        assert datetime in SQLITE_TYPE_MAP
        assert date in SQLITE_TYPE_MAP
        assert dict in SQLITE_TYPE_MAP
        assert list in SQLITE_TYPE_MAP
        assert bytes in SQLITE_TYPE_MAP

    def test_postgresql_uses_appropriate_types(self) -> None:
        """Test PostgreSQL type mappings are correct."""
        assert POSTGRESQL_TYPE_MAP[UUID] == "UUID"
        assert POSTGRESQL_TYPE_MAP[str] == "VARCHAR(255)"
        assert POSTGRESQL_TYPE_MAP[int] == "INTEGER"
        assert POSTGRESQL_TYPE_MAP[float] == "DOUBLE PRECISION"
        assert POSTGRESQL_TYPE_MAP[Decimal] == "DECIMAL(18, 6)"
        assert POSTGRESQL_TYPE_MAP[bool] == "BOOLEAN"
        assert POSTGRESQL_TYPE_MAP[datetime] == "TIMESTAMP WITH TIME ZONE"
        assert POSTGRESQL_TYPE_MAP[date] == "DATE"
        assert POSTGRESQL_TYPE_MAP[dict] == "JSONB"
        assert POSTGRESQL_TYPE_MAP[list] == "JSONB"
        assert POSTGRESQL_TYPE_MAP[bytes] == "BYTEA"

    def test_sqlite_uses_appropriate_types(self) -> None:
        """Test SQLite type mappings are correct."""
        assert SQLITE_TYPE_MAP[UUID] == "TEXT"
        assert SQLITE_TYPE_MAP[str] == "TEXT"
        assert SQLITE_TYPE_MAP[int] == "INTEGER"
        assert SQLITE_TYPE_MAP[float] == "REAL"
        assert SQLITE_TYPE_MAP[Decimal] == "REAL"
        assert SQLITE_TYPE_MAP[bool] == "INTEGER"
        assert SQLITE_TYPE_MAP[datetime] == "TEXT"
        assert SQLITE_TYPE_MAP[date] == "TEXT"
        assert SQLITE_TYPE_MAP[dict] == "TEXT"
        assert SQLITE_TYPE_MAP[list] == "TEXT"
        assert SQLITE_TYPE_MAP[bytes] == "BLOB"


class TestGenerateSchema:
    """Tests for generate_schema function."""

    def test_simple_model_postgresql(self) -> None:
        """Test schema generation for simple model in PostgreSQL."""
        sql = generate_schema(SimpleModel, dialect="postgresql")

        assert "CREATE TABLE IF NOT EXISTS simple_models" in sql
        assert "id UUID PRIMARY KEY" in sql
        assert "name VARCHAR(255) NOT NULL" in sql
        assert "count INTEGER NOT NULL DEFAULT 0" in sql
        assert "created_at TIMESTAMP WITH TIME ZONE NOT NULL" in sql
        assert "updated_at TIMESTAMP WITH TIME ZONE NOT NULL" in sql
        assert "version INTEGER NOT NULL DEFAULT 1" in sql
        # deleted_at is optional, should not have NOT NULL
        assert "deleted_at TIMESTAMP WITH TIME ZONE" in sql
        assert "deleted_at TIMESTAMP WITH TIME ZONE NOT NULL" not in sql

    def test_simple_model_sqlite(self) -> None:
        """Test schema generation for simple model in SQLite."""
        sql = generate_schema(SimpleModel, dialect="sqlite")

        assert "CREATE TABLE IF NOT EXISTS simple_models" in sql
        assert "id TEXT PRIMARY KEY" in sql
        assert "name TEXT NOT NULL" in sql
        assert "count INTEGER NOT NULL DEFAULT 0" in sql
        assert "created_at TEXT NOT NULL" in sql
        assert "updated_at TEXT NOT NULL" in sql
        assert "version INTEGER NOT NULL DEFAULT 1" in sql

    def test_complex_model_postgresql(self) -> None:
        """Test schema generation with various types."""
        sql = generate_schema(ComplexModel, dialect="postgresql")

        assert "order_number VARCHAR(255) NOT NULL" in sql
        assert "status VARCHAR(255) NOT NULL" in sql
        assert "total_amount DECIMAL(18, 6) NOT NULL" in sql
        assert "is_priority BOOLEAN NOT NULL DEFAULT FALSE" in sql
        assert "tags JSONB" in sql
        # metadata is Optional, should not have NOT NULL
        assert "metadata JSONB" in sql

    def test_complex_model_sqlite(self) -> None:
        """Test complex model schema in SQLite."""
        sql = generate_schema(ComplexModel, dialect="sqlite")

        assert "order_number TEXT NOT NULL" in sql
        assert "status TEXT NOT NULL" in sql
        assert "total_amount REAL NOT NULL" in sql
        assert "is_priority INTEGER NOT NULL DEFAULT 0" in sql
        assert "tags TEXT" in sql
        assert "metadata TEXT" in sql

    def test_all_types_postgresql(self) -> None:
        """Test all supported types in PostgreSQL."""
        sql = generate_schema(AllTypesModel, dialect="postgresql")

        assert "string_field VARCHAR(255) NOT NULL" in sql
        assert "int_field INTEGER NOT NULL" in sql
        assert "float_field DOUBLE PRECISION NOT NULL" in sql
        assert "decimal_field DECIMAL(18, 6) NOT NULL" in sql
        assert "bool_field BOOLEAN NOT NULL" in sql
        assert "datetime_field TIMESTAMP WITH TIME ZONE NOT NULL" in sql
        assert "date_field DATE NOT NULL" in sql
        assert "uuid_field UUID NOT NULL" in sql
        assert "bytes_field BYTEA NOT NULL" in sql
        assert "list_field JSONB NOT NULL" in sql
        assert "dict_field JSONB NOT NULL" in sql

    def test_all_types_sqlite(self) -> None:
        """Test all supported types in SQLite."""
        sql = generate_schema(AllTypesModel, dialect="sqlite")

        assert "string_field TEXT NOT NULL" in sql
        assert "int_field INTEGER NOT NULL" in sql
        assert "float_field REAL NOT NULL" in sql
        assert "decimal_field REAL NOT NULL" in sql
        assert "bool_field INTEGER NOT NULL" in sql
        assert "datetime_field TEXT NOT NULL" in sql
        assert "date_field TEXT NOT NULL" in sql
        assert "uuid_field TEXT NOT NULL" in sql
        assert "bytes_field BLOB NOT NULL" in sql
        assert "list_field TEXT NOT NULL" in sql
        assert "dict_field TEXT NOT NULL" in sql

    def test_custom_table_name(self) -> None:
        """Test that custom table name is used."""
        sql = generate_schema(CustomTableModel, dialect="postgresql")

        assert "CREATE TABLE IF NOT EXISTS my_custom_table" in sql
        assert "custom_table_models" not in sql

    def test_if_not_exists_default(self) -> None:
        """Test that IF NOT EXISTS is included by default."""
        sql = generate_schema(SimpleModel, dialect="postgresql")

        assert "IF NOT EXISTS" in sql

    def test_if_not_exists_disabled(self) -> None:
        """Test disabling IF NOT EXISTS clause."""
        sql = generate_schema(SimpleModel, dialect="postgresql", if_not_exists=False)

        assert "IF NOT EXISTS" not in sql
        assert "CREATE TABLE simple_models" in sql

    def test_optional_field_no_not_null(self) -> None:
        """Test that Optional fields do not get NOT NULL constraint."""
        sql = generate_schema(ModelWithDefaults, dialect="postgresql")

        # optional_string is Optional[str], should not have NOT NULL
        assert "optional_string VARCHAR(255)" in sql
        assert "optional_string VARCHAR(255) NOT NULL" not in sql

    def test_defaults_postgresql(self) -> None:
        """Test default value generation for PostgreSQL."""
        sql = generate_schema(ModelWithDefaults, dialect="postgresql")

        assert "string_with_default VARCHAR(255) NOT NULL DEFAULT 'default_value'" in sql
        assert "int_with_default INTEGER NOT NULL DEFAULT 42" in sql
        assert "float_with_default DOUBLE PRECISION NOT NULL DEFAULT 3.14" in sql
        assert "bool_true_default BOOLEAN NOT NULL DEFAULT TRUE" in sql
        assert "bool_false_default BOOLEAN NOT NULL DEFAULT FALSE" in sql
        assert "decimal_default DECIMAL(18, 6) NOT NULL DEFAULT 99.99" in sql

    def test_defaults_sqlite(self) -> None:
        """Test default value generation for SQLite."""
        sql = generate_schema(ModelWithDefaults, dialect="sqlite")

        assert "string_with_default TEXT NOT NULL DEFAULT 'default_value'" in sql
        assert "int_with_default INTEGER NOT NULL DEFAULT 42" in sql
        assert "float_with_default REAL NOT NULL DEFAULT 3.14" in sql
        assert "bool_true_default INTEGER NOT NULL DEFAULT 1" in sql
        assert "bool_false_default INTEGER NOT NULL DEFAULT 0" in sql
        assert "decimal_default REAL NOT NULL DEFAULT 99.99" in sql

    def test_custom_sql_type_dialect_specific(self) -> None:
        """Test custom SQL type specification with dialect-specific types."""
        sql = generate_schema(ModelWithCustomSqlType, dialect="postgresql")
        assert "description TEXT NOT NULL" in sql

        sql_sqlite = generate_schema(ModelWithCustomSqlType, dialect="sqlite")
        assert "description TEXT NOT NULL" in sql_sqlite

    def test_custom_sql_type_single(self) -> None:
        """Test custom SQL type specification with single type for all dialects."""
        sql = generate_schema(ModelWithCustomSqlType, dialect="postgresql")
        assert "short_code CHAR(10) NOT NULL" in sql

        sql_sqlite = generate_schema(ModelWithCustomSqlType, dialect="sqlite")
        assert "short_code CHAR(10) NOT NULL" in sql_sqlite

    def test_primary_key_on_id(self) -> None:
        """Test that id column has PRIMARY KEY constraint."""
        sql = generate_schema(SimpleModel, dialect="postgresql")

        assert "id UUID PRIMARY KEY" in sql

    def test_statement_ends_with_semicolon(self) -> None:
        """Test that SQL statement ends with semicolon."""
        sql = generate_schema(SimpleModel, dialect="postgresql")

        assert sql.strip().endswith(");")


class TestGenerateIndexes:
    """Tests for generate_indexes function."""

    def test_generates_soft_delete_index_postgresql(self) -> None:
        """Test that soft delete index is generated for PostgreSQL."""
        indexes = generate_indexes(SimpleModel, dialect="postgresql")

        assert len(indexes) >= 1
        soft_delete_idx = [i for i in indexes if "deleted" in i]
        assert len(soft_delete_idx) == 1
        assert "WHERE deleted_at IS NOT NULL" in soft_delete_idx[0]
        assert "IF NOT EXISTS" in soft_delete_idx[0]

    def test_generates_soft_delete_index_sqlite(self) -> None:
        """Test that soft delete index is generated for SQLite."""
        indexes = generate_indexes(SimpleModel, dialect="sqlite")

        assert len(indexes) >= 1
        soft_delete_idx = [i for i in indexes if "deleted" in i]
        assert len(soft_delete_idx) == 1
        # SQLite doesn't support partial indexes
        assert "WHERE" not in soft_delete_idx[0]
        assert "IF NOT EXISTS" in soft_delete_idx[0]

    def test_custom_indexes_postgresql(self) -> None:
        """Test custom index generation for PostgreSQL."""
        indexes = generate_indexes(IndexedModel, dialect="postgresql")

        # Should have soft delete index + 3 custom indexes
        assert len(indexes) >= 4

        # Check status index with WHERE clause
        # The status index has "deleted_at IS NULL" in WHERE, so use index name
        status_idx = [i for i in indexes if "idx_indexed_models_status" in i]
        assert len(status_idx) == 1
        assert "WHERE deleted_at IS NULL" in status_idx[0]

        # Check compound index
        compound_idx = [i for i in indexes if "customer_id" in i]
        assert len(compound_idx) == 1
        assert "customer_id, created_at" in compound_idx[0]

        # Check custom name index
        custom_name_idx = [i for i in indexes if "idx_priority_custom" in i]
        assert len(custom_name_idx) == 1

    def test_custom_indexes_sqlite(self) -> None:
        """Test custom index generation for SQLite."""
        indexes = generate_indexes(IndexedModel, dialect="sqlite")

        # Should have soft delete index + 3 custom indexes
        assert len(indexes) >= 4

        # SQLite doesn't support partial indexes, WHERE should be removed
        status_idx = [i for i in indexes if "status" in i and "deleted" not in i]
        assert len(status_idx) == 1
        # WHERE clause should not be in SQLite index
        assert "WHERE deleted_at IS NULL" not in status_idx[0]

    def test_index_name_generation(self) -> None:
        """Test automatic index name generation."""
        indexes = generate_indexes(IndexedModel, dialect="postgresql")

        # Check auto-generated name for compound index
        compound_idx = [i for i in indexes if "customer_id" in i]
        assert "idx_indexed_models_customer_id_created_at" in compound_idx[0]

    def test_model_without_custom_indexes(self) -> None:
        """Test model without custom indexes only gets soft delete index."""
        indexes = generate_indexes(SimpleModel, dialect="postgresql")

        assert len(indexes) == 1
        assert "deleted" in indexes[0]

    def test_empty_fields_in_index_spec_ignored(self) -> None:
        """Test that index specs with empty fields are ignored."""

        class ModelWithEmptyIndex(ReadModel):
            __indexes__: ClassVar[list[dict[str, Any]]] = [{"fields": []}]
            value: int

        indexes = generate_indexes(ModelWithEmptyIndex, dialect="postgresql")

        # Should only have soft delete index
        assert len(indexes) == 1
        assert "deleted" in indexes[0]


class TestGenerateFullSchema:
    """Tests for generate_full_schema function."""

    def test_includes_table_and_indexes(self) -> None:
        """Test that full schema includes both table and indexes."""
        sql = generate_full_schema(SimpleModel, dialect="postgresql")

        assert "CREATE TABLE" in sql
        assert "CREATE INDEX" in sql

    def test_parts_separated_by_blank_lines(self) -> None:
        """Test that table and indexes are separated by blank lines."""
        sql = generate_full_schema(SimpleModel, dialect="postgresql")

        assert "\n\n" in sql

    def test_full_schema_postgresql(self) -> None:
        """Test full schema generation for PostgreSQL."""
        sql = generate_full_schema(IndexedModel, dialect="postgresql")

        # Table
        assert "CREATE TABLE IF NOT EXISTS indexed_models" in sql
        assert "id UUID PRIMARY KEY" in sql
        assert "status VARCHAR(255) NOT NULL" in sql

        # Indexes
        assert "CREATE INDEX IF NOT EXISTS idx_indexed_models_deleted" in sql
        assert "WHERE deleted_at IS NOT NULL" in sql
        assert "idx_indexed_models_status" in sql
        assert "idx_indexed_models_customer_id_created_at" in sql
        assert "idx_priority_custom" in sql

    def test_full_schema_sqlite(self) -> None:
        """Test full schema generation for SQLite."""
        sql = generate_full_schema(IndexedModel, dialect="sqlite")

        # Table
        assert "CREATE TABLE IF NOT EXISTS indexed_models" in sql
        assert "id TEXT PRIMARY KEY" in sql
        assert "status TEXT NOT NULL" in sql

        # Indexes (without partial index support)
        assert "CREATE INDEX IF NOT EXISTS idx_indexed_models_deleted" in sql
        assert "idx_indexed_models_status" in sql


class TestExtractType:
    """Tests for _extract_type helper function."""

    def test_simple_types(self) -> None:
        """Test extraction of simple types."""
        assert _extract_type(str) is str
        assert _extract_type(int) is int
        assert _extract_type(float) is float
        assert _extract_type(bool) is bool

    def test_optional_type(self) -> None:
        """Test extraction of Optional types."""
        assert _extract_type(str | None) is str
        assert _extract_type(int | None) is int

    def test_list_type(self) -> None:
        """Test extraction of list types."""
        assert _extract_type(list[str]) is list
        assert _extract_type(list[int]) is list

    def test_dict_type(self) -> None:
        """Test extraction of dict types."""
        assert _extract_type(dict[str, Any]) is dict
        assert _extract_type(dict[str, int]) is dict

    def test_none_annotation(self) -> None:
        """Test handling of None annotation."""
        assert _extract_type(None) is str  # Default fallback


class TestIsOptional:
    """Tests for _is_optional helper function."""

    def test_optional_type(self) -> None:
        """Test detection of Optional types."""
        assert _is_optional(str | None) is True
        assert _is_optional(int | None) is True

    def test_non_optional_type(self) -> None:
        """Test detection of non-Optional types."""
        assert _is_optional(str) is False
        assert _is_optional(int) is False
        assert _is_optional(list[str]) is False


class TestFormatDefault:
    """Tests for _format_default helper function."""

    def test_boolean_postgresql(self) -> None:
        """Test boolean formatting for PostgreSQL."""
        assert _format_default(True, "postgresql") == "TRUE"
        assert _format_default(False, "postgresql") == "FALSE"

    def test_boolean_sqlite(self) -> None:
        """Test boolean formatting for SQLite."""
        assert _format_default(True, "sqlite") == "1"
        assert _format_default(False, "sqlite") == "0"

    def test_integer(self) -> None:
        """Test integer formatting."""
        assert _format_default(42, "postgresql") == "42"
        assert _format_default(0, "sqlite") == "0"
        assert _format_default(-10, "postgresql") == "-10"

    def test_float(self) -> None:
        """Test float formatting."""
        assert _format_default(3.14, "postgresql") == "3.14"
        assert _format_default(0.0, "sqlite") == "0.0"

    def test_string(self) -> None:
        """Test string formatting with quoting."""
        assert _format_default("hello", "postgresql") == "'hello'"
        assert _format_default("", "sqlite") == "''"

    def test_string_with_quotes(self) -> None:
        """Test string escaping of single quotes."""
        assert _format_default("it's", "postgresql") == "'it''s'"
        assert _format_default("test'quote'here", "sqlite") == "'test''quote''here'"

    def test_decimal(self) -> None:
        """Test Decimal formatting."""
        assert _format_default(Decimal("99.99"), "postgresql") == "99.99"
        assert _format_default(Decimal("0"), "sqlite") == "0"

    def test_complex_types_return_none(self) -> None:
        """Test that complex types return None."""
        assert _format_default([], "postgresql") is None
        assert _format_default({}, "sqlite") is None
        assert _format_default(lambda: None, "postgresql") is None


class TestGetCustomSqlType:
    """Tests for _get_custom_sql_type helper function."""

    def test_no_custom_type(self) -> None:
        """Test field without custom type."""
        field_info = SimpleModel.model_fields["name"]
        assert _get_custom_sql_type(field_info, "postgresql") is None

    def test_dialect_specific_type(self) -> None:
        """Test dialect-specific custom type."""
        field_info = ModelWithCustomSqlType.model_fields["description"]
        assert _get_custom_sql_type(field_info, "postgresql") == "TEXT"
        assert _get_custom_sql_type(field_info, "sqlite") == "TEXT"

    def test_single_type_for_all_dialects(self) -> None:
        """Test single custom type for all dialects."""
        field_info = ModelWithCustomSqlType.model_fields["short_code"]
        assert _get_custom_sql_type(field_info, "postgresql") == "CHAR(10)"
        assert _get_custom_sql_type(field_info, "sqlite") == "CHAR(10)"


class TestIntegration:
    """Integration tests for schema generation."""

    def test_generated_sql_is_valid_structure(self) -> None:
        """Test that generated SQL has valid structure."""
        sql = generate_full_schema(ComplexModel, dialect="postgresql")

        # Should have balanced parentheses
        assert sql.count("(") == sql.count(")")

        # Should have at least table and index creation
        assert sql.count("CREATE TABLE") == 1
        assert sql.count("CREATE INDEX") >= 1

    def test_all_base_fields_present(self) -> None:
        """Test that all base ReadModel fields are in generated schema."""
        sql = generate_schema(SimpleModel, dialect="postgresql")

        # All base fields should be present
        assert "id " in sql
        assert "created_at " in sql
        assert "updated_at " in sql
        assert "version " in sql
        assert "deleted_at " in sql

    def test_module_exports(self) -> None:
        """Test that schema utilities are exported from module."""
        from eventsource.readmodels import (
            POSTGRESQL_TYPE_MAP,
            SQLITE_TYPE_MAP,
            generate_full_schema,
            generate_indexes,
            generate_schema,
        )

        # Verify exports work
        assert callable(generate_schema)
        assert callable(generate_indexes)
        assert callable(generate_full_schema)
        assert isinstance(POSTGRESQL_TYPE_MAP, dict)
        assert isinstance(SQLITE_TYPE_MAP, dict)

    def test_schema_can_be_used_for_test_setup(self) -> None:
        """Test that schema generation works for typical test setup."""

        class TestOrderView(ReadModel):
            order_id: str
            customer_email: str
            total: Decimal
            item_count: int = 0
            is_shipped: bool = False
            notes: str | None = None

        # Generate for both dialects
        pg_sql = generate_full_schema(TestOrderView, dialect="postgresql")
        sqlite_sql = generate_full_schema(TestOrderView, dialect="sqlite")

        # Both should be non-empty valid SQL
        assert len(pg_sql) > 100
        assert len(sqlite_sql) > 100
        assert "CREATE TABLE" in pg_sql
        assert "CREATE TABLE" in sqlite_sql
