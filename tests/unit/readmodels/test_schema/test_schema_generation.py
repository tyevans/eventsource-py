"""Tests for schema and index generation functions."""

from typing import Any, ClassVar

from eventsource.adapters.sql.readmodel_schema import (
    generate_full_schema,
    generate_indexes,
    generate_schema,
)
from eventsource.ports.readmodels.model import ReadModel
from tests.unit.readmodels.test_schema._fixtures import (
    AllTypesModel,
    ComplexModel,
    CustomTableModel,
    IndexedModel,
    ModelWithCustomSqlType,
    ModelWithDefaults,
    SimpleModel,
)


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
