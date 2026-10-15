"""Tests for schema and index generation functions."""

from typing import Any, ClassVar

from eventsource.adapters.sql.readmodel_schema import (
    generate_full_schema,
    generate_indexes,
    generate_schema,
)
from eventsource.ports.readmodels.model import ReadModel
from tests.unit.readmodels.test_schema import _fixtures as fx


def _check(sql: str, clauses: list[str]) -> None:
    for clause in clauses:
        assert clause in sql


class TestGenerateSchema:
    """Tests for generate_schema function."""

    def test_simple_model_postgresql(self) -> None:
        sql = generate_schema(fx.SimpleModel, dialect="postgresql")
        _check(
            sql,
            [
                "CREATE TABLE IF NOT EXISTS simple_models",
                "id UUID PRIMARY KEY",
                "name VARCHAR(255) NOT NULL",
                "count INTEGER NOT NULL DEFAULT 0",
                "created_at TIMESTAMP WITH TIME ZONE NOT NULL",
                "updated_at TIMESTAMP WITH TIME ZONE NOT NULL",
                "version INTEGER NOT NULL DEFAULT 1",
                "deleted_at TIMESTAMP WITH TIME ZONE",
            ],
        )
        assert "deleted_at TIMESTAMP WITH TIME ZONE NOT NULL" not in sql

    def test_simple_model_sqlite(self) -> None:
        sql = generate_schema(fx.SimpleModel, dialect="sqlite")
        assert "CREATE TABLE IF NOT EXISTS simple_models" in sql
        assert "id TEXT PRIMARY KEY" in sql and "name TEXT NOT NULL" in sql
        assert "count INTEGER NOT NULL DEFAULT 0" in sql and "created_at TEXT NOT NULL" in sql
        assert "updated_at TEXT NOT NULL" in sql and "version INTEGER NOT NULL DEFAULT 1" in sql

    def test_complex_model_postgresql(self) -> None:
        sql = generate_schema(fx.ComplexModel, dialect="postgresql")
        assert "order_number VARCHAR(255) NOT NULL" in sql and "status VARCHAR(255) NOT NULL" in sql
        assert "total_amount DECIMAL(18, 6) NOT NULL" in sql
        assert "is_priority BOOLEAN NOT NULL DEFAULT FALSE" in sql
        assert "tags JSONB" in sql and "metadata JSONB" in sql

    def test_complex_model_sqlite(self) -> None:
        sql = generate_schema(fx.ComplexModel, dialect="sqlite")
        assert "order_number TEXT NOT NULL" in sql and "status TEXT NOT NULL" in sql
        assert "total_amount REAL NOT NULL" in sql
        assert "is_priority INTEGER NOT NULL DEFAULT 0" in sql
        assert "tags TEXT" in sql and "metadata TEXT" in sql

    def test_all_types_postgresql(self) -> None:
        sql = generate_schema(fx.AllTypesModel, dialect="postgresql")
        assert "string_field VARCHAR(255) NOT NULL" in sql and "int_field INTEGER NOT NULL" in sql
        assert "float_field DOUBLE PRECISION NOT NULL" in sql
        assert (
            "decimal_field DECIMAL(18, 6) NOT NULL" in sql and "bool_field BOOLEAN NOT NULL" in sql
        )
        assert "datetime_field TIMESTAMP WITH TIME ZONE NOT NULL" in sql
        assert "date_field DATE NOT NULL" in sql and "uuid_field UUID NOT NULL" in sql
        assert "bytes_field BYTEA NOT NULL" in sql and "list_field JSONB NOT NULL" in sql
        assert "dict_field JSONB NOT NULL" in sql

    def test_all_types_sqlite(self) -> None:
        sql = generate_schema(fx.AllTypesModel, dialect="sqlite")
        assert "string_field TEXT NOT NULL" in sql and "int_field INTEGER NOT NULL" in sql
        assert "float_field REAL NOT NULL" in sql and "decimal_field REAL NOT NULL" in sql
        assert "bool_field INTEGER NOT NULL" in sql and "datetime_field TEXT NOT NULL" in sql
        assert "date_field TEXT NOT NULL" in sql and "uuid_field TEXT NOT NULL" in sql
        assert "bytes_field BLOB NOT NULL" in sql and "list_field TEXT NOT NULL" in sql
        assert "dict_field TEXT NOT NULL" in sql

    def test_custom_table_name(self) -> None:
        sql = generate_schema(fx.CustomTableModel, dialect="postgresql")
        assert "CREATE TABLE IF NOT EXISTS my_custom_table" in sql
        assert "custom_table_models" not in sql

    def test_if_not_exists_default(self) -> None:
        assert "IF NOT EXISTS" in generate_schema(fx.SimpleModel, dialect="postgresql")

    def test_if_not_exists_disabled(self) -> None:
        sql = generate_schema(fx.SimpleModel, dialect="postgresql", if_not_exists=False)
        assert "IF NOT EXISTS" not in sql and "CREATE TABLE simple_models" in sql

    def test_optional_field_no_not_null(self) -> None:
        sql = generate_schema(fx.ModelWithDefaults, dialect="postgresql")
        assert "optional_string VARCHAR(255)" in sql
        assert "optional_string VARCHAR(255) NOT NULL" not in sql

    def test_defaults_postgresql(self) -> None:
        sql = generate_schema(fx.ModelWithDefaults, dialect="postgresql")
        assert "string_with_default VARCHAR(255) NOT NULL DEFAULT 'default_value'" in sql
        assert "int_with_default INTEGER NOT NULL DEFAULT 42" in sql
        assert "float_with_default DOUBLE PRECISION NOT NULL DEFAULT 3.14" in sql
        assert "bool_true_default BOOLEAN NOT NULL DEFAULT TRUE" in sql
        assert "bool_false_default BOOLEAN NOT NULL DEFAULT FALSE" in sql
        assert "decimal_default DECIMAL(18, 6) NOT NULL DEFAULT 99.99" in sql

    def test_defaults_sqlite(self) -> None:
        sql = generate_schema(fx.ModelWithDefaults, dialect="sqlite")
        assert "string_with_default TEXT NOT NULL DEFAULT 'default_value'" in sql
        assert "int_with_default INTEGER NOT NULL DEFAULT 42" in sql
        assert "float_with_default REAL NOT NULL DEFAULT 3.14" in sql
        assert "bool_true_default INTEGER NOT NULL DEFAULT 1" in sql
        assert "bool_false_default INTEGER NOT NULL DEFAULT 0" in sql
        assert "decimal_default REAL NOT NULL DEFAULT 99.99" in sql

    def test_custom_sql_type_dialect_specific(self) -> None:
        for dialect in ("postgresql", "sqlite"):
            assert "description TEXT NOT NULL" in generate_schema(
                fx.ModelWithCustomSqlType, dialect=dialect
            )

    def test_custom_sql_type_single(self) -> None:
        for dialect in ("postgresql", "sqlite"):
            assert "short_code CHAR(10) NOT NULL" in generate_schema(
                fx.ModelWithCustomSqlType, dialect=dialect
            )

    def test_primary_key_on_id(self) -> None:
        assert "id UUID PRIMARY KEY" in generate_schema(fx.SimpleModel, dialect="postgresql")

    def test_statement_ends_with_semicolon(self) -> None:
        assert generate_schema(fx.SimpleModel, dialect="postgresql").strip().endswith(");")


class TestGenerateIndexes:
    """Tests for generate_indexes function."""

    def test_generates_soft_delete_index_postgresql(self) -> None:
        indexes = generate_indexes(fx.SimpleModel, dialect="postgresql")
        soft_delete_idx = [i for i in indexes if "deleted" in i]
        assert len(soft_delete_idx) == 1
        assert "WHERE deleted_at IS NOT NULL" in soft_delete_idx[0]
        assert "IF NOT EXISTS" in soft_delete_idx[0]

    def test_generates_soft_delete_index_sqlite(self) -> None:
        indexes = generate_indexes(fx.SimpleModel, dialect="sqlite")
        soft_delete_idx = [i for i in indexes if "deleted" in i]
        assert len(soft_delete_idx) == 1
        assert "WHERE" not in soft_delete_idx[0] and "IF NOT EXISTS" in soft_delete_idx[0]

    def test_custom_indexes_postgresql(self) -> None:
        indexes = generate_indexes(fx.IndexedModel, dialect="postgresql")
        assert len(indexes) >= 4
        status_idx = [i for i in indexes if "idx_indexed_models_status" in i]
        assert len(status_idx) == 1 and "WHERE deleted_at IS NULL" in status_idx[0]
        compound_idx = [i for i in indexes if "customer_id" in i]
        assert len(compound_idx) == 1 and "customer_id, created_at" in compound_idx[0]
        assert len([i for i in indexes if "idx_priority_custom" in i]) == 1

    def test_custom_indexes_sqlite(self) -> None:
        indexes = generate_indexes(fx.IndexedModel, dialect="sqlite")
        assert len(indexes) >= 4
        status_idx = [i for i in indexes if "status" in i and "deleted" not in i]
        assert len(status_idx) == 1 and "WHERE deleted_at IS NULL" not in status_idx[0]

    def test_index_name_generation(self) -> None:
        indexes = generate_indexes(fx.IndexedModel, dialect="postgresql")
        compound_idx = [i for i in indexes if "customer_id" in i]
        assert "idx_indexed_models_customer_id_created_at" in compound_idx[0]

    def test_model_without_custom_indexes(self) -> None:
        indexes = generate_indexes(fx.SimpleModel, dialect="postgresql")
        assert len(indexes) == 1 and "deleted" in indexes[0]

    def test_empty_fields_in_index_spec_ignored(self) -> None:
        class ModelWithEmptyIndex(ReadModel):
            __indexes__: ClassVar[list[dict[str, Any]]] = [{"fields": []}]
            value: int

        indexes = generate_indexes(ModelWithEmptyIndex, dialect="postgresql")
        assert len(indexes) == 1 and "deleted" in indexes[0]


class TestGenerateFullSchema:
    """Tests for generate_full_schema function."""

    def test_includes_table_and_indexes(self) -> None:
        sql = generate_full_schema(fx.SimpleModel, dialect="postgresql")
        assert "CREATE TABLE" in sql and "CREATE INDEX" in sql

    def test_parts_separated_by_blank_lines(self) -> None:
        assert "\n\n" in generate_full_schema(fx.SimpleModel, dialect="postgresql")

    def test_full_schema_postgresql(self) -> None:
        sql = generate_full_schema(fx.IndexedModel, dialect="postgresql")
        assert "CREATE TABLE IF NOT EXISTS indexed_models" in sql
        assert "id UUID PRIMARY KEY" in sql and "status VARCHAR(255) NOT NULL" in sql
        assert "CREATE INDEX IF NOT EXISTS idx_indexed_models_deleted" in sql
        assert "WHERE deleted_at IS NOT NULL" in sql and "idx_indexed_models_status" in sql
        assert "idx_indexed_models_customer_id_created_at" in sql and "idx_priority_custom" in sql

    def test_full_schema_sqlite(self) -> None:
        sql = generate_full_schema(fx.IndexedModel, dialect="sqlite")
        assert "CREATE TABLE IF NOT EXISTS indexed_models" in sql
        assert "id TEXT PRIMARY KEY" in sql and "status TEXT NOT NULL" in sql
        assert "CREATE INDEX IF NOT EXISTS idx_indexed_models_deleted" in sql
        assert "idx_indexed_models_status" in sql
