"""Tests for schema generation helper functions and type mappings."""

from datetime import date, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from eventsource.adapters.sql.readmodel_schema import (
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
from eventsource.ports.readmodels.model import ReadModel
from tests.unit.readmodels.test_schema._fixtures import (
    ComplexModel,
    ModelWithCustomSqlType,
    SimpleModel,
)

_EXPECTED_TYPES = (UUID, str, int, float, Decimal, bool, datetime, date, dict, list, bytes)


class TestTypeMaps:
    def test_postgresql_type_map_has_expected_types(self) -> None:
        for t in _EXPECTED_TYPES:
            assert t in POSTGRESQL_TYPE_MAP

    def test_sqlite_type_map_has_expected_types(self) -> None:
        for t in _EXPECTED_TYPES:
            assert t in SQLITE_TYPE_MAP

    def test_postgresql_uses_appropriate_types(self) -> None:
        expected = {
            UUID: "UUID",
            str: "VARCHAR(255)",
            int: "INTEGER",
            float: "DOUBLE PRECISION",
            Decimal: "DECIMAL(18, 6)",
            bool: "BOOLEAN",
            datetime: "TIMESTAMP WITH TIME ZONE",
            date: "DATE",
            dict: "JSONB",
            list: "JSONB",
            bytes: "BYTEA",
        }
        for k, v in expected.items():
            assert POSTGRESQL_TYPE_MAP[k] == v

    def test_sqlite_uses_appropriate_types(self) -> None:
        expected = {
            UUID: "TEXT",
            str: "TEXT",
            int: "INTEGER",
            float: "REAL",
            Decimal: "REAL",
            bool: "INTEGER",
            datetime: "TEXT",
            date: "TEXT",
            dict: "TEXT",
            list: "TEXT",
            bytes: "BLOB",
        }
        for k, v in expected.items():
            assert SQLITE_TYPE_MAP[k] == v


class TestExtractType:
    def test_simple_types(self) -> None:
        for t in (str, int, float, bool):
            assert _extract_type(t) is t

    def test_optional_type(self) -> None:
        assert _extract_type(str | None) is str
        assert _extract_type(int | None) is int

    def test_list_type(self) -> None:
        assert _extract_type(list[str]) is list
        assert _extract_type(list[int]) is list

    def test_dict_type(self) -> None:
        assert _extract_type(dict[str, Any]) is dict
        assert _extract_type(dict[str, int]) is dict

    def test_none_annotation(self) -> None:
        assert _extract_type(None) is str


class TestIsOptional:
    def test_optional_type(self) -> None:
        assert _is_optional(str | None) is True
        assert _is_optional(int | None) is True

    def test_non_optional_type(self) -> None:
        assert _is_optional(str) is False
        assert _is_optional(int) is False
        assert _is_optional(list[str]) is False


class TestFormatDefault:
    def test_boolean_postgresql(self) -> None:
        assert _format_default(True, "postgresql") == "TRUE"
        assert _format_default(False, "postgresql") == "FALSE"

    def test_boolean_sqlite(self) -> None:
        assert _format_default(True, "sqlite") == "1"
        assert _format_default(False, "sqlite") == "0"

    def test_integer(self) -> None:
        assert _format_default(42, "postgresql") == "42"
        assert _format_default(0, "sqlite") == "0"
        assert _format_default(-10, "postgresql") == "-10"

    def test_float(self) -> None:
        assert _format_default(3.14, "postgresql") == "3.14"
        assert _format_default(0.0, "sqlite") == "0.0"

    def test_string(self) -> None:
        assert _format_default("hello", "postgresql") == "'hello'"
        assert _format_default("", "sqlite") == "''"

    def test_string_with_quotes(self) -> None:
        assert _format_default("it's", "postgresql") == "'it''s'"
        assert _format_default("test'quote'here", "sqlite") == "'test''quote''here'"

    def test_decimal(self) -> None:
        assert _format_default(Decimal("99.99"), "postgresql") == "99.99"
        assert _format_default(Decimal("0"), "sqlite") == "0"

    def test_complex_types_return_none(self) -> None:
        assert _format_default([], "postgresql") is None
        assert _format_default({}, "sqlite") is None
        assert _format_default(lambda: None, "postgresql") is None


class TestGetCustomSqlType:
    def test_no_custom_type(self) -> None:
        assert _get_custom_sql_type(SimpleModel.model_fields["name"], "postgresql") is None

    def test_dialect_specific_type(self) -> None:
        field_info = ModelWithCustomSqlType.model_fields["description"]
        assert _get_custom_sql_type(field_info, "postgresql") == "TEXT"
        assert _get_custom_sql_type(field_info, "sqlite") == "TEXT"

    def test_single_type_for_all_dialects(self) -> None:
        field_info = ModelWithCustomSqlType.model_fields["short_code"]
        assert _get_custom_sql_type(field_info, "postgresql") == "CHAR(10)"
        assert _get_custom_sql_type(field_info, "sqlite") == "CHAR(10)"


class TestIntegration:
    def test_generated_sql_is_valid_structure(self) -> None:
        sql = generate_full_schema(ComplexModel, dialect="postgresql")
        assert sql.count("(") == sql.count(")")
        assert sql.count("CREATE TABLE") == 1 and sql.count("CREATE INDEX") >= 1

    def test_all_base_fields_present(self) -> None:
        sql = generate_schema(SimpleModel, dialect="postgresql")
        for col in ("id ", "created_at ", "updated_at ", "version ", "deleted_at "):
            assert col in sql

    def test_module_exports(self) -> None:
        assert callable(generate_schema) and callable(generate_indexes)
        assert callable(generate_full_schema)
        assert isinstance(POSTGRESQL_TYPE_MAP, dict) and isinstance(SQLITE_TYPE_MAP, dict)

    def test_schema_can_be_used_for_test_setup(self) -> None:
        class TestOrderView(ReadModel):
            order_id: str
            customer_email: str
            total: Decimal
            item_count: int = 0
            is_shipped: bool = False
            notes: str | None = None

        pg_sql = generate_full_schema(TestOrderView, dialect="postgresql")
        sqlite_sql = generate_full_schema(TestOrderView, dialect="sqlite")
        assert len(pg_sql) > 100 and len(sqlite_sql) > 100
        assert "CREATE TABLE" in pg_sql and "CREATE TABLE" in sqlite_sql
