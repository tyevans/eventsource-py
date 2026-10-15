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
    generate_schema,
)
from eventsource.ports.readmodels.model import ReadModel
from tests.unit.readmodels.test_schema._fixtures import (
    ComplexModel,
    ModelWithCustomSqlType,
    SimpleModel,
)


class TestTypeMaps:
    def test_postgresql_type_map_has_expected_types(self) -> None:
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


class TestExtractType:
    def test_simple_types(self) -> None:
        assert _extract_type(str) is str
        assert _extract_type(int) is int
        assert _extract_type(float) is float
        assert _extract_type(bool) is bool

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
        field_info = SimpleModel.model_fields["name"]
        assert _get_custom_sql_type(field_info, "postgresql") is None

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
        assert sql.count("CREATE TABLE") == 1
        assert sql.count("CREATE INDEX") >= 1

    def test_all_base_fields_present(self) -> None:
        sql = generate_schema(SimpleModel, dialect="postgresql")
        assert "id " in sql
        assert "created_at " in sql
        assert "updated_at " in sql
        assert "version " in sql
        assert "deleted_at " in sql

    def test_module_exports(self) -> None:
        from eventsource.adapters.sql.readmodel_schema import (
            POSTGRESQL_TYPE_MAP,
            SQLITE_TYPE_MAP,
            generate_full_schema,
            generate_indexes,
            generate_schema,
        )

        assert callable(generate_schema)
        assert callable(generate_indexes)
        assert callable(generate_full_schema)
        assert isinstance(POSTGRESQL_TYPE_MAP, dict)
        assert isinstance(SQLITE_TYPE_MAP, dict)

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
        assert len(pg_sql) > 100
        assert len(sqlite_sql) > 100
        assert "CREATE TABLE" in pg_sql
        assert "CREATE TABLE" in sqlite_sql
