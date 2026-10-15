"""Type mapping and inspection utilities for read model schema generation."""

from __future__ import annotations

import types
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Union, get_args, get_origin
from uuid import UUID

from pydantic.fields import FieldInfo

POSTGRESQL_TYPE_MAP: dict[type, str] = {
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

SQLITE_TYPE_MAP: dict[type, str] = {
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


def _extract_type(annotation: Any) -> type:
    """Extract the base Python type from a Pydantic field type annotation."""
    if annotation is None:
        return str

    origin = get_origin(annotation)
    if origin is not None:
        args = get_args(annotation)
        if origin is type(None):
            return type(None)
        if origin is Union or origin is types.UnionType:
            for arg in args:
                if arg is not type(None):
                    return _extract_type(arg)
        if origin is list:
            return list
        if origin is dict:
            return dict
        if args and args[0] is not type(None):
            return _extract_type(args[0])
        return origin if isinstance(origin, type) else type(origin)

    return annotation if isinstance(annotation, type) else type(annotation)


def _is_optional(annotation: Any) -> bool:
    """Check if a type annotation is Optional (Union with None)."""
    origin = get_origin(annotation)
    if origin is Union or origin is types.UnionType:
        return type(None) in get_args(annotation)
    return False


def _format_default(value: Any, dialect: str) -> str | None:
    """Format a Python default value as a SQL literal for the specified dialect."""
    if isinstance(value, bool):
        if dialect == "sqlite":
            return "1" if value else "0"
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float, Decimal)):
        return str(value)
    if isinstance(value, str):
        escaped = value.replace("'", "''")
        return f"'{escaped}'"
    return None


def _get_custom_sql_type(field_info: FieldInfo, dialect: str) -> str | None:
    """Get custom SQL type from field metadata (`json_schema_extra['sql_type']`) if specified."""
    extra = field_info.json_schema_extra
    if not isinstance(extra, dict):
        return None
    sql_type_spec = extra.get("sql_type")
    if isinstance(sql_type_spec, str):
        return sql_type_spec
    if isinstance(sql_type_spec, dict):
        value = sql_type_spec.get(dialect)
        return value if isinstance(value, str) else None
    return None


__all__ = [
    "POSTGRESQL_TYPE_MAP",
    "SQLITE_TYPE_MAP",
    "_extract_type",
    "_format_default",
    "_get_custom_sql_type",
    "_is_optional",
]
