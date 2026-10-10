"""Type mapping and inspection utilities for read model schema generation."""

from __future__ import annotations

import types
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Union, get_args, get_origin
from uuid import UUID

from pydantic.fields import FieldInfo

# Type mappings for PostgreSQL
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

# Type mappings for SQLite
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
    """Extract the base type from a type annotation.

    Handles Optional[T], Union[T, None], T | None, and generic types to extract
    the primary type for SQL mapping.

    Args:
        annotation: Type annotation from Pydantic field

    Returns:
        The base Python type
    """
    if annotation is None:
        return str

    # Handle Optional[T] -> T
    origin = get_origin(annotation)
    if origin is not None:
        args = get_args(annotation)
        if origin is type(None):
            return type(None)
        # Union types (Optional is Union[T, None] or T | None in Python 3.10+)
        if origin is Union or origin is types.UnionType:
            for arg in args:
                if arg is not type(None):
                    return _extract_type(arg)
        # For list[T], dict[K, V], etc. - return the origin type
        if origin is list:
            return list
        if origin is dict:
            return dict
        # For other generic types, try to get the first arg
        if args and args[0] is not type(None):
            return _extract_type(args[0])
        return origin if isinstance(origin, type) else type(origin)

    return annotation if isinstance(annotation, type) else type(annotation)


def _is_optional(annotation: Any) -> bool:
    """Check if a type annotation is Optional (Union with None).

    Handles both typing.Union and types.UnionType (Python 3.10+ | syntax).

    Args:
        annotation: Type annotation to check

    Returns:
        True if the annotation allows None values
    """
    origin = get_origin(annotation)
    if origin is None:
        return False

    # Handle Union types (both typing.Union and types.UnionType for T | None syntax)
    if origin is Union or origin is types.UnionType:
        args = get_args(annotation)
        return type(None) in args

    return False


def _format_default(value: Any, dialect: str) -> str | None:
    """Format a Python default value for SQL.

    Converts Python values to appropriate SQL literal syntax for
    the specified dialect.

    Args:
        value: Python default value
        dialect: Database dialect ('postgresql' or 'sqlite')

    Returns:
        SQL literal string, or None if the value cannot be formatted
    """
    if isinstance(value, bool):
        if dialect == "sqlite":
            return "1" if value else "0"
        return "TRUE" if value else "FALSE"
    elif isinstance(value, (int, float)):
        return str(value)
    elif isinstance(value, str):
        # Escape single quotes
        escaped = value.replace("'", "''")
        return f"'{escaped}'"
    elif isinstance(value, Decimal):
        return str(value)

    # For complex types (lists, dicts, datetime factories), skip DEFAULT
    return None


def _get_custom_sql_type(field_info: FieldInfo, dialect: str) -> str | None:
    """Get custom SQL type from field metadata if specified.

    Allows users to override default type mapping by specifying
    sql_type in Field's json_schema_extra.

    Args:
        field_info: Pydantic FieldInfo for the field
        dialect: Database dialect ('postgresql' or 'sqlite')

    Returns:
        Custom SQL type string, or None if not specified
    """
    extra = field_info.json_schema_extra
    if extra is None:
        return None

    if isinstance(extra, dict):
        sql_type_spec = extra.get("sql_type")
        if sql_type_spec is None:
            return None

        if isinstance(sql_type_spec, str):
            # Single type for all dialects
            return sql_type_spec
        elif isinstance(sql_type_spec, dict):
            # Dialect-specific types
            value = sql_type_spec.get(dialect)
            if isinstance(value, str):
                return value
            return None

    return None


__all__ = [
    "POSTGRESQL_TYPE_MAP",
    "SQLITE_TYPE_MAP",
    "_extract_type",
    "_format_default",
    "_get_custom_sql_type",
    "_is_optional",
]
