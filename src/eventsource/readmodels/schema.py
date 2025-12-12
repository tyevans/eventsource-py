"""
Schema generation utilities for read models.

Generates CREATE TABLE SQL statements from ReadModel class definitions.
Supports PostgreSQL and SQLite dialects with appropriate type mappings.

Example:
    >>> from eventsource.readmodels import ReadModel
    >>> from eventsource.readmodels.schema import generate_schema
    >>> from decimal import Decimal
    >>>
    >>> class OrderSummary(ReadModel):
    ...     order_number: str
    ...     status: str
    ...     total_amount: Decimal
    ...     item_count: int = 0
    ...
    >>> sql = generate_schema(OrderSummary, dialect="postgresql")
    >>> print(sql)
    CREATE TABLE IF NOT EXISTS order_summaries (
        id UUID PRIMARY KEY,
        created_at TIMESTAMP WITH TIME ZONE NOT NULL,
        updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
        version INTEGER NOT NULL DEFAULT 1,
        deleted_at TIMESTAMP WITH TIME ZONE,
        order_number VARCHAR(255) NOT NULL,
        status VARCHAR(255) NOT NULL,
        total_amount DECIMAL(18, 6) NOT NULL,
        item_count INTEGER NOT NULL DEFAULT 0
    );
"""

import types
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Literal, Union, get_args, get_origin
from uuid import UUID

from pydantic.fields import FieldInfo

from eventsource.readmodels.base import ReadModel

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


def generate_schema(
    model_class: type[ReadModel],
    dialect: Literal["postgresql", "sqlite"] = "postgresql",
    if_not_exists: bool = True,
) -> str:
    """
    Generate CREATE TABLE SQL for a ReadModel class.

    Analyzes the Pydantic model fields and generates appropriate SQL
    column definitions for the specified database dialect.

    Args:
        model_class: The ReadModel subclass to generate schema for
        dialect: Database dialect ('postgresql' or 'sqlite')
        if_not_exists: Include IF NOT EXISTS clause (default True)

    Returns:
        CREATE TABLE SQL statement

    Example:
        >>> from eventsource.readmodels import ReadModel
        >>> from decimal import Decimal
        >>>
        >>> class OrderSummary(ReadModel):
        ...     order_number: str
        ...     status: str
        ...     total_amount: Decimal
        ...     item_count: int = 0
        ...
        >>> sql = generate_schema(OrderSummary, dialect="postgresql")
        >>> print(sql)
        CREATE TABLE IF NOT EXISTS order_summaries (
            id UUID PRIMARY KEY,
            created_at TIMESTAMP WITH TIME ZONE NOT NULL,
            updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
            version INTEGER NOT NULL DEFAULT 1,
            deleted_at TIMESTAMP WITH TIME ZONE,
            order_number VARCHAR(255) NOT NULL,
            status VARCHAR(255) NOT NULL,
            total_amount DECIMAL(18, 6) NOT NULL,
            item_count INTEGER NOT NULL DEFAULT 0
        );

    Note:
        - Table name is derived from model class or __table_name__ attribute
        - Required fields (no default) get NOT NULL constraint
        - Fields with defaults get DEFAULT clause where appropriate
        - Complex types (dict, list) use JSONB in PostgreSQL, TEXT in SQLite
    """
    type_map = POSTGRESQL_TYPE_MAP if dialect == "postgresql" else SQLITE_TYPE_MAP
    table_name = model_class.table_name()

    # Build column definitions
    columns = []
    for field_name, field_info in model_class.model_fields.items():
        column_sql = _generate_column(field_name, field_info, type_map, dialect)
        columns.append(column_sql)

    # Build CREATE TABLE statement
    exists_clause = "IF NOT EXISTS " if if_not_exists else ""
    columns_sql = ",\n    ".join(columns)

    return f"""CREATE TABLE {exists_clause}{table_name} (
    {columns_sql}
);"""


def generate_indexes(
    model_class: type[ReadModel],
    dialect: Literal["postgresql", "sqlite"] = "postgresql",
) -> list[str]:
    """
    Generate CREATE INDEX statements for a ReadModel class.

    Generates standard indexes (soft delete, common query patterns) plus
    any custom indexes defined in the model's __indexes__ attribute.

    Args:
        model_class: The ReadModel subclass to generate indexes for
        dialect: Database dialect ('postgresql' or 'sqlite')

    Returns:
        List of CREATE INDEX SQL statements

    Example:
        >>> from eventsource.readmodels import ReadModel
        >>> from uuid import UUID
        >>>
        >>> class OrderSummary(ReadModel):
        ...     __indexes__ = [
        ...         {"fields": ["status"], "where": "deleted_at IS NULL"},
        ...         {"fields": ["customer_id", "created_at"]},
        ...     ]
        ...     status: str
        ...     customer_id: UUID
        ...
        >>> indexes = generate_indexes(OrderSummary, dialect="postgresql")
        >>> for idx in indexes:
        ...     print(idx)
        CREATE INDEX IF NOT EXISTS idx_order_summaries_deleted ON order_summaries(deleted_at) WHERE deleted_at IS NOT NULL;
        CREATE INDEX IF NOT EXISTS idx_order_summaries_status ON order_summaries(status) WHERE deleted_at IS NULL;
        CREATE INDEX IF NOT EXISTS idx_order_summaries_customer_id_created_at ON order_summaries(customer_id, created_at);
    """
    table_name = model_class.table_name()
    indexes = []

    # Standard index on deleted_at for soft delete queries
    if dialect == "postgresql":
        indexes.append(
            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_deleted "
            f"ON {table_name}(deleted_at) WHERE deleted_at IS NOT NULL;"
        )
    else:
        indexes.append(
            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_deleted ON {table_name}(deleted_at);"
        )

    # Custom indexes from model metadata
    custom_indexes: list[dict[str, Any]] = getattr(model_class, "__indexes__", [])
    for idx_spec in custom_indexes:
        fields = idx_spec.get("fields", [])
        if not fields:
            continue

        idx_name = idx_spec.get("name", f"idx_{table_name}_{'_'.join(fields)}")
        fields_sql = ", ".join(fields)
        where_clause = idx_spec.get("where", "")

        if where_clause and dialect == "postgresql":
            indexes.append(
                f"CREATE INDEX IF NOT EXISTS {idx_name} "
                f"ON {table_name}({fields_sql}) WHERE {where_clause};"
            )
        else:
            indexes.append(f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table_name}({fields_sql});")

    return indexes


def generate_full_schema(
    model_class: type[ReadModel],
    dialect: Literal["postgresql", "sqlite"] = "postgresql",
) -> str:
    """
    Generate complete schema including table and indexes.

    Combines CREATE TABLE and CREATE INDEX statements into a single
    SQL script that can be executed to set up the complete schema
    for a read model.

    Args:
        model_class: The ReadModel subclass to generate schema for
        dialect: Database dialect ('postgresql' or 'sqlite')

    Returns:
        Complete schema SQL with table and indexes

    Example:
        >>> from eventsource.readmodels import ReadModel
        >>>
        >>> class SimpleModel(ReadModel):
        ...     name: str
        ...     count: int = 0
        ...
        >>> sql = generate_full_schema(SimpleModel, dialect="postgresql")
        >>> print(sql)
        CREATE TABLE IF NOT EXISTS simple_models (
            id UUID PRIMARY KEY,
            ...
        );
        <BLANKLINE>
        CREATE INDEX IF NOT EXISTS idx_simple_models_deleted ON simple_models(deleted_at) WHERE deleted_at IS NOT NULL;
    """
    table_sql = generate_schema(model_class, dialect)
    index_sqls = generate_indexes(model_class, dialect)

    parts = [table_sql] + index_sqls
    return "\n\n".join(parts)


def _generate_column(
    field_name: str,
    field_info: FieldInfo,
    type_map: dict[type, str],
    dialect: str,
) -> str:
    """
    Generate a single column definition.

    Args:
        field_name: Name of the field/column
        field_info: Pydantic FieldInfo for the field
        type_map: Mapping of Python types to SQL types
        dialect: Database dialect ('postgresql' or 'sqlite')

    Returns:
        SQL column definition string
    """
    # Get the Python type
    python_type = _extract_type(field_info.annotation)

    # Check for custom SQL type in field metadata/json_schema_extra
    custom_sql_type = _get_custom_sql_type(field_info, dialect)
    sql_type = custom_sql_type or type_map.get(python_type, "TEXT")

    # Special handling for id column
    if field_name == "id":
        return f"id {sql_type} PRIMARY KEY"

    # Build column definition
    parts = [field_name, sql_type]

    # Determine if field is optional (allows None)
    is_optional = _is_optional(field_info.annotation)

    # NOT NULL logic:
    # - If field is optional (Union with None), do NOT add NOT NULL
    # - If field has no default and no default_factory, it's required -> NOT NULL
    # - If field has a concrete default value (not None), it's required -> NOT NULL
    if not is_optional:
        # Check if required (no default) or has a concrete default
        has_default = field_info.default is not None or field_info.default_factory is not None
        if not has_default:
            # Required field with no default
            parts.append("NOT NULL")
        elif field_info.default is not None:
            # Has a concrete default (not a factory)
            parts.append("NOT NULL")
        elif field_info.default_factory is not None:
            # Has a default_factory (like datetime.now)
            parts.append("NOT NULL")

    # DEFAULT clause for simple defaults
    if field_info.default is not None:
        default_value = _format_default(field_info.default, dialect)
        if default_value is not None:
            parts.append(f"DEFAULT {default_value}")

    return " ".join(parts)


def _extract_type(annotation: Any) -> type:
    """
    Extract the base type from a type annotation.

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
    """
    Check if a type annotation is Optional (Union with None).

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
    """
    Format a Python default value for SQL.

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
    """
    Get custom SQL type from field metadata if specified.

    Allows users to override default type mapping by specifying
    sql_type in Field's json_schema_extra.

    Args:
        field_info: Pydantic FieldInfo for the field
        dialect: Database dialect ('postgresql' or 'sqlite')

    Returns:
        Custom SQL type string, or None if not specified

    Example:
        >>> from pydantic import Field
        >>> class MyModel(ReadModel):
        ...     description: str = Field(
        ...         ...,
        ...         json_schema_extra={
        ...             "sql_type": {"postgresql": "TEXT", "sqlite": "TEXT"}
        ...         }
        ...     )
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
    "generate_schema",
    "generate_indexes",
    "generate_full_schema",
    "POSTGRESQL_TYPE_MAP",
    "SQLITE_TYPE_MAP",
]
