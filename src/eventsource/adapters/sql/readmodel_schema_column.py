"""Column generation utilities for read model schema."""

from __future__ import annotations

from pydantic.fields import FieldInfo

from eventsource.adapters.sql.readmodel_schema_type import (
    _extract_type,
    _format_default,
    _get_custom_sql_type,
    _is_optional,
)


def _generate_column(
    field_name: str,
    field_info: FieldInfo,
    type_map: dict[type, str],
    dialect: str,
) -> str:
    """Generate a single column definition.

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


__all__ = ["_generate_column"]
