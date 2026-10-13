"""Fixtures and model definitions for schema generation tests."""

from datetime import date, datetime
from decimal import Decimal
from typing import Any, ClassVar
from uuid import UUID

from pydantic import Field

from eventsource.ports.readmodels.model import ReadModel


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
