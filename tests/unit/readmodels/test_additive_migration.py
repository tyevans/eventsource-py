"""Reconciling a live read-model table with a model that gained fields.

`generate_schema` emits `CREATE TABLE IF NOT EXISTS`, which does nothing to a
table that already exists. Adding a field to a `ReadModel` therefore does not
add a column to any database that was created before the field existed -- and
every test passes, because tests build their tables from nothing.
"""

from datetime import datetime
from decimal import Decimal
from typing import Any

import pytest
from pydantic import Field

from eventsource.adapters.sql.readmodel_schema import generate_additive_migration
from eventsource.ports.readmodels.exceptions import ReadModelSchemaMismatchError
from eventsource.ports.readmodels.model import ReadModel

# The columns every ReadModel starts with, as an existing table would report
# them. Tests name the added fields; this is the baseline they are added to.
BASE_COLUMNS = ["id", "created_at", "updated_at", "version", "deleted_at"]


class OrderSummary(ReadModel):
    """The model as it stands after two fields were added."""

    order_number: str
    status: str = "pending"
    total_amount: Decimal = Decimal(0)
    note: str | None = None
    tags: list[str] = Field(default_factory=list)
    shipped_at: datetime | None = None
    metadata: dict[str, Any] | None = None


def test_no_statements_when_the_table_already_matches() -> None:
    existing = [*BASE_COLUMNS, *OrderSummary.model_fields]

    assert generate_additive_migration(OrderSummary, existing) == []


def test_adds_only_the_missing_columns() -> None:
    existing = [*BASE_COLUMNS, "order_number", "status", "total_amount", "tags"]

    statements = generate_additive_migration(OrderSummary, existing)

    added = {line.split("ADD COLUMN ")[1].split()[0] for line in statements}
    assert added == {"note", "shipped_at", "metadata"}


def test_emits_alter_table_against_the_model_table_name() -> None:
    existing = [*BASE_COLUMNS, *OrderSummary.model_fields]
    existing.remove("note")

    statements = generate_additive_migration(OrderSummary, existing)

    assert statements == [f"ALTER TABLE {OrderSummary.table_name()} ADD COLUMN note VARCHAR(255);"]


def test_uses_the_dialect_type_map() -> None:
    existing = [*BASE_COLUMNS, *OrderSummary.model_fields]
    existing.remove("shipped_at")

    postgres = generate_additive_migration(OrderSummary, existing, dialect="postgresql")
    sqlite = generate_additive_migration(OrderSummary, existing, dialect="sqlite")

    assert "TIMESTAMP WITH TIME ZONE" in postgres[0]
    assert "TEXT" in sqlite[0]


def test_carries_the_default_onto_a_column_that_has_one() -> None:
    existing = [*BASE_COLUMNS, *OrderSummary.model_fields]
    existing.remove("status")

    statement = generate_additive_migration(OrderSummary, existing)[0]

    assert "NOT NULL" in statement
    assert "DEFAULT 'pending'" in statement


def test_ignores_a_column_the_model_no_longer_declares() -> None:
    """Additive only: a leftover column is not this function's business."""
    existing = [*BASE_COLUMNS, *OrderSummary.model_fields, "legacy_column"]

    assert generate_additive_migration(OrderSummary, existing) == []


def test_column_names_are_compared_case_insensitively() -> None:
    """PostgreSQL folds unquoted identifiers to lower case; SQLite preserves them."""
    existing = [c.upper() for c in (*BASE_COLUMNS, *OrderSummary.model_fields)]

    assert generate_additive_migration(OrderSummary, existing) == []


class RequiredFieldAdded(ReadModel):
    order_number: str
    audited_by: str  # required, no default -- cannot be added to a populated table


def test_rejects_a_required_column_with_no_default() -> None:
    existing = [*BASE_COLUMNS, "order_number"]

    with pytest.raises(ReadModelSchemaMismatchError) as exc_info:
        generate_additive_migration(RequiredFieldAdded, existing)

    error = exc_info.value
    assert error.column == "audited_by"
    assert "default" in str(error).lower()


def test_rejects_a_table_missing_the_primary_key() -> None:
    """No `id` column means this is not the model's table -- adding one cannot fix it."""
    with pytest.raises(ReadModelSchemaMismatchError) as exc_info:
        generate_additive_migration(OrderSummary, ["order_number"])

    assert exc_info.value.column == "id"


def test_rejects_an_empty_column_list() -> None:
    """An absent table is a create, not a reconcile."""
    with pytest.raises(ReadModelSchemaMismatchError):
        generate_additive_migration(OrderSummary, [])
