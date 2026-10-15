"""
Base class for read models in event-sourced projections.

Read models are denormalized views of aggregate state, optimized for
query performance. Unlike aggregates (which are rebuilt from events),
read models are persisted directly and updated by projection handlers.
"""

import re
from datetime import UTC, datetime
from typing import Any, ClassVar
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


def _camel_to_snake(name: str) -> str:
    """Convert CamelCase to snake_case (e.g., 'OrderSummary' -> 'order_summary')."""
    s1 = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


def _pluralize(name: str) -> str:
    """Simple English pluralization for common regular nouns."""
    if name.endswith("y") and len(name) > 1 and name[-2] not in "aeiou":
        return name[:-1] + "ies"
    if name.endswith(("s", "x", "z", "ch", "sh")):
        return name + "es"
    return name + "s"


class ReadModel(BaseModel):
    """
    Base class for mutable read models in event-sourced projections.

    Provides standard fields for identity (`id`), timestamps (`created_at`,
    `updated_at`), optimistic locking (`version`), and soft delete (`deleted_at`).
    """

    model_config = ConfigDict(
        from_attributes=True,
        populate_by_name=True,
    )

    id: UUID = Field(
        ...,
        description="Unique identifier for this read model",
    )
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        description="When this read model was first created",
    )
    updated_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        description="When this read model was last updated",
    )
    version: int = Field(
        default=1,
        ge=1,
        description="Optimistic locking version (incremented on each save)",
    )
    deleted_at: datetime | None = Field(
        default=None,
        description="Soft delete timestamp (None if not deleted)",
    )

    __table_name__: ClassVar[str | None] = None

    @classmethod
    def table_name(cls) -> str:
        """Return explicit `__table_name__` or pluralized snake_case class name."""
        if cls.__table_name__:
            return cls.__table_name__
        return _pluralize(_camel_to_snake(cls.__name__))

    @classmethod
    def field_names(cls) -> list[str]:
        """Get all field names defined on this read model (including base fields)."""
        return list(cls.model_fields.keys())

    @classmethod
    def custom_field_names(cls) -> list[str]:
        """Get only custom subclass field names (excluding base class fields)."""
        base_fields = {"id", "created_at", "updated_at", "version", "deleted_at"}
        return [name for name in cls.model_fields if name not in base_fields]

    def is_deleted(self) -> bool:
        """Return True if this read model has been soft-deleted (`deleted_at` is set)."""
        return self.deleted_at is not None

    def to_dict(self) -> dict[str, Any]:
        """Convert read model to a JSON-serializable dictionary."""
        return self.model_dump(mode="json")

    def __str__(self) -> str:
        """Human-readable string representation."""
        return f"{self.__class__.__name__}(id={self.id}, version={self.version})"

    def __repr__(self) -> str:
        """Detailed representation for debugging."""
        deleted_str = f", deleted_at={self.deleted_at!r}" if self.deleted_at else ""
        return (
            f"{self.__class__.__name__}("
            f"id={self.id!r}, "
            f"version={self.version}, "
            f"created_at={self.created_at!r}, "
            f"updated_at={self.updated_at!r}"
            f"{deleted_str})"
        )


__all__ = ["ReadModel"]
