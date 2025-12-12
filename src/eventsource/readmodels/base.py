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
    """
    Convert CamelCase to snake_case.

    Examples:
        >>> _camel_to_snake("OrderSummary")
        'order_summary'
        >>> _camel_to_snake("UserProfileDTO")
        'user_profile_dto'
        >>> _camel_to_snake("HTTPResponse")
        'http_response'
    """
    # Insert underscore before uppercase letters that follow lowercase letters
    s1 = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    # Insert underscore before uppercase letters that follow lowercase letters or other uppercase sequences
    return re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


def _pluralize(name: str) -> str:
    """
    Simple English pluralization.

    Handles common cases but not irregular plurals.

    Examples:
        >>> _pluralize("order_summary")
        'order_summaries'
        >>> _pluralize("user")
        'users'
        >>> _pluralize("address")
        'addresses'
        >>> _pluralize("batch")
        'batches'
    """
    if name.endswith("y") and len(name) > 1 and name[-2] not in "aeiou":
        return name[:-1] + "ies"
    if name.endswith(("s", "x", "z", "ch", "sh")):
        return name + "es"
    return name + "s"


class ReadModel(BaseModel):
    """
    Base class for read models in event-sourced projections.

    Provides standard fields for identity, timestamps, versioning, and soft delete.
    Subclasses define domain-specific fields for their particular read model.

    Unlike DomainEvent (which is frozen/immutable), ReadModel is mutable because
    projection handlers need to update field values before saving.

    Attributes:
        id: Unique identifier for this read model instance
        created_at: When this read model was first created (auto-set on first save)
        updated_at: When this read model was last updated (auto-updated on save)
        version: Optimistic locking version (incremented on each save)
        deleted_at: Soft delete timestamp (None if not deleted)

    Example:
        >>> from decimal import Decimal
        >>> from uuid import uuid4
        >>>
        >>> class OrderSummary(ReadModel):
        ...     order_number: str
        ...     customer_name: str
        ...     status: str
        ...     total_amount: Decimal
        ...     item_count: int = 0
        ...
        >>> summary = OrderSummary(
        ...     id=uuid4(),
        ...     order_number="ORD-001",
        ...     customer_name="Alice Smith",
        ...     status="pending",
        ...     total_amount=Decimal("99.99"),
        ... )
        >>> print(summary.table_name())
        'order_summaries'
    """

    model_config = ConfigDict(
        from_attributes=True,  # Allow construction from ORM/dict with attribute access
        populate_by_name=True,  # Allow field aliases in construction
        # NOT frozen - read models must be mutable for updates
    )

    # Identity
    id: UUID = Field(
        ...,
        description="Unique identifier for this read model",
    )

    # Timestamps (auto-managed by repository)
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        description="When this read model was first created",
    )
    updated_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        description="When this read model was last updated",
    )

    # Optimistic locking
    version: int = Field(
        default=1,
        ge=1,
        description="Optimistic locking version (incremented on each save)",
    )

    # Soft delete
    deleted_at: datetime | None = Field(
        default=None,
        description="Soft delete timestamp (None if not deleted)",
    )

    # Class-level table name override
    __table_name__: ClassVar[str | None] = None

    @classmethod
    def table_name(cls) -> str:
        """
        Get the database table name for this read model.

        Returns __table_name__ if explicitly set, otherwise derives the
        table name from the class name using snake_case and pluralization.

        Examples:
            OrderSummary -> order_summaries
            UserProfile -> user_profiles
            Address -> addresses

        Returns:
            Table name string

        Example:
            >>> class CustomerView(ReadModel):
            ...     name: str
            >>> CustomerView.table_name()
            'customer_views'

            >>> class Invoice(ReadModel):
            ...     __table_name__ = "billing_invoices"
            ...     amount: Decimal
            >>> Invoice.table_name()
            'billing_invoices'
        """
        if cls.__table_name__:
            return cls.__table_name__
        return _pluralize(_camel_to_snake(cls.__name__))

    @classmethod
    def field_names(cls) -> list[str]:
        """
        Get all field names defined on this read model.

        Includes both base class fields (id, created_at, etc.) and
        subclass-specific fields.

        Returns:
            List of field name strings

        Example:
            >>> class ProductView(ReadModel):
            ...     sku: str
            ...     name: str
            ...     price: Decimal
            >>> ProductView.field_names()
            ['id', 'created_at', 'updated_at', 'version', 'deleted_at', 'sku', 'name', 'price']
        """
        return list(cls.model_fields.keys())

    @classmethod
    def custom_field_names(cls) -> list[str]:
        """
        Get only the custom field names (excluding base class fields).

        Useful for generating SQL column lists that exclude standard fields.

        Returns:
            List of custom field name strings

        Example:
            >>> class ProductView(ReadModel):
            ...     sku: str
            ...     name: str
            >>> ProductView.custom_field_names()
            ['sku', 'name']
        """
        base_fields = {"id", "created_at", "updated_at", "version", "deleted_at"}
        return [name for name in cls.model_fields if name not in base_fields]

    def is_deleted(self) -> bool:
        """
        Check if this read model has been soft-deleted.

        Returns:
            True if deleted_at is set, False otherwise

        Example:
            >>> model = OrderSummary(id=uuid4(), ...)
            >>> model.is_deleted()
            False
            >>> model.deleted_at = datetime.now(UTC)
            >>> model.is_deleted()
            True
        """
        return self.deleted_at is not None

    def to_dict(self) -> dict[str, Any]:
        """
        Convert read model to dictionary for serialization.

        Uses Pydantic's JSON-compatible serialization mode to ensure
        all values (including UUIDs and datetimes) are JSON-serializable.

        Returns:
            Dictionary representation with all fields JSON-serializable
        """
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
