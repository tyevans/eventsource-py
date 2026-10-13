"""Shared read models for end-to-end integration tests."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel


class OrderSummary(BaseModel):
    """Read model for order summary."""

    order_id: UUID
    customer_id: UUID
    item_count: int = 0
    total_amount: float = 0.0
    status: str = "pending"
    completed_at: datetime | None = None


__all__ = ["OrderSummary"]
