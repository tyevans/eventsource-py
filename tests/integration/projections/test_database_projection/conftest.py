"""Pytest fixtures and test models for DatabaseProjection integration tests."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from pydantic import Field
from sqlalchemy import text

from eventsource.domain.event import DomainEvent
from tests.integration.conftest import skip_if_no_postgres_infra

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from sqlalchemy.ext.asyncio import AsyncEngine

pytestmark = [
    skip_if_no_postgres_infra,
    pytest.mark.integration,
    pytest.mark.postgres,
]


class OrderCreated(DomainEvent):
    """Test event for order creation."""

    aggregate_type: str = "Order"
    order_number: str = Field(..., description="Order number")
    amount: float = Field(default=0.0, description="Order amount")


class OrderShipped(DomainEvent):
    """Test event for order shipping."""

    aggregate_type: str = "Order"
    tracking_number: str = Field(..., description="Tracking number")


class OrderCancelled(DomainEvent):
    """Test event for order cancellation."""

    aggregate_type: str = "Order"
    reason: str = Field(..., description="Cancellation reason")


TEST_ORDERS_SCHEMA = """
CREATE TABLE IF NOT EXISTS test_orders (
    id UUID PRIMARY KEY,
    order_number VARCHAR(50) NOT NULL,
    amount DECIMAL(10, 2) NOT NULL DEFAULT 0,
    status VARCHAR(50) NOT NULL DEFAULT 'created',
    tracking_number VARCHAR(100),
    cancelled_reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
"""


@pytest.fixture
async def orders_table(postgres_engine: AsyncEngine) -> AsyncGenerator[None]:
    """Create and clean up test orders table."""
    async with postgres_engine.begin() as conn:
        await conn.execute(text(TEST_ORDERS_SCHEMA))
        await conn.execute(text("TRUNCATE TABLE test_orders CASCADE"))

    yield

    async with postgres_engine.begin() as conn:
        await conn.execute(text("TRUNCATE TABLE test_orders CASCADE"))
