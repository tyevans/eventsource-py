"""Tests for TenantMismatchError details raised by TenantAwareRepository.save."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from eventsource import TenantMismatchError, clear_tenant_context, tenant_scope
from eventsource.application.aggregates.tenant_repository import TenantAwareRepository
from tests.unit.application.aggregates.test_tenant_repository._fixtures import MockAggregate
from tests.unit.application.aggregates.test_tenant_repository.test_tenant_repository_save import (
    _order_event,
)


class TestTenantMismatchErrorDetails:
    """Tests for TenantMismatchError details."""

    def setup_method(self) -> None:
        clear_tenant_context()

    def teardown_method(self) -> None:
        clear_tenant_context()

    async def test_error_contains_expected_tenant(self) -> None:
        mock_repo = MagicMock(save=AsyncMock(), aggregate_type="Order")
        tenant_repo: TenantAwareRepository[MockAggregate] = TenantAwareRepository(mock_repo)
        context_tenant, event_tenant, aggregate_id = uuid4(), uuid4(), uuid4()
        event = _order_event(aggregate_id, event_tenant)
        aggregate = MockAggregate(aggregate_id=aggregate_id, uncommitted_events=[event])
        async with tenant_scope(context_tenant):
            with pytest.raises(TenantMismatchError) as exc_info:
                await tenant_repo.save(aggregate)
            error = exc_info.value
            assert error.expected == context_tenant
            assert error.actual == event_tenant
            assert str(context_tenant) in str(error)
            assert str(event_tenant) in str(error)

    async def test_error_contains_affected_event_ids(self) -> None:
        mock_repo = MagicMock(save=AsyncMock(), aggregate_type="Order")
        tenant_repo: TenantAwareRepository[MockAggregate] = TenantAwareRepository(mock_repo)
        context_tenant, wrong_tenant, aggregate_id = uuid4(), uuid4(), uuid4()
        events = [_order_event(aggregate_id, wrong_tenant, version=v) for v in (1, 2)]
        aggregate = MockAggregate(aggregate_id=aggregate_id, uncommitted_events=events)
        async with tenant_scope(context_tenant):
            with pytest.raises(TenantMismatchError) as exc_info:
                await tenant_repo.save(aggregate)
            error = exc_info.value
            assert len(error.event_ids) == 2
            assert events[0].event_id in error.event_ids
            assert events[1].event_id in error.event_ids


__all__ = ["TestTenantMismatchErrorDetails"]
