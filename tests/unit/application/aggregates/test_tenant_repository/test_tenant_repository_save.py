"""Tests for TenantAwareRepository save operations and error details."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest

from eventsource import (
    TenantContextNotSetError,
    TenantMismatchError,
    clear_tenant_context,
    tenant_scope,
)
from eventsource.application.aggregates.tenant_repository import TenantAwareRepository
from eventsource.domain.event import DomainEvent
from tests.unit.application.aggregates.test_tenant_repository._fixtures import (
    MockAggregate,
    NonTenantOrderCreated,
    OrderCreated,
)


def _order_event(agg_id: UUID, tenant_id: UUID, version: int = 1) -> OrderCreated:
    return OrderCreated(
        aggregate_id=agg_id,
        aggregate_type="Order",
        aggregate_version=version,
        tenant_id=tenant_id,
        customer_id=uuid4(),
    )


class TestTenantAwareRepositorySave:
    """Tests for save() method."""

    @pytest.fixture
    def mock_repo(self) -> MagicMock:
        return MagicMock(save=AsyncMock(), aggregate_type="Order")

    @pytest.fixture
    def tenant_repo(self, mock_repo: MagicMock) -> TenantAwareRepository[MockAggregate]:
        return TenantAwareRepository(mock_repo)

    def setup_method(self) -> None:
        clear_tenant_context()

    def teardown_method(self) -> None:
        clear_tenant_context()

    async def test_save_validates_tenant_and_delegates(
        self, tenant_repo: TenantAwareRepository[MockAggregate], mock_repo: MagicMock
    ) -> None:
        tenant_id, aggregate_id = uuid4(), uuid4()
        event = _order_event(aggregate_id, tenant_id)
        aggregate = MockAggregate(aggregate_id=aggregate_id, uncommitted_events=[event])
        async with tenant_scope(tenant_id):
            await tenant_repo.save(aggregate)
        mock_repo.save.assert_called_once_with(aggregate)

    async def test_save_raises_on_tenant_mismatch(
        self, tenant_repo: TenantAwareRepository[MockAggregate]
    ) -> None:
        context_tenant, event_tenant, aggregate_id = uuid4(), uuid4(), uuid4()
        event = _order_event(aggregate_id, event_tenant)
        aggregate = MockAggregate(aggregate_id=aggregate_id, uncommitted_events=[event])
        async with tenant_scope(context_tenant):
            with pytest.raises(TenantMismatchError) as exc_info:
                await tenant_repo.save(aggregate)
            assert exc_info.value.expected == context_tenant
            assert exc_info.value.actual == event_tenant
            assert len(exc_info.value.event_ids) == 1

    async def test_save_raises_without_tenant_context(
        self, tenant_repo: TenantAwareRepository[MockAggregate]
    ) -> None:
        aggregate_id = uuid4()
        event = _order_event(aggregate_id, uuid4())
        aggregate = MockAggregate(aggregate_id=aggregate_id, uncommitted_events=[event])
        with pytest.raises(TenantContextNotSetError):
            await tenant_repo.save(aggregate)

    async def test_save_allows_events_without_tenant_id(
        self, tenant_repo: TenantAwareRepository[MockAggregate], mock_repo: MagicMock
    ) -> None:
        aggregate_id = uuid4()
        event = NonTenantOrderCreated(
            aggregate_id=aggregate_id,
            aggregate_type="Order",
            aggregate_version=1,
            customer_id=uuid4(),
        )
        aggregate = MockAggregate(aggregate_id=aggregate_id, uncommitted_events=[event])
        async with tenant_scope(uuid4()):
            await tenant_repo.save(aggregate)
        mock_repo.save.assert_called_once()

    async def test_save_allows_events_with_none_tenant_id(
        self, tenant_repo: TenantAwareRepository[MockAggregate], mock_repo: MagicMock
    ) -> None:
        aggregate_id = uuid4()
        event = DomainEvent(aggregate_id=aggregate_id, aggregate_type="Order", aggregate_version=1)
        aggregate = MockAggregate(aggregate_id=aggregate_id, uncommitted_events=[event])
        async with tenant_scope(uuid4()):
            await tenant_repo.save(aggregate)
        mock_repo.save.assert_called_once()

    async def test_save_with_mixed_tenant_and_non_tenant_events(
        self, tenant_repo: TenantAwareRepository[MockAggregate], mock_repo: MagicMock
    ) -> None:
        tenant_id, aggregate_id = uuid4(), uuid4()
        events = [
            NonTenantOrderCreated(
                aggregate_id=aggregate_id,
                aggregate_type="Order",
                aggregate_version=1,
                customer_id=uuid4(),
            ),
            _order_event(aggregate_id, tenant_id, version=2),
        ]
        aggregate = MockAggregate(aggregate_id=aggregate_id, uncommitted_events=events)
        async with tenant_scope(tenant_id):
            await tenant_repo.save(aggregate)
        mock_repo.save.assert_called_once()

    async def test_save_with_multiple_mismatched_events(
        self, tenant_repo: TenantAwareRepository[MockAggregate]
    ) -> None:
        context_tenant, wrong_tenant, aggregate_id = uuid4(), uuid4(), uuid4()
        events = [_order_event(aggregate_id, wrong_tenant, version=v) for v in (1, 2)]
        aggregate = MockAggregate(aggregate_id=aggregate_id, uncommitted_events=events)
        async with tenant_scope(context_tenant):
            with pytest.raises(TenantMismatchError) as exc_info:
                await tenant_repo.save(aggregate)
            assert len(exc_info.value.event_ids) == 2

    async def test_save_validation_disabled(self, mock_repo: MagicMock) -> None:
        tenant_repo: TenantAwareRepository[MockAggregate] = TenantAwareRepository(
            mock_repo, validate_on_save=False
        )
        event_tenant, context_tenant, aggregate_id = uuid4(), uuid4(), uuid4()
        event = _order_event(aggregate_id, event_tenant)
        aggregate = MockAggregate(aggregate_id=aggregate_id, uncommitted_events=[event])
        async with tenant_scope(context_tenant):
            await tenant_repo.save(aggregate)
        mock_repo.save.assert_called_once()

    async def test_save_with_empty_uncommitted_events(
        self, tenant_repo: TenantAwareRepository[MockAggregate], mock_repo: MagicMock
    ) -> None:
        aggregate = MockAggregate(aggregate_id=uuid4(), uncommitted_events=[])
        async with tenant_scope(uuid4()):
            await tenant_repo.save(aggregate)
        mock_repo.save.assert_called_once()


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


__all__ = ["TestTenantAwareRepositorySave", "TestTenantMismatchErrorDetails"]
