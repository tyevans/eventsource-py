"""Tests for TenantAwareRepository save operations and error details."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

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


class TestTenantAwareRepositorySave:
    """Tests for save() method."""

    @pytest.fixture
    def mock_repo(self) -> MagicMock:
        """Create a mock repository."""
        repo = MagicMock()
        repo.save = AsyncMock()
        repo.aggregate_type = "Order"
        return repo

    @pytest.fixture
    def tenant_repo(self, mock_repo: MagicMock) -> TenantAwareRepository[MockAggregate]:
        """Create a TenantAwareRepository wrapper."""
        return TenantAwareRepository(mock_repo)

    def setup_method(self) -> None:
        """Clear tenant context before each test."""
        clear_tenant_context()

    def teardown_method(self) -> None:
        """Clear tenant context after each test."""
        clear_tenant_context()

    async def test_save_validates_tenant_and_delegates(
        self, tenant_repo: TenantAwareRepository[MockAggregate], mock_repo: MagicMock
    ) -> None:
        """save() validates tenant before delegating to underlying repository."""
        tenant_id, aggregate_id, customer_id = uuid4(), uuid4(), uuid4()
        aggregate = MockAggregate(
            aggregate_id=aggregate_id,
            uncommitted_events=[
                OrderCreated(
                    aggregate_id=aggregate_id,
                    aggregate_type="Order",
                    aggregate_version=1,
                    tenant_id=tenant_id,
                    customer_id=customer_id,
                )
            ],
        )
        async with tenant_scope(tenant_id):
            await tenant_repo.save(aggregate)
        mock_repo.save.assert_called_once_with(aggregate)

    async def test_save_raises_on_tenant_mismatch(
        self, tenant_repo: TenantAwareRepository[MockAggregate]
    ) -> None:
        """save() raises TenantMismatchError when event tenant doesn't match context."""
        context_tenant, event_tenant, aggregate_id = uuid4(), uuid4(), uuid4()
        aggregate = MockAggregate(
            aggregate_id=aggregate_id,
            uncommitted_events=[
                OrderCreated(
                    aggregate_id=aggregate_id,
                    aggregate_type="Order",
                    aggregate_version=1,
                    tenant_id=event_tenant,
                    customer_id=uuid4(),
                )
            ],
        )
        async with tenant_scope(context_tenant):
            with pytest.raises(TenantMismatchError) as exc_info:
                await tenant_repo.save(aggregate)
            assert exc_info.value.expected == context_tenant
            assert exc_info.value.actual == event_tenant
            assert len(exc_info.value.event_ids) == 1

    async def test_save_raises_without_tenant_context(
        self, tenant_repo: TenantAwareRepository[MockAggregate]
    ) -> None:
        """save() raises TenantContextNotSetError when no context is set."""
        aggregate_id = uuid4()
        aggregate = MockAggregate(
            aggregate_id=aggregate_id,
            uncommitted_events=[
                OrderCreated(
                    aggregate_id=aggregate_id,
                    aggregate_type="Order",
                    aggregate_version=1,
                    tenant_id=uuid4(),
                    customer_id=uuid4(),
                )
            ],
        )
        with pytest.raises(TenantContextNotSetError):
            await tenant_repo.save(aggregate)

    async def test_save_allows_events_without_tenant_id(
        self, tenant_repo: TenantAwareRepository[MockAggregate], mock_repo: MagicMock
    ) -> None:
        """save() allows events that don't have a tenant_id field."""
        aggregate_id = uuid4()
        aggregate = MockAggregate(
            aggregate_id=aggregate_id,
            uncommitted_events=[
                NonTenantOrderCreated(
                    aggregate_id=aggregate_id,
                    aggregate_type="Order",
                    aggregate_version=1,
                    customer_id=uuid4(),
                )
            ],
        )
        async with tenant_scope(uuid4()):
            await tenant_repo.save(aggregate)
        mock_repo.save.assert_called_once()

    async def test_save_allows_events_with_none_tenant_id(
        self, tenant_repo: TenantAwareRepository[MockAggregate], mock_repo: MagicMock
    ) -> None:
        """save() allows events where tenant_id is None."""
        aggregate_id = uuid4()
        event = DomainEvent(aggregate_id=aggregate_id, aggregate_type="Order", aggregate_version=1)
        aggregate = MockAggregate(aggregate_id=aggregate_id, uncommitted_events=[event])
        async with tenant_scope(uuid4()):
            await tenant_repo.save(aggregate)
        mock_repo.save.assert_called_once()

    async def test_save_with_mixed_tenant_and_non_tenant_events(
        self, tenant_repo: TenantAwareRepository[MockAggregate], mock_repo: MagicMock
    ) -> None:
        """save() validates tenant events while allowing non-tenant events."""
        tenant_id, aggregate_id = uuid4(), uuid4()
        aggregate = MockAggregate(
            aggregate_id=aggregate_id,
            uncommitted_events=[
                NonTenantOrderCreated(
                    aggregate_id=aggregate_id,
                    aggregate_type="Order",
                    aggregate_version=1,
                    customer_id=uuid4(),
                ),
                OrderCreated(
                    aggregate_id=aggregate_id,
                    aggregate_type="Order",
                    aggregate_version=2,
                    tenant_id=tenant_id,
                    customer_id=uuid4(),
                ),
            ],
        )
        async with tenant_scope(tenant_id):
            await tenant_repo.save(aggregate)
        mock_repo.save.assert_called_once()

    async def test_save_with_multiple_mismatched_events(
        self, tenant_repo: TenantAwareRepository[MockAggregate]
    ) -> None:
        """save() reports all mismatched events."""
        context_tenant, wrong_tenant, aggregate_id = uuid4(), uuid4(), uuid4()
        aggregate = MockAggregate(
            aggregate_id=aggregate_id,
            uncommitted_events=[
                OrderCreated(
                    aggregate_id=aggregate_id,
                    aggregate_type="Order",
                    aggregate_version=1,
                    tenant_id=wrong_tenant,
                    customer_id=uuid4(),
                ),
                OrderCreated(
                    aggregate_id=aggregate_id,
                    aggregate_type="Order",
                    aggregate_version=2,
                    tenant_id=wrong_tenant,
                    customer_id=uuid4(),
                ),
            ],
        )
        async with tenant_scope(context_tenant):
            with pytest.raises(TenantMismatchError) as exc_info:
                await tenant_repo.save(aggregate)
            assert len(exc_info.value.event_ids) == 2

    async def test_save_validation_disabled(self, mock_repo: MagicMock) -> None:
        """save() skips validation when validate_on_save=False."""
        tenant_repo: TenantAwareRepository[MockAggregate] = TenantAwareRepository(
            mock_repo, validate_on_save=False
        )
        event_tenant, context_tenant, aggregate_id = uuid4(), uuid4(), uuid4()
        aggregate = MockAggregate(
            aggregate_id=aggregate_id,
            uncommitted_events=[
                OrderCreated(
                    aggregate_id=aggregate_id,
                    aggregate_type="Order",
                    aggregate_version=1,
                    tenant_id=event_tenant,
                    customer_id=uuid4(),
                )
            ],
        )
        async with tenant_scope(context_tenant):
            await tenant_repo.save(aggregate)
        mock_repo.save.assert_called_once()

    async def test_save_with_empty_uncommitted_events(
        self, tenant_repo: TenantAwareRepository[MockAggregate], mock_repo: MagicMock
    ) -> None:
        """save() handles aggregates with no uncommitted events."""
        aggregate = MockAggregate(aggregate_id=uuid4(), uncommitted_events=[])
        async with tenant_scope(uuid4()):
            await tenant_repo.save(aggregate)
        mock_repo.save.assert_called_once()


class TestTenantMismatchErrorDetails:
    """Tests for TenantMismatchError details."""

    def setup_method(self) -> None:
        """Clear tenant context before each test."""
        clear_tenant_context()

    def teardown_method(self) -> None:
        """Clear tenant context after each test."""
        clear_tenant_context()

    async def test_error_contains_expected_tenant(self) -> None:
        """TenantMismatchError contains the expected tenant ID."""
        mock_repo = MagicMock(save=AsyncMock(), aggregate_type="Order")
        tenant_repo: TenantAwareRepository[MockAggregate] = TenantAwareRepository(mock_repo)
        context_tenant, event_tenant, aggregate_id = uuid4(), uuid4(), uuid4()
        aggregate = MockAggregate(
            aggregate_id=aggregate_id,
            uncommitted_events=[
                OrderCreated(
                    aggregate_id=aggregate_id,
                    aggregate_type="Order",
                    aggregate_version=1,
                    tenant_id=event_tenant,
                    customer_id=uuid4(),
                )
            ],
        )
        async with tenant_scope(context_tenant):
            with pytest.raises(TenantMismatchError) as exc_info:
                await tenant_repo.save(aggregate)
            error = exc_info.value
            assert error.expected == context_tenant
            assert error.actual == event_tenant
            assert str(context_tenant) in str(error)
            assert str(event_tenant) in str(error)

    async def test_error_contains_affected_event_ids(self) -> None:
        """TenantMismatchError lists all affected event IDs."""
        mock_repo = MagicMock(save=AsyncMock(), aggregate_type="Order")
        tenant_repo: TenantAwareRepository[MockAggregate] = TenantAwareRepository(mock_repo)
        context_tenant, wrong_tenant, aggregate_id = uuid4(), uuid4(), uuid4()
        events = [
            OrderCreated(
                aggregate_id=aggregate_id,
                aggregate_type="Order",
                aggregate_version=1,
                tenant_id=wrong_tenant,
                customer_id=uuid4(),
            ),
            OrderCreated(
                aggregate_id=aggregate_id,
                aggregate_type="Order",
                aggregate_version=2,
                tenant_id=wrong_tenant,
                customer_id=uuid4(),
            ),
        ]
        aggregate = MockAggregate(aggregate_id=aggregate_id, uncommitted_events=events)
        async with tenant_scope(context_tenant):
            with pytest.raises(TenantMismatchError) as exc_info:
                await tenant_repo.save(aggregate)
            error = exc_info.value
            assert len(error.event_ids) == 2
            assert events[0].event_id in error.event_ids
            assert events[1].event_id in error.event_ids


__all__ = [
    "TestTenantAwareRepositorySave",
    "TestTenantMismatchErrorDetails",
]
