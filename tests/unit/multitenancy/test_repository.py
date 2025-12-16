"""
Unit tests for TenantAwareRepository.

Tests cover:
- Tenant validation on save()
- Tenant context requirement on load() (with enforce_on_load)
- Configuration options (validate_on_save, enforce_on_load)
- Delegation to underlying repository
- Error handling and edge cases
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest

from eventsource.events.base import DomainEvent
from eventsource.multitenancy import (
    TenantAwareRepository,
    TenantContextNotSetError,
    TenantDomainEvent,
    TenantMismatchError,
    clear_tenant_context,
    tenant_scope,
)


# Test events
class OrderCreated(TenantDomainEvent):
    """Test event with tenant_id."""

    aggregate_type: str = "Order"
    customer_id: UUID


class NonTenantOrderCreated(DomainEvent):
    """Test event without tenant_id (uses base DomainEvent)."""

    aggregate_type: str = "Order"
    customer_id: UUID


# Mock aggregate for testing
class MockAggregate:
    """Mock aggregate for testing."""

    def __init__(
        self,
        aggregate_id: UUID | None = None,
        uncommitted_events: list[DomainEvent] | None = None,
        version: int = 0,
    ) -> None:
        self.aggregate_id = aggregate_id or uuid4()
        self.uncommitted_events = uncommitted_events or []
        self.version = version
        self._state: Any = None


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
        tenant_id = uuid4()
        aggregate_id = uuid4()
        customer_id = uuid4()

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
        context_tenant = uuid4()
        event_tenant = uuid4()
        aggregate_id = uuid4()

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

        # Base DomainEvent has tenant_id=None by default
        event = DomainEvent(
            aggregate_id=aggregate_id,
            aggregate_type="Order",
            aggregate_version=1,
        )

        aggregate = MockAggregate(
            aggregate_id=aggregate_id,
            uncommitted_events=[event],
        )

        async with tenant_scope(uuid4()):
            await tenant_repo.save(aggregate)

        mock_repo.save.assert_called_once()

    async def test_save_with_mixed_tenant_and_non_tenant_events(
        self, tenant_repo: TenantAwareRepository[MockAggregate], mock_repo: MagicMock
    ) -> None:
        """save() validates tenant events while allowing non-tenant events."""
        tenant_id = uuid4()
        aggregate_id = uuid4()

        aggregate = MockAggregate(
            aggregate_id=aggregate_id,
            uncommitted_events=[
                # Non-tenant event (allowed)
                NonTenantOrderCreated(
                    aggregate_id=aggregate_id,
                    aggregate_type="Order",
                    aggregate_version=1,
                    customer_id=uuid4(),
                ),
                # Tenant event with matching tenant (allowed)
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
        context_tenant = uuid4()
        wrong_tenant = uuid4()
        aggregate_id = uuid4()

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

            # Should report both mismatched events
            assert len(exc_info.value.event_ids) == 2

    async def test_save_validation_disabled(self, mock_repo: MagicMock) -> None:
        """save() skips validation when validate_on_save=False."""
        tenant_repo: TenantAwareRepository[MockAggregate] = TenantAwareRepository(
            mock_repo, validate_on_save=False
        )

        event_tenant = uuid4()
        context_tenant = uuid4()
        aggregate_id = uuid4()

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

        # Should not raise despite tenant mismatch
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


class TestTenantAwareRepositoryLoad:
    """Tests for load() method."""

    @pytest.fixture
    def mock_repo(self) -> MagicMock:
        """Create a mock repository."""
        repo = MagicMock()
        repo.load = AsyncMock(return_value=MockAggregate())
        repo.aggregate_type = "Order"
        return repo

    def setup_method(self) -> None:
        """Clear tenant context before each test."""
        clear_tenant_context()

    def teardown_method(self) -> None:
        """Clear tenant context after each test."""
        clear_tenant_context()

    async def test_load_delegates_to_repository(self, mock_repo: MagicMock) -> None:
        """load() delegates to underlying repository."""
        tenant_repo: TenantAwareRepository[MockAggregate] = TenantAwareRepository(mock_repo)
        aggregate_id = uuid4()

        result = await tenant_repo.load(aggregate_id)

        mock_repo.load.assert_called_once_with(aggregate_id)
        assert result is not None

    async def test_load_without_enforce_does_not_require_context(
        self, mock_repo: MagicMock
    ) -> None:
        """load() works without context when enforce_on_load=False."""
        tenant_repo: TenantAwareRepository[MockAggregate] = TenantAwareRepository(
            mock_repo, enforce_on_load=False
        )
        aggregate_id = uuid4()

        # No context set - should work
        result = await tenant_repo.load(aggregate_id)

        mock_repo.load.assert_called_once()
        assert result is not None

    async def test_load_with_enforce_requires_context(self, mock_repo: MagicMock) -> None:
        """load() raises TenantContextNotSetError when enforce_on_load=True and no context."""
        tenant_repo: TenantAwareRepository[MockAggregate] = TenantAwareRepository(
            mock_repo, enforce_on_load=True
        )
        aggregate_id = uuid4()

        with pytest.raises(TenantContextNotSetError):
            await tenant_repo.load(aggregate_id)

    async def test_load_with_enforce_and_context_succeeds(self, mock_repo: MagicMock) -> None:
        """load() works when enforce_on_load=True and context is set."""
        tenant_repo: TenantAwareRepository[MockAggregate] = TenantAwareRepository(
            mock_repo, enforce_on_load=True
        )
        aggregate_id = uuid4()
        tenant_id = uuid4()

        async with tenant_scope(tenant_id):
            result = await tenant_repo.load(aggregate_id)

        mock_repo.load.assert_called_once_with(aggregate_id)
        assert result is not None


class TestTenantAwareRepositoryExists:
    """Tests for exists() method."""

    @pytest.fixture
    def mock_repo(self) -> MagicMock:
        """Create a mock repository."""
        repo = MagicMock()
        repo.exists = AsyncMock(return_value=True)
        repo.aggregate_type = "Order"
        return repo

    def setup_method(self) -> None:
        """Clear tenant context before each test."""
        clear_tenant_context()

    def teardown_method(self) -> None:
        """Clear tenant context after each test."""
        clear_tenant_context()

    async def test_exists_delegates_to_repository(self, mock_repo: MagicMock) -> None:
        """exists() delegates to underlying repository."""
        tenant_repo: TenantAwareRepository[MockAggregate] = TenantAwareRepository(mock_repo)
        aggregate_id = uuid4()

        result = await tenant_repo.exists(aggregate_id)

        mock_repo.exists.assert_called_once_with(aggregate_id)
        assert result is True

    async def test_exists_without_enforce_does_not_require_context(
        self, mock_repo: MagicMock
    ) -> None:
        """exists() works without context when enforce_on_load=False."""
        tenant_repo: TenantAwareRepository[MockAggregate] = TenantAwareRepository(
            mock_repo, enforce_on_load=False
        )
        aggregate_id = uuid4()

        result = await tenant_repo.exists(aggregate_id)

        mock_repo.exists.assert_called_once()
        assert result is True

    async def test_exists_with_enforce_requires_context(self, mock_repo: MagicMock) -> None:
        """exists() raises TenantContextNotSetError when enforce_on_load=True and no context."""
        tenant_repo: TenantAwareRepository[MockAggregate] = TenantAwareRepository(
            mock_repo, enforce_on_load=True
        )
        aggregate_id = uuid4()

        with pytest.raises(TenantContextNotSetError):
            await tenant_repo.exists(aggregate_id)

    async def test_exists_with_enforce_and_context_succeeds(self, mock_repo: MagicMock) -> None:
        """exists() works when enforce_on_load=True and context is set."""
        tenant_repo: TenantAwareRepository[MockAggregate] = TenantAwareRepository(
            mock_repo, enforce_on_load=True
        )
        aggregate_id = uuid4()
        tenant_id = uuid4()

        async with tenant_scope(tenant_id):
            result = await tenant_repo.exists(aggregate_id)

        mock_repo.exists.assert_called_once()
        assert result is True


class TestTenantAwareRepositoryLoadOrCreate:
    """Tests for load_or_create() method."""

    @pytest.fixture
    def mock_repo(self) -> MagicMock:
        """Create a mock repository."""
        repo = MagicMock()
        repo.load_or_create = AsyncMock(return_value=MockAggregate())
        repo.aggregate_type = "Order"
        return repo

    def setup_method(self) -> None:
        """Clear tenant context before each test."""
        clear_tenant_context()

    def teardown_method(self) -> None:
        """Clear tenant context after each test."""
        clear_tenant_context()

    async def test_load_or_create_delegates_to_repository(self, mock_repo: MagicMock) -> None:
        """load_or_create() delegates to underlying repository."""
        tenant_repo: TenantAwareRepository[MockAggregate] = TenantAwareRepository(mock_repo)
        aggregate_id = uuid4()

        result = await tenant_repo.load_or_create(aggregate_id)

        mock_repo.load_or_create.assert_called_once_with(aggregate_id)
        assert result is not None

    async def test_load_or_create_with_enforce_requires_context(self, mock_repo: MagicMock) -> None:
        """load_or_create() raises when enforce_on_load=True and no context."""
        tenant_repo: TenantAwareRepository[MockAggregate] = TenantAwareRepository(
            mock_repo, enforce_on_load=True
        )
        aggregate_id = uuid4()

        with pytest.raises(TenantContextNotSetError):
            await tenant_repo.load_or_create(aggregate_id)


class TestTenantAwareRepositoryCreateNew:
    """Tests for create_new() method."""

    @pytest.fixture
    def mock_repo(self) -> MagicMock:
        """Create a mock repository."""
        repo = MagicMock()
        repo.create_new = MagicMock(return_value=MockAggregate())
        repo.aggregate_type = "Order"
        return repo

    def test_create_new_delegates_to_repository(self, mock_repo: MagicMock) -> None:
        """create_new() delegates to underlying repository."""
        tenant_repo: TenantAwareRepository[MockAggregate] = TenantAwareRepository(mock_repo)
        aggregate_id = uuid4()

        result = tenant_repo.create_new(aggregate_id)

        mock_repo.create_new.assert_called_once_with(aggregate_id)
        assert result is not None


class TestTenantAwareRepositoryProperties:
    """Tests for repository properties."""

    @pytest.fixture
    def mock_repo(self) -> MagicMock:
        """Create a mock repository."""
        repo = MagicMock()
        repo.aggregate_type = "Order"
        return repo

    def test_repository_property(self, mock_repo: MagicMock) -> None:
        """repository property returns the underlying repository."""
        tenant_repo: TenantAwareRepository[MockAggregate] = TenantAwareRepository(mock_repo)

        assert tenant_repo.repository is mock_repo

    def test_aggregate_type_property(self, mock_repo: MagicMock) -> None:
        """aggregate_type property returns the underlying repository's aggregate_type."""
        tenant_repo: TenantAwareRepository[MockAggregate] = TenantAwareRepository(mock_repo)

        assert tenant_repo.aggregate_type == "Order"

    def test_repr(self, mock_repo: MagicMock) -> None:
        """__repr__ returns expected string representation."""
        tenant_repo: TenantAwareRepository[MockAggregate] = TenantAwareRepository(
            mock_repo, enforce_on_load=True, validate_on_save=False
        )

        repr_str = repr(tenant_repo)

        assert "TenantAwareRepository" in repr_str
        assert "MagicMock" in repr_str
        assert "enforce_on_load=True" in repr_str
        assert "validate_on_save=False" in repr_str

    def test_default_configuration(self, mock_repo: MagicMock) -> None:
        """Default configuration has validate_on_save=True and enforce_on_load=False."""
        tenant_repo: TenantAwareRepository[MockAggregate] = TenantAwareRepository(mock_repo)

        # Check internal state via repr
        repr_str = repr(tenant_repo)
        assert "enforce_on_load=False" in repr_str
        assert "validate_on_save=True" in repr_str


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
        mock_repo = MagicMock()
        mock_repo.save = AsyncMock()
        mock_repo.aggregate_type = "Order"

        tenant_repo: TenantAwareRepository[MockAggregate] = TenantAwareRepository(mock_repo)

        context_tenant = uuid4()
        event_tenant = uuid4()
        aggregate_id = uuid4()

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

            # Verify error details
            error = exc_info.value
            assert error.expected == context_tenant
            assert error.actual == event_tenant
            assert str(context_tenant) in str(error)
            assert str(event_tenant) in str(error)

    async def test_error_contains_affected_event_ids(self) -> None:
        """TenantMismatchError lists all affected event IDs."""
        mock_repo = MagicMock()
        mock_repo.save = AsyncMock()
        mock_repo.aggregate_type = "Order"

        tenant_repo: TenantAwareRepository[MockAggregate] = TenantAwareRepository(mock_repo)

        context_tenant = uuid4()
        wrong_tenant = uuid4()
        aggregate_id = uuid4()

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

        aggregate = MockAggregate(
            aggregate_id=aggregate_id,
            uncommitted_events=events,
        )

        async with tenant_scope(context_tenant):
            with pytest.raises(TenantMismatchError) as exc_info:
                await tenant_repo.save(aggregate)

            # Verify event IDs are captured
            error = exc_info.value
            assert len(error.event_ids) == 2
            assert events[0].event_id in error.event_ids
            assert events[1].event_id in error.event_ids
