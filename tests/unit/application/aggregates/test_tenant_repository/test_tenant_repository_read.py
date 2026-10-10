"""Tests for TenantAwareRepository read methods, properties, and non-isolation semantics."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from eventsource import (
    TenantContextNotSetError,
    clear_tenant_context,
    tenant_scope,
)
from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.aggregates.repository import AggregateRepository
from eventsource.application.aggregates.tenant_repository import TenantAwareRepository
from tests.unit.application.aggregates.test_tenant_repository._fixtures import (
    MockAggregate,
    TenantOrderAggregate,
)


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

    async def test_load_without_requirement_does_not_require_context(
        self, mock_repo: MagicMock
    ) -> None:
        """load() works without context when require_tenant_context=False."""
        tenant_repo: TenantAwareRepository[MockAggregate] = TenantAwareRepository(
            mock_repo, require_tenant_context=False
        )
        aggregate_id = uuid4()

        # No context set - should work
        result = await tenant_repo.load(aggregate_id)

        mock_repo.load.assert_called_once()
        assert result is not None

    async def test_load_with_requirement_requires_context(self, mock_repo: MagicMock) -> None:
        """load() raises TenantContextNotSetError when require_tenant_context=True and no context."""
        tenant_repo: TenantAwareRepository[MockAggregate] = TenantAwareRepository(
            mock_repo, require_tenant_context=True
        )
        aggregate_id = uuid4()

        with pytest.raises(TenantContextNotSetError):
            await tenant_repo.load(aggregate_id)

    async def test_load_with_requirement_and_context_succeeds(self, mock_repo: MagicMock) -> None:
        """load() works when require_tenant_context=True and context is set."""
        tenant_repo: TenantAwareRepository[MockAggregate] = TenantAwareRepository(
            mock_repo, require_tenant_context=True
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

    async def test_exists_without_requirement_does_not_require_context(
        self, mock_repo: MagicMock
    ) -> None:
        """exists() works without context when require_tenant_context=False."""
        tenant_repo: TenantAwareRepository[MockAggregate] = TenantAwareRepository(
            mock_repo, require_tenant_context=False
        )
        aggregate_id = uuid4()

        result = await tenant_repo.exists(aggregate_id)

        mock_repo.exists.assert_called_once()
        assert result is True

    async def test_exists_with_requirement_requires_context(self, mock_repo: MagicMock) -> None:
        """exists() raises TenantContextNotSetError when require_tenant_context=True and no context."""
        tenant_repo: TenantAwareRepository[MockAggregate] = TenantAwareRepository(
            mock_repo, require_tenant_context=True
        )
        aggregate_id = uuid4()

        with pytest.raises(TenantContextNotSetError):
            await tenant_repo.exists(aggregate_id)

    async def test_exists_with_requirement_and_context_succeeds(self, mock_repo: MagicMock) -> None:
        """exists() works when require_tenant_context=True and context is set."""
        tenant_repo: TenantAwareRepository[MockAggregate] = TenantAwareRepository(
            mock_repo, require_tenant_context=True
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

    async def test_load_or_create_with_requirement_requires_context(
        self, mock_repo: MagicMock
    ) -> None:
        """load_or_create() raises when require_tenant_context=True and no context."""
        tenant_repo: TenantAwareRepository[MockAggregate] = TenantAwareRepository(
            mock_repo, require_tenant_context=True
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
            mock_repo, require_tenant_context=True, validate_on_save=False
        )

        repr_str = repr(tenant_repo)

        assert "TenantAwareRepository" in repr_str
        assert "MagicMock" in repr_str
        assert "require_tenant_context=True" in repr_str
        assert "validate_on_save=False" in repr_str

    def test_default_configuration(self, mock_repo: MagicMock) -> None:
        """Default configuration has validate_on_save=True and require_tenant_context=False."""
        tenant_repo: TenantAwareRepository[MockAggregate] = TenantAwareRepository(mock_repo)

        # Check internal state via repr
        repr_str = repr(tenant_repo)
        assert "require_tenant_context=False" in repr_str
        assert "validate_on_save=True" in repr_str


class TestReadsAreNotTenantIsolated:
    """
    Pins the guarantee `require_tenant_context` does NOT make.

    These tests assert the *documented* behavior of ADR 0018 and ADR 0057: the
    flag is a precondition check on the ambient context, not read isolation.
    They exist so that a future change which starts filtering reads has to come
    here and say so deliberately -- and so that nobody reads the flag's name as
    protection it does not provide.
    """

    def setup_method(self) -> None:
        clear_tenant_context()

    def teardown_method(self) -> None:
        clear_tenant_context()

    @pytest.fixture
    def tenant_repo(self) -> TenantAwareRepository[TenantOrderAggregate]:
        base: AggregateRepository[TenantOrderAggregate] = AggregateRepository(
            event_store=InMemoryEventStore(),
            aggregate_factory=TenantOrderAggregate,
        )
        return TenantAwareRepository(base, require_tenant_context=True)

    async def test_load_returns_another_tenants_aggregate(
        self, tenant_repo: TenantAwareRepository[TenantOrderAggregate]
    ) -> None:
        """A load inside tenant A's scope returns tenant B's aggregate, fully replayed."""
        tenant_a, tenant_b = uuid4(), uuid4()
        aggregate_id = uuid4()
        customer_id = uuid4()

        async with tenant_scope(tenant_b):
            order = tenant_repo.create_new(aggregate_id)
            order.create(customer_id=customer_id)
            await tenant_repo.save(order)

        async with tenant_scope(tenant_a):
            leaked = await tenant_repo.load(aggregate_id)

        # No filtering, no partial replay, no error: tenant A gets all of it.
        assert leaked.version == 1
        assert leaked.state is not None
        assert leaked.state.customer_id == customer_id

    async def test_exists_reports_another_tenants_aggregate(
        self, tenant_repo: TenantAwareRepository[TenantOrderAggregate]
    ) -> None:
        """exists() is not scoped to a tenant either."""
        tenant_a, tenant_b = uuid4(), uuid4()
        aggregate_id = uuid4()

        async with tenant_scope(tenant_b):
            order = tenant_repo.create_new(aggregate_id)
            order.create(customer_id=uuid4())
            await tenant_repo.save(order)

        async with tenant_scope(tenant_a):
            assert await tenant_repo.exists(aggregate_id) is True

    def test_create_new_is_unguarded_even_when_context_is_required(
        self, tenant_repo: TenantAwareRepository[TenantOrderAggregate]
    ) -> None:
        """create_new() touches no storage, so it does not check the context."""
        aggregate = tenant_repo.create_new(uuid4())

        assert aggregate.version == 0


__all__ = [
    "TestReadsAreNotTenantIsolated",
    "TestTenantAwareRepositoryCreateNew",
    "TestTenantAwareRepositoryExists",
    "TestTenantAwareRepositoryLoad",
    "TestTenantAwareRepositoryLoadOrCreate",
    "TestTenantAwareRepositoryProperties",
]
