"""Tests for TenantAwareRepository read methods, properties, and non-isolation semantics."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from eventsource import TenantContextNotSetError, clear_tenant_context, tenant_scope
from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.aggregates.repository import AggregateRepository
from eventsource.application.aggregates.tenant_repository import TenantAwareRepository
from tests.unit.application.aggregates.test_tenant_repository._fixtures import (
    MockAggregate,
    TenantOrderAggregate,
)


def _build_mock_repo(**attrs: object) -> MagicMock:
    repo = MagicMock(aggregate_type="Order")
    for k, v in attrs.items():
        setattr(repo, k, v)
    return repo


class _ClearTenantMixin:
    def setup_method(self) -> None:
        clear_tenant_context()

    def teardown_method(self) -> None:
        clear_tenant_context()


class TestTenantAwareRepositoryLoad(_ClearTenantMixin):
    @pytest.fixture
    def mock_repo(self) -> MagicMock:
        return _build_mock_repo(load=AsyncMock(return_value=MockAggregate()))

    async def test_load_delegates_to_repository(self, mock_repo: MagicMock) -> None:
        tenant_repo: TenantAwareRepository[MockAggregate] = TenantAwareRepository(mock_repo)
        aggregate_id = uuid4()
        result = await tenant_repo.load(aggregate_id)
        mock_repo.load.assert_called_once_with(aggregate_id)
        assert result is not None

    async def test_load_without_requirement_does_not_require_context(
        self, mock_repo: MagicMock
    ) -> None:
        tenant_repo = TenantAwareRepository(mock_repo, require_tenant_context=False)
        result = await tenant_repo.load(uuid4())
        mock_repo.load.assert_called_once()
        assert result is not None

    async def test_load_with_requirement_requires_context(self, mock_repo: MagicMock) -> None:
        tenant_repo = TenantAwareRepository(mock_repo, require_tenant_context=True)
        with pytest.raises(TenantContextNotSetError):
            await tenant_repo.load(uuid4())

    async def test_load_with_requirement_and_context_succeeds(self, mock_repo: MagicMock) -> None:
        tenant_repo = TenantAwareRepository(mock_repo, require_tenant_context=True)
        aggregate_id, tenant_id = uuid4(), uuid4()
        async with tenant_scope(tenant_id):
            result = await tenant_repo.load(aggregate_id)
        mock_repo.load.assert_called_once_with(aggregate_id)
        assert result is not None


class TestTenantAwareRepositoryExists(_ClearTenantMixin):
    @pytest.fixture
    def mock_repo(self) -> MagicMock:
        return _build_mock_repo(exists=AsyncMock(return_value=True))

    async def test_exists_delegates_to_repository(self, mock_repo: MagicMock) -> None:
        tenant_repo: TenantAwareRepository[MockAggregate] = TenantAwareRepository(mock_repo)
        aggregate_id = uuid4()
        result = await tenant_repo.exists(aggregate_id)
        mock_repo.exists.assert_called_once_with(aggregate_id)
        assert result is True

    async def test_exists_without_requirement_does_not_require_context(
        self, mock_repo: MagicMock
    ) -> None:
        tenant_repo = TenantAwareRepository(mock_repo, require_tenant_context=False)
        result = await tenant_repo.exists(uuid4())
        mock_repo.exists.assert_called_once()
        assert result is True

    async def test_exists_with_requirement_requires_context(self, mock_repo: MagicMock) -> None:
        tenant_repo = TenantAwareRepository(mock_repo, require_tenant_context=True)
        with pytest.raises(TenantContextNotSetError):
            await tenant_repo.exists(uuid4())

    async def test_exists_with_requirement_and_context_succeeds(self, mock_repo: MagicMock) -> None:
        tenant_repo = TenantAwareRepository(mock_repo, require_tenant_context=True)
        aggregate_id, tenant_id = uuid4(), uuid4()
        async with tenant_scope(tenant_id):
            result = await tenant_repo.exists(aggregate_id)
        mock_repo.exists.assert_called_once()
        assert result is True


class TestTenantAwareRepositoryLoadOrCreate(_ClearTenantMixin):
    @pytest.fixture
    def mock_repo(self) -> MagicMock:
        return _build_mock_repo(load_or_create=AsyncMock(return_value=MockAggregate()))

    async def test_load_or_create_delegates_to_repository(self, mock_repo: MagicMock) -> None:
        tenant_repo: TenantAwareRepository[MockAggregate] = TenantAwareRepository(mock_repo)
        aggregate_id = uuid4()
        result = await tenant_repo.load_or_create(aggregate_id)
        mock_repo.load_or_create.assert_called_once_with(aggregate_id)
        assert result is not None

    async def test_load_or_create_with_requirement_requires_context(
        self, mock_repo: MagicMock
    ) -> None:
        tenant_repo = TenantAwareRepository(mock_repo, require_tenant_context=True)
        with pytest.raises(TenantContextNotSetError):
            await tenant_repo.load_or_create(uuid4())


class TestTenantAwareRepositoryCreateNew:
    @pytest.fixture
    def mock_repo(self) -> MagicMock:
        return _build_mock_repo(create_new=MagicMock(return_value=MockAggregate()))

    def test_create_new_delegates_to_repository(self, mock_repo: MagicMock) -> None:
        tenant_repo: TenantAwareRepository[MockAggregate] = TenantAwareRepository(mock_repo)
        aggregate_id = uuid4()
        result = tenant_repo.create_new(aggregate_id)
        mock_repo.create_new.assert_called_once_with(aggregate_id)
        assert result is not None


class TestTenantAwareRepositoryProperties:
    @pytest.fixture
    def mock_repo(self) -> MagicMock:
        return _build_mock_repo()

    def test_repository_property(self, mock_repo: MagicMock) -> None:
        tenant_repo: TenantAwareRepository[MockAggregate] = TenantAwareRepository(mock_repo)
        assert tenant_repo.repository is mock_repo

    def test_aggregate_type_property(self, mock_repo: MagicMock) -> None:
        tenant_repo: TenantAwareRepository[MockAggregate] = TenantAwareRepository(mock_repo)
        assert tenant_repo.aggregate_type == "Order"

    def test_repr(self, mock_repo: MagicMock) -> None:
        tenant_repo = TenantAwareRepository(
            mock_repo, require_tenant_context=True, validate_on_save=False
        )
        repr_str = repr(tenant_repo)
        assert "TenantAwareRepository" in repr_str
        assert "MagicMock" in repr_str
        assert "require_tenant_context=True" in repr_str
        assert "validate_on_save=False" in repr_str

    def test_default_configuration(self, mock_repo: MagicMock) -> None:
        tenant_repo: TenantAwareRepository[MockAggregate] = TenantAwareRepository(mock_repo)
        repr_str = repr(tenant_repo)
        assert "require_tenant_context=False" in repr_str
        assert "validate_on_save=True" in repr_str


class TestReadsAreNotTenantIsolated(_ClearTenantMixin):
    @pytest.fixture
    def tenant_repo(self) -> TenantAwareRepository[TenantOrderAggregate]:
        base: AggregateRepository[TenantOrderAggregate] = AggregateRepository(
            event_store=InMemoryEventStore(), aggregate_factory=TenantOrderAggregate
        )
        return TenantAwareRepository(base, require_tenant_context=True)

    async def test_load_returns_another_tenants_aggregate(
        self, tenant_repo: TenantAwareRepository[TenantOrderAggregate]
    ) -> None:
        tenant_a, tenant_b, aggregate_id, customer_id = uuid4(), uuid4(), uuid4(), uuid4()
        async with tenant_scope(tenant_b):
            order = tenant_repo.create_new(aggregate_id)
            order.create(customer_id=customer_id)
            await tenant_repo.save(order)
        async with tenant_scope(tenant_a):
            leaked = await tenant_repo.load(aggregate_id)
        assert leaked.version == 1
        assert leaked.state is not None
        assert leaked.state.customer_id == customer_id

    async def test_exists_reports_another_tenants_aggregate(
        self, tenant_repo: TenantAwareRepository[TenantOrderAggregate]
    ) -> None:
        tenant_a, tenant_b, aggregate_id = uuid4(), uuid4(), uuid4()
        async with tenant_scope(tenant_b):
            order = tenant_repo.create_new(aggregate_id)
            order.create(customer_id=uuid4())
            await tenant_repo.save(order)
        async with tenant_scope(tenant_a):
            assert await tenant_repo.exists(aggregate_id) is True

    def test_create_new_is_unguarded_even_when_context_is_required(
        self, tenant_repo: TenantAwareRepository[TenantOrderAggregate]
    ) -> None:
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
