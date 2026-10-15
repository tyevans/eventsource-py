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

    async def test_load_and_get_or_raise_delegate_to_repository(self, mock_repo: MagicMock) -> None:
        tenant_repo: TenantAwareRepository[MockAggregate] = TenantAwareRepository(mock_repo)
        aggregate_id = uuid4()
        assert await tenant_repo.load(aggregate_id) is not None
        assert await tenant_repo.get_or_raise(aggregate_id) is not None
        assert mock_repo.load.call_count == 2

    async def test_load_without_requirement_does_not_require_context(self, mock_repo: MagicMock) -> None:  # fmt: skip
        tenant_repo = TenantAwareRepository(mock_repo, require_tenant_context=False)
        assert await tenant_repo.load(uuid4()) is not None
        mock_repo.load.assert_called_once()

    async def test_load_and_get_or_raise_with_requirement_require_context(self, mock_repo: MagicMock) -> None:  # fmt: skip
        tenant_repo = TenantAwareRepository(mock_repo, require_tenant_context=True)
        with pytest.raises(TenantContextNotSetError):
            await tenant_repo.load(uuid4())
        with pytest.raises(TenantContextNotSetError):
            await tenant_repo.get_or_raise(uuid4())

    async def test_load_with_requirement_and_context_succeeds(self, mock_repo: MagicMock) -> None:
        tenant_repo = TenantAwareRepository(mock_repo, require_tenant_context=True)
        aggregate_id = uuid4()
        async with tenant_scope(uuid4()):
            result = await tenant_repo.load(aggregate_id)
        mock_repo.load.assert_called_once_with(aggregate_id)
        assert result is not None


class TestTenantAwareRepositoryExists(_ClearTenantMixin):
    @pytest.fixture
    def mock_repo(self) -> MagicMock:
        return _build_mock_repo(exists=AsyncMock(return_value=True), get_version=AsyncMock(return_value=3))  # fmt: skip

    async def test_exists_and_get_version_delegate_to_repository(self, mock_repo: MagicMock) -> None:  # fmt: skip
        tenant_repo: TenantAwareRepository[MockAggregate] = TenantAwareRepository(mock_repo)
        aggregate_id = uuid4()
        assert await tenant_repo.exists(aggregate_id) is True
        assert await tenant_repo.get_version(aggregate_id) == 3
        mock_repo.exists.assert_called_once_with(aggregate_id)
        mock_repo.get_version.assert_called_once_with(aggregate_id)

    async def test_exists_without_requirement_does_not_require_context(self, mock_repo: MagicMock) -> None:  # fmt: skip
        tenant_repo = TenantAwareRepository(mock_repo, require_tenant_context=False)
        assert await tenant_repo.exists(uuid4()) is True
        mock_repo.exists.assert_called_once()

    async def test_exists_and_get_version_with_requirement_require_context(self, mock_repo: MagicMock) -> None:  # fmt: skip
        tenant_repo = TenantAwareRepository(mock_repo, require_tenant_context=True)
        with pytest.raises(TenantContextNotSetError):
            await tenant_repo.exists(uuid4())
        with pytest.raises(TenantContextNotSetError):
            await tenant_repo.get_version(uuid4())

    async def test_exists_and_get_version_with_requirement_and_context_succeed(self, mock_repo: MagicMock) -> None:  # fmt: skip
        tenant_repo = TenantAwareRepository(mock_repo, require_tenant_context=True)
        async with tenant_scope(uuid4()):
            assert await tenant_repo.exists(uuid4()) is True
            assert await tenant_repo.get_version(uuid4()) == 3
        mock_repo.exists.assert_called_once()
        mock_repo.get_version.assert_called_once()


class TestTenantAwareRepositoryLoadOrCreate(_ClearTenantMixin):
    @pytest.fixture
    def mock_repo(self) -> MagicMock:
        return _build_mock_repo(load_or_create=AsyncMock(return_value=MockAggregate()))

    async def test_load_or_create_delegates_to_repository(self, mock_repo: MagicMock) -> None:
        tenant_repo: TenantAwareRepository[MockAggregate] = TenantAwareRepository(mock_repo)
        aggregate_id = uuid4()
        assert await tenant_repo.load_or_create(aggregate_id) is not None
        mock_repo.load_or_create.assert_called_once_with(aggregate_id)

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
        assert tenant_repo.create_new(aggregate_id) is not None
        mock_repo.create_new.assert_called_once_with(aggregate_id)


class TestTenantAwareRepositoryProperties:
    @pytest.fixture
    def mock_repo(self) -> MagicMock:
        return _build_mock_repo()

    def test_repository_and_aggregate_type_properties(self, mock_repo: MagicMock) -> None:
        repo = TenantAwareRepository(mock_repo)
        assert repo.repository is mock_repo and repo.aggregate_type == "Order"

    def test_repr_and_defaults(self, mock_repo: MagicMock) -> None:
        repo = TenantAwareRepository(mock_repo, require_tenant_context=True, validate_on_save=False)
        repr_str, default_repr = repr(repo), repr(TenantAwareRepository(mock_repo))
        assert "TenantAwareRepository" in repr_str and "MagicMock" in repr_str
        assert "require_tenant_context=True" in repr_str and "validate_on_save=False" in repr_str
        assert "require_tenant_context=False" in default_repr and "validate_on_save=True" in default_repr  # fmt: skip


class TestReadsAreNotTenantIsolated(_ClearTenantMixin):
    @pytest.fixture
    def tenant_repo(self) -> TenantAwareRepository[TenantOrderAggregate]:
        base = AggregateRepository(InMemoryEventStore(), TenantOrderAggregate)
        return TenantAwareRepository(base, require_tenant_context=True)

    async def test_load_and_exists_return_another_tenants_aggregate(
        self, tenant_repo: TenantAwareRepository[TenantOrderAggregate]
    ) -> None:
        tenant_a, tenant_b, aggregate_id, customer_id = uuid4(), uuid4(), uuid4(), uuid4()
        async with tenant_scope(tenant_b):
            order = tenant_repo.create_new(aggregate_id)
            order.create(customer_id=customer_id)
            await tenant_repo.save(order)
        async with tenant_scope(tenant_a):
            leaked = await tenant_repo.load(aggregate_id)
            assert await tenant_repo.exists(aggregate_id) is True
        assert leaked.version == 1 and leaked.state is not None
        assert leaked.state.customer_id == customer_id

    def test_create_new_is_unguarded_even_when_context_is_required(
        self, tenant_repo: TenantAwareRepository[TenantOrderAggregate]
    ) -> None:
        assert tenant_repo.create_new(uuid4()).version == 0


__all__ = [
    "TestReadsAreNotTenantIsolated",
    "TestTenantAwareRepositoryCreateNew",
    "TestTenantAwareRepositoryExists",
    "TestTenantAwareRepositoryLoad",
    "TestTenantAwareRepositoryLoadOrCreate",
    "TestTenantAwareRepositoryProperties",
]
