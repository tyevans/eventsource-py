"""Unit tests for TenantAwareRepository."""

from __future__ import annotations

from ._fixtures import (  # pragma: allowlist secret
    MockAggregate,
    NonTenantOrderCreated,
    OrderCreated,
    TenantOrderAggregate,
    TenantOrderState,
)
from .test_tenant_repository_read import (  # pragma: allowlist secret
    TestReadsAreNotTenantIsolated,
    TestTenantAwareRepositoryCreateNew,
    TestTenantAwareRepositoryExists,
    TestTenantAwareRepositoryLoad,
    TestTenantAwareRepositoryLoadOrCreate,
    TestTenantAwareRepositoryProperties,
)
from .test_tenant_repository_save import (  # pragma: allowlist secret
    TestTenantAwareRepositorySave,
    TestTenantMismatchErrorDetails,
)

__all__ = [  # pragma: allowlist secret
    "MockAggregate",  # pragma: allowlist secret
    "NonTenantOrderCreated",  # pragma: allowlist secret
    "OrderCreated",  # pragma: allowlist secret
    "TenantOrderAggregate",  # pragma: allowlist secret
    "TenantOrderState",  # pragma: allowlist secret
    "TestReadsAreNotTenantIsolated",  # pragma: allowlist secret
    "TestTenantAwareRepositoryCreateNew",  # pragma: allowlist secret
    "TestTenantAwareRepositoryExists",  # pragma: allowlist secret
    "TestTenantAwareRepositoryLoad",  # pragma: allowlist secret
    "TestTenantAwareRepositoryLoadOrCreate",  # pragma: allowlist secret
    "TestTenantAwareRepositoryProperties",  # pragma: allowlist secret
    "TestTenantAwareRepositorySave",  # pragma: allowlist secret
    "TestTenantMismatchErrorDetails",  # pragma: allowlist secret
]
