"""Routing repository unit tests package."""

from __future__ import annotations

from .test_caching import (
    TestPostgreSQLTenantRoutingRepositoryCaching,
)
from .test_helpers_and_workflow import (
    TestMigrationStateTransitionWorkflow,
    TestPostgreSQLTenantRoutingRepositoryHelpers,
)
from .test_protocol_and_crud import (
    TestPostgreSQLTenantRoutingRepositoryDeleteRouting,
    TestPostgreSQLTenantRoutingRepositoryGetOrDefault,
    TestPostgreSQLTenantRoutingRepositoryGetRouting,
    TestPostgreSQLTenantRoutingRepositoryInit,
    TestPostgreSQLTenantRoutingRepositorySetRouting,
    TestTenantRoutingRepositoryProtocol,
)
from .test_state_and_listing import (
    TestPostgreSQLTenantRoutingRepositoryClearMigrationState,
    TestPostgreSQLTenantRoutingRepositoryListByState,
    TestPostgreSQLTenantRoutingRepositoryListByStore,
    TestPostgreSQLTenantRoutingRepositorySetMigrationState,
)

__all__ = [
    "TestMigrationStateTransitionWorkflow",
    "TestPostgreSQLTenantRoutingRepositoryCaching",  # pragma: allowlist secret
    "TestPostgreSQLTenantRoutingRepositoryClearMigrationState",  # pragma: allowlist secret
    "TestPostgreSQLTenantRoutingRepositoryDeleteRouting",  # pragma: allowlist secret
    "TestPostgreSQLTenantRoutingRepositoryGetOrDefault",  # pragma: allowlist secret
    "TestPostgreSQLTenantRoutingRepositoryGetRouting",  # pragma: allowlist secret
    "TestPostgreSQLTenantRoutingRepositoryHelpers",  # pragma: allowlist secret
    "TestPostgreSQLTenantRoutingRepositoryInit",  # pragma: allowlist secret
    "TestPostgreSQLTenantRoutingRepositoryListByState",  # pragma: allowlist secret
    "TestPostgreSQLTenantRoutingRepositoryListByStore",  # pragma: allowlist secret
    "TestPostgreSQLTenantRoutingRepositorySetMigrationState",  # pragma: allowlist secret
    "TestPostgreSQLTenantRoutingRepositorySetRouting",  # pragma: allowlist secret
    "TestTenantRoutingRepositoryProtocol",
]
