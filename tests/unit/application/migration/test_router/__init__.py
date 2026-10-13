"""Unit tests for TenantStoreRouter package."""

from ._fixtures import (
    AsyncIteratorMock,
    TestEvent,
    async_generator_mock,
    create_mock_store,
    mock_dedicated_store,
    mock_default_store,
    mock_routing_repo,
    router,
    sid,
)
from .test_router_append_routing import TestAppendRouting
from .test_router_init_and_registry import (
    TestRouterStructuralConformance,
    TestStoreNotFoundError,
    TestStoreRegistryManagement,
    TestTenantStoreRouterInit,
)
from .test_router_interceptor_and_pause import (
    TestDualWriteInterceptorManagement,
    TestDualWriteStateWithoutInterceptor,
    TestMigrationStateRouting,
    TestWaitIfPaused,
    TestWritePausedError,
    TestWritePauseManagement,
)
from .test_router_resolution_and_helpers import (
    TestHelperMethods,
    TestTenantStoreResolution,
)
from .test_router_routing_and_concurrency import (
    TestConcurrentOperations,
    TestReadOperationsRouting,
    TestStreamOperationsRouting,
)

__all__ = [  # pragma: allowlist secret
    "AsyncIteratorMock",
    "TestAppendRouting",
    "TestConcurrentOperations",
    "TestDualWriteInterceptorManagement",
    "TestDualWriteStateWithoutInterceptor",
    "TestEvent",
    "TestHelperMethods",
    "TestMigrationStateRouting",
    "TestReadOperationsRouting",
    "TestRouterStructuralConformance",
    "TestStoreNotFoundError",
    "TestStoreRegistryManagement",
    "TestStreamOperationsRouting",
    "TestTenantStoreResolution",
    "TestTenantStoreRouterInit",
    "TestWaitIfPaused",
    "TestWritePauseManagement",
    "TestWritePausedError",
    "async_generator_mock",
    "create_mock_store",
    "mock_dedicated_store",
    "mock_default_store",
    "mock_routing_repo",
    "router",
    "sid",
]
