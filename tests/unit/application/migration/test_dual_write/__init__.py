"""Unit tests for DualWriteInterceptor package."""

from ._fixtures import (
    TestEvent,
    async_generator_mock,
    create_mock_store,
    interceptor,
    sid,
    source_store,
    target_store,
    tenant_id,
)
from .test_dual_write_failures import (
    TestAppendSourceFailure,
    TestAppendTargetFailure,
    TestFailureStats,
    TestFailureTracking,
)
from .test_dual_write_init_and_append import (
    TestAppendSuccess,
    TestDualWriteInterceptorInit,
    TestFailedWrite,
)
from .test_dual_write_operations import (
    TestConcurrentOperations,
    TestEdgeCases,
    TestReadOperations,
    TestRouterIntegration,
)

__all__ = [  # pragma: allowlist secret
    "TestAppendSourceFailure",
    "TestAppendSuccess",
    "TestAppendTargetFailure",
    "TestConcurrentOperations",
    "TestDualWriteInterceptorInit",
    "TestEdgeCases",
    "TestEvent",
    "TestFailedWrite",
    "TestFailureStats",
    "TestFailureTracking",
    "TestReadOperations",
    "TestRouterIntegration",
    "async_generator_mock",
    "create_mock_store",
    "interceptor",
    "sid",
    "source_store",
    "target_store",
    "tenant_id",
]
