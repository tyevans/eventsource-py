"""Unit tests for AggregateRepository tracing functionality.

O11Y-008: Tests for AggregateRepository Tracer composition integration.
"""

from __future__ import annotations

from ._fixtures import (  # pragma: allowlist secret
    TracingTestAggregate,
    TracingTestEvent,
    TracingTestState,
)
from .test_span_attributes import (  # pragma: allowlist secret
    TestAggregateRepositorySpanDynamicAttributes,
    TestAggregateRepositoryTracingMultipleEvents,
)
from .test_span_creation import TestAggregateRepositorySpanCreation  # pragma: allowlist secret
from .test_tracing_composition import (  # pragma: allowlist secret
    TestAggregateRepositoryStandardAttributes,
    TestAggregateRepositoryTracingComposition,
    TestAggregateRepositoryTracingDisabled,
)

__all__ = [  # pragma: allowlist secret
    "TestAggregateRepositorySpanCreation",  # pragma: allowlist secret
    "TestAggregateRepositorySpanDynamicAttributes",  # pragma: allowlist secret
    "TestAggregateRepositoryStandardAttributes",  # pragma: allowlist secret
    "TestAggregateRepositoryTracingComposition",  # pragma: allowlist secret
    "TestAggregateRepositoryTracingDisabled",  # pragma: allowlist secret
    "TestAggregateRepositoryTracingMultipleEvents",  # pragma: allowlist secret
    "TracingTestAggregate",  # pragma: allowlist secret
    "TracingTestEvent",  # pragma: allowlist secret
    "TracingTestState",  # pragma: allowlist secret
]
