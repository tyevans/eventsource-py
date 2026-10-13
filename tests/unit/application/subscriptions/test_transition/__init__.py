"""Tests for TransitionCoordinator and StartFromResolver."""

from tests.unit.application.subscriptions.test_transition._fixtures import (
    AnotherTransitionEvent,
    MockTransitionSubscriber,
    TransitionTestEvent,
    add_events_to_store,
    checkpoint_repo,
    config,
    coordinator,
    current,
    event_bus,
    event_store,
    subscriber,
    subscription,
)
from tests.unit.application.subscriptions.test_transition.test_start_from_resolver import (
    TestStartFromResolver,
)
from tests.unit.application.subscriptions.test_transition.test_transition_buffering import (
    TestTransitionBufferMode,
    TestTransitionLiveRunnerAccess,
)
from tests.unit.application.subscriptions.test_transition.test_transition_edge_cases import (
    TestTransitionEdgeCases,
)
from tests.unit.application.subscriptions.test_transition.test_transition_error_handling import (
    TestTransitionErrorHandling,
)
from tests.unit.application.subscriptions.test_transition.test_transition_lifecycle import (
    TestTransitionAlreadyCaughtUp,
    TestTransitionCoordinatorBasic,
    TestTransitionPhaseTracking,
    TestTransitionStop,
)
from tests.unit.application.subscriptions.test_transition.test_transition_types import (
    TestTransitionImports,
    TestTransitionPhase,
    TestTransitionResult,
)

__all__ = [  # pragma: allowlist secret
    "AnotherTransitionEvent",
    "MockTransitionSubscriber",
    "TestStartFromResolver",
    "TestTransitionAlreadyCaughtUp",
    "TestTransitionBufferMode",
    "TestTransitionCoordinatorBasic",
    "TestTransitionEdgeCases",
    "TestTransitionErrorHandling",
    "TestTransitionImports",
    "TestTransitionLiveRunnerAccess",
    "TestTransitionPhase",
    "TestTransitionPhaseTracking",
    "TestTransitionResult",
    "TestTransitionStop",
    "TransitionTestEvent",
    "add_events_to_store",
    "checkpoint_repo",
    "config",
    "coordinator",
    "current",
    "event_bus",
    "event_store",
    "subscriber",
    "subscription",
]
