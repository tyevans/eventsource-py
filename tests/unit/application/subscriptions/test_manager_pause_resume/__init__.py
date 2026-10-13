"""Unit tests for SubscriptionManager pause/resume functionality (PHASE3-005)."""

from tests.unit.application.subscriptions.test_manager_pause_resume._fixtures import (
    CustomerProjection,
    MockSubscriber,
    OrderProjection,
    PauseTestEvent,
    add_events_to_store,
    checkpoint_repo,
    event_bus,
    event_store,
    manager,
)
from tests.unit.application.subscriptions.test_manager_pause_resume.test_pause_resume_bulk_and_lifecycle import (
    TestHealthStatusWithPaused,
    TestManagerPauseAll,
    TestManagerResumeAll,
    TestPauseResumeLifecycle,
)
from tests.unit.application.subscriptions.test_manager_pause_resume.test_pause_resume_single import (
    TestManagerPauseSubscription,
    TestManagerResumeSubscription,
    TestPausedSubscriptionsProperties,
)

__all__ = [  # pragma: allowlist secret
    "CustomerProjection",
    "MockSubscriber",
    "OrderProjection",
    "PauseTestEvent",
    "TestHealthStatusWithPaused",
    "TestManagerPauseAll",
    "TestManagerPauseSubscription",
    "TestManagerResumeAll",
    "TestManagerResumeSubscription",
    "TestPauseResumeLifecycle",
    "TestPausedSubscriptionsProperties",
    "add_events_to_store",
    "checkpoint_repo",
    "event_bus",
    "event_store",
    "manager",
]
