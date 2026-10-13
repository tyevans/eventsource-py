"""Unit tests for subscription pause/resume functionality (PHASE3-005)."""

from tests.unit.application.subscriptions.test_pause_resume._fixtures import (
    MockSubscriber,
    config,
    mock_subscriber,
    pos,
    subscription,
)
from tests.unit.application.subscriptions.test_pause_resume.test_pause_resume_basic import (
    TestPauseProperties,
    TestPauseReasonEnum,
    TestPauseResumeModuleImports,
    TestSubscriptionPause,
    TestSubscriptionResume,
)
from tests.unit.application.subscriptions.test_pause_resume.test_pause_resume_scenarios import (
    TestConcurrentPauseResume,
    TestMultiplePauseResumeCycles,
    TestSubscriptionPauseInvalidStates,
    TestSubscriptionResumeInvalidStates,
    TestWaitIfPaused,
)

__all__ = [  # pragma: allowlist secret
    "MockSubscriber",
    "TestConcurrentPauseResume",
    "TestMultiplePauseResumeCycles",
    "TestPauseProperties",
    "TestPauseReasonEnum",
    "TestPauseResumeModuleImports",
    "TestSubscriptionPause",
    "TestSubscriptionPauseInvalidStates",
    "TestSubscriptionResume",
    "TestSubscriptionResumeInvalidStates",
    "TestWaitIfPaused",
    "config",
    "mock_subscriber",
    "pos",
    "subscription",
]
