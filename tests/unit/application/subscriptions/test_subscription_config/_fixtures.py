"""Fixtures for subscription configuration and exception tests."""

import pytest

from eventsource.application.subscriptions import (
    CheckpointNotFoundError,
    EventBusConnectionError,
    SubscriptionAlreadyExistsError,
    SubscriptionConfig,
    SubscriptionConfigError,
    SubscriptionError,
    SubscriptionStateError,
    TransitionError,
    create_catch_up_config,
    create_live_only_config,
)


@pytest.fixture
def default_config() -> SubscriptionConfig:
    """Return a default SubscriptionConfig instance."""
    return SubscriptionConfig()


@pytest.fixture
def sample_catch_up_config() -> SubscriptionConfig:
    """Return a default catch-up SubscriptionConfig instance."""
    return create_catch_up_config()


@pytest.fixture
def sample_live_only_config() -> SubscriptionConfig:
    """Return a default live-only SubscriptionConfig instance."""
    return create_live_only_config()


@pytest.fixture
def sample_subscription_exceptions() -> list[SubscriptionError]:
    """Return a representative sample list of subscription exceptions."""
    return [
        SubscriptionConfigError("config error"),
        SubscriptionStateError("state error"),
        SubscriptionAlreadyExistsError("name"),
        CheckpointNotFoundError("projection"),
        EventBusConnectionError("bus error"),
        TransitionError("transition error"),
    ]
