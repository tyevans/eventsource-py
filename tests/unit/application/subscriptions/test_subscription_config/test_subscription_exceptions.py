"""Tests for CheckpointStrategy enum, subscription exceptions, and module imports."""

import pytest

from eventsource.application.subscriptions import (
    CheckpointNotFoundError,
    CheckpointStrategy,
    EventBusConnectionError,
    EventStoreConnectionError,
    SubscriptionAlreadyExistsError,
    SubscriptionConfigError,
    SubscriptionError,
    SubscriptionStateError,
    TransitionError,
)
from eventsource.domain.exceptions import EventStoreError


class TestCheckpointStrategy:
    """Tests for CheckpointStrategy enum."""

    def test_every_event_value(self):
        """Test EVERY_EVENT has correct value."""
        assert CheckpointStrategy.EVERY_EVENT.value == "every_event"

    def test_every_batch_value(self):
        """Test EVERY_BATCH has correct value."""
        assert CheckpointStrategy.EVERY_BATCH.value == "every_batch"

    def test_periodic_value(self):
        """Test PERIODIC has correct value."""
        assert CheckpointStrategy.PERIODIC.value == "periodic"

    def test_enum_members(self):
        """Test all expected enum members exist."""
        members = list(CheckpointStrategy)
        assert len(members) == 3
        assert CheckpointStrategy.EVERY_EVENT in members
        assert CheckpointStrategy.EVERY_BATCH in members
        assert CheckpointStrategy.PERIODIC in members


class TestSubscriptionExceptions:
    """Tests for subscription exceptions."""

    def test_subscription_error_base(self):
        """Test SubscriptionError can be raised with message."""
        with pytest.raises(SubscriptionError) as exc_info:
            raise SubscriptionError("Test error")
        assert str(exc_info.value) == "Test error"

    def test_subscription_error_is_exception(self):
        """Test SubscriptionError is subclass of Exception."""
        assert issubclass(SubscriptionError, Exception)

    def test_subscription_config_error(self):
        """Test SubscriptionConfigError."""
        with pytest.raises(SubscriptionConfigError) as exc_info:
            raise SubscriptionConfigError("Invalid configuration")
        assert "Invalid configuration" in str(exc_info.value)

    def test_subscription_config_error_is_subscription_error(self):
        """Test SubscriptionConfigError is subclass of SubscriptionError."""
        assert issubclass(SubscriptionConfigError, SubscriptionError)

    def test_subscription_state_error(self):
        """Test SubscriptionStateError."""
        with pytest.raises(SubscriptionStateError) as exc_info:
            raise SubscriptionStateError("Cannot pause in current state")
        assert "Cannot pause" in str(exc_info.value)

    def test_subscription_state_error_is_subscription_error(self):
        """Test SubscriptionStateError is subclass of SubscriptionError."""
        assert issubclass(SubscriptionStateError, SubscriptionError)

    def test_subscription_already_exists_error(self):
        """Test SubscriptionAlreadyExistsError."""
        with pytest.raises(SubscriptionAlreadyExistsError) as exc_info:
            raise SubscriptionAlreadyExistsError("order-projection")
        error = exc_info.value
        assert error.name == "order-projection"
        assert "order-projection" in str(error)
        assert "already exists" in str(error)

    def test_subscription_already_exists_error_is_subscription_error(self):
        """Test SubscriptionAlreadyExistsError is subclass of SubscriptionError."""
        assert issubclass(SubscriptionAlreadyExistsError, SubscriptionError)

    def test_checkpoint_not_found_error(self):
        """Test CheckpointNotFoundError."""
        with pytest.raises(CheckpointNotFoundError) as exc_info:
            raise CheckpointNotFoundError("my-projection")
        error = exc_info.value
        assert error.projection_name == "my-projection"
        assert "my-projection" in str(error)
        assert "No checkpoint found" in str(error)
        assert "start_from='beginning'" in str(error)

    def test_checkpoint_not_found_error_is_subscription_error(self):
        """Test CheckpointNotFoundError is subclass of SubscriptionError."""
        assert issubclass(CheckpointNotFoundError, SubscriptionError)

    def test_event_store_connection_error(self):
        """Test EventStoreConnectionError."""
        with pytest.raises(EventStoreConnectionError) as exc_info:
            raise EventStoreConnectionError("Connection refused")
        assert "Connection refused" in str(exc_info.value)

    def test_event_store_connection_error_is_an_event_store_error(self):
        """EventStoreConnectionError belongs to the store taxonomy.

        It was a SubscriptionError subclass, which filed a store-connection
        failure under subscriptions and made `except SubscriptionError` the
        only way to catch it -- including for callers that never ran a
        subscription.
        """
        assert issubclass(EventStoreConnectionError, EventStoreError)
        assert not issubclass(EventStoreConnectionError, SubscriptionError)

    def test_event_bus_connection_error(self):
        """Test EventBusConnectionError."""
        with pytest.raises(EventBusConnectionError) as exc_info:
            raise EventBusConnectionError("Kafka unavailable")
        assert "Kafka unavailable" in str(exc_info.value)

    def test_event_bus_connection_error_is_subscription_error(self):
        """Test EventBusConnectionError is subclass of SubscriptionError."""
        assert issubclass(EventBusConnectionError, SubscriptionError)

    def test_transition_error(self):
        """Test TransitionError."""
        with pytest.raises(TransitionError) as exc_info:
            raise TransitionError("Gap detected during transition")
        assert "Gap detected" in str(exc_info.value)

    def test_transition_error_is_subscription_error(self):
        """Test TransitionError is subclass of SubscriptionError."""
        assert issubclass(TransitionError, SubscriptionError)


class TestExceptionHierarchy:
    """Tests for exception hierarchy and catching."""

    def test_catch_all_subscription_exceptions_with_base(self):
        """Test that all subscription exceptions can be caught with SubscriptionError."""
        exceptions = [
            SubscriptionConfigError("config error"),
            SubscriptionStateError("state error"),
            SubscriptionAlreadyExistsError("name"),
            CheckpointNotFoundError("projection"),
            EventBusConnectionError("bus error"),
            TransitionError("transition error"),
        ]

        for exc in exceptions:
            try:
                raise exc
            except SubscriptionError as caught:
                assert caught is exc
            except Exception:
                pytest.fail(f"{type(exc).__name__} was not caught by SubscriptionError")

    def test_event_store_connection_error_is_not_in_that_catch_all(self):
        """It is deliberately outside the subscription hierarchy."""
        with pytest.raises(EventStoreError):
            raise EventStoreConnectionError("connection error")

    def test_each_exception_is_distinct(self):
        """Test that each exception type can be caught separately."""
        # SubscriptionConfigError
        with pytest.raises(SubscriptionConfigError):
            raise SubscriptionConfigError("test")

        # SubscriptionStateError
        with pytest.raises(SubscriptionStateError):
            raise SubscriptionStateError("test")

        # SubscriptionAlreadyExistsError
        with pytest.raises(SubscriptionAlreadyExistsError):
            raise SubscriptionAlreadyExistsError("name")

        # CheckpointNotFoundError
        with pytest.raises(CheckpointNotFoundError):
            raise CheckpointNotFoundError("projection")

        # EventStoreConnectionError
        with pytest.raises(EventStoreConnectionError):
            raise EventStoreConnectionError("test")

        # EventBusConnectionError
        with pytest.raises(EventBusConnectionError):
            raise EventBusConnectionError("test")

        # TransitionError
        with pytest.raises(TransitionError):
            raise TransitionError("test")


class TestModuleImports:
    """Tests for module imports and exports."""

    def test_import_from_subscriptions_module(self):
        """Test all public exports can be imported from subscriptions module."""
        from eventsource.application.subscriptions import (
            CheckpointNotFoundError,
            CheckpointStrategy,
            EventBusConnectionError,
            EventStoreConnectionError,
            StartPosition,
            SubscriptionAlreadyExistsError,
            SubscriptionConfig,
            SubscriptionConfigError,
            SubscriptionError,
            SubscriptionStateError,
            TransitionError,
            create_catch_up_config,
            create_live_only_config,
        )

        # Just verify they are accessible
        assert SubscriptionConfig is not None
        assert CheckpointStrategy is not None
        assert StartPosition is not None
        assert create_catch_up_config is not None
        assert create_live_only_config is not None
        assert SubscriptionError is not None
        assert SubscriptionConfigError is not None
        assert SubscriptionStateError is not None
        assert SubscriptionAlreadyExistsError is not None
        assert CheckpointNotFoundError is not None
        assert EventStoreConnectionError is not None
        assert EventBusConnectionError is not None
        assert TransitionError is not None

    def test_import_from_config_submodule(self):
        """Test direct import from config submodule."""
        from eventsource.application.subscriptions.config import (
            CheckpointStrategy,
            StartPosition,
            SubscriptionConfig,
            create_catch_up_config,
            create_live_only_config,
        )

        assert SubscriptionConfig is not None
        assert CheckpointStrategy is not None
        assert StartPosition is not None
        assert create_catch_up_config is not None
        assert create_live_only_config is not None

    def test_import_from_exceptions_submodule(self):
        """Test direct import from the exceptions' canonical home (ADR 0032)."""
        from eventsource.ports.exceptions import (
            CheckpointNotFoundError,
            EventBusConnectionError,
            EventStoreConnectionError,
            SubscriptionAlreadyExistsError,
            SubscriptionConfigError,
            SubscriptionError,
            SubscriptionStateError,
            TransitionError,
        )

        assert SubscriptionError is not None
        assert SubscriptionConfigError is not None
        assert SubscriptionStateError is not None
        assert SubscriptionAlreadyExistsError is not None
        assert CheckpointNotFoundError is not None
        assert EventStoreConnectionError is not None
        assert EventBusConnectionError is not None
        assert TransitionError is not None

    def test_runners_submodule_exists(self):
        """Test runners submodule can be imported."""
        from eventsource.application.subscriptions import runners

        assert runners is not None
        assert "CatchUpRunner" in runners.__all__
        assert "CatchUpResult" in runners.__all__
