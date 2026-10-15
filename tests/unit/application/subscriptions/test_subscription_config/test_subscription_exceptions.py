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
    def test_every_event_value(self):
        assert CheckpointStrategy.EVERY_EVENT.value == "every_event"

    def test_every_batch_value(self):
        assert CheckpointStrategy.EVERY_BATCH.value == "every_batch"

    def test_periodic_value(self):
        assert CheckpointStrategy.PERIODIC.value == "periodic"

    def test_enum_members(self):
        members = list(CheckpointStrategy)
        assert len(members) == 3
        assert CheckpointStrategy.EVERY_EVENT in members
        assert CheckpointStrategy.EVERY_BATCH in members
        assert CheckpointStrategy.PERIODIC in members


class TestSubscriptionExceptions:
    def test_subscription_error_base(self):
        with pytest.raises(SubscriptionError) as exc_info:
            raise SubscriptionError("Test error")
        assert str(exc_info.value) == "Test error"

    def test_subscription_error_is_exception(self):
        assert issubclass(SubscriptionError, Exception)

    def test_subscription_config_error(self):
        with pytest.raises(SubscriptionConfigError) as exc_info:
            raise SubscriptionConfigError("Invalid configuration")
        assert "Invalid configuration" in str(exc_info.value)

    def test_subscription_config_error_is_subscription_error(self):
        assert issubclass(SubscriptionConfigError, SubscriptionError)

    def test_subscription_state_error(self):
        with pytest.raises(SubscriptionStateError) as exc_info:
            raise SubscriptionStateError("Cannot pause in current state")
        assert "Cannot pause" in str(exc_info.value)

    def test_subscription_state_error_is_subscription_error(self):
        assert issubclass(SubscriptionStateError, SubscriptionError)

    def test_subscription_already_exists_error(self):
        with pytest.raises(SubscriptionAlreadyExistsError) as exc_info:
            raise SubscriptionAlreadyExistsError("order-projection")
        error = exc_info.value
        assert error.name == "order-projection"
        assert "order-projection" in str(error)
        assert "already exists" in str(error)

    def test_subscription_already_exists_error_is_subscription_error(self):
        assert issubclass(SubscriptionAlreadyExistsError, SubscriptionError)

    def test_checkpoint_not_found_error(self):
        with pytest.raises(CheckpointNotFoundError) as exc_info:
            raise CheckpointNotFoundError("my-projection")
        error = exc_info.value
        assert error.projection_name == "my-projection"
        assert "my-projection" in str(error)
        assert "No checkpoint found" in str(error)
        assert "start_from='beginning'" in str(error)

    def test_checkpoint_not_found_error_is_subscription_error(self):
        assert issubclass(CheckpointNotFoundError, SubscriptionError)

    def test_event_store_connection_error(self):
        with pytest.raises(EventStoreConnectionError) as exc_info:
            raise EventStoreConnectionError("Connection refused")
        assert "Connection refused" in str(exc_info.value)

    def test_event_store_connection_error_is_an_event_store_error(self):
        """EventStoreConnectionError belongs to the store taxonomy."""
        assert issubclass(EventStoreConnectionError, EventStoreError)
        assert not issubclass(EventStoreConnectionError, SubscriptionError)

    def test_event_bus_connection_error(self):
        with pytest.raises(EventBusConnectionError) as exc_info:
            raise EventBusConnectionError("Kafka unavailable")
        assert "Kafka unavailable" in str(exc_info.value)

    def test_event_bus_connection_error_is_subscription_error(self):
        assert issubclass(EventBusConnectionError, SubscriptionError)

    def test_transition_error(self):
        with pytest.raises(TransitionError) as exc_info:
            raise TransitionError("Gap detected during transition")
        assert "Gap detected" in str(exc_info.value)

    def test_transition_error_is_subscription_error(self):
        assert issubclass(TransitionError, SubscriptionError)


class TestExceptionHierarchy:
    def test_catch_all_subscription_exceptions_with_base(self):
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
        with pytest.raises(EventStoreError):
            raise EventStoreConnectionError("connection error")

    def test_each_exception_is_distinct(self):
        with pytest.raises(SubscriptionConfigError):
            raise SubscriptionConfigError("test")
        with pytest.raises(SubscriptionStateError):
            raise SubscriptionStateError("test")
        with pytest.raises(SubscriptionAlreadyExistsError):
            raise SubscriptionAlreadyExistsError("name")
        with pytest.raises(CheckpointNotFoundError):
            raise CheckpointNotFoundError("projection")
        with pytest.raises(EventStoreConnectionError):
            raise EventStoreConnectionError("test")
        with pytest.raises(EventBusConnectionError):
            raise EventBusConnectionError("test")
        with pytest.raises(TransitionError):
            raise TransitionError("test")


class TestModuleImports:
    def test_import_from_subscriptions_module(self):
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
        from eventsource.application.subscriptions import runners

        assert runners is not None
        assert "CatchUpRunner" in runners.__all__
        assert "CatchUpResult" in runners.__all__
