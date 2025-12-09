"""
Unit tests for subscription configuration module.

Tests all configuration classes, validation, and exception hierarchy.
"""

import pytest

from eventsource.subscriptions import (
    CheckpointNotFoundError,
    CheckpointStrategy,
    EventBusConnectionError,
    EventStoreConnectionError,
    SubscriptionAlreadyExistsError,
    SubscriptionConfig,
    SubscriptionConfigError,
    SubscriptionError,
    SubscriptionStateError,
    TransitionError,
    create_catch_up_config,
    create_live_only_config,
)


class TestSubscriptionConfigDefaults:
    """Tests for default configuration values."""

    def test_default_start_from(self):
        """Test default start_from is checkpoint."""
        config = SubscriptionConfig()
        assert config.start_from == "checkpoint"

    def test_default_batch_size(self):
        """Test default batch_size is 100."""
        config = SubscriptionConfig()
        assert config.batch_size == 100

    def test_default_max_in_flight(self):
        """Test default max_in_flight is 1000."""
        config = SubscriptionConfig()
        assert config.max_in_flight == 1000

    def test_default_checkpoint_strategy(self):
        """Test default checkpoint_strategy is EVERY_BATCH."""
        config = SubscriptionConfig()
        assert config.checkpoint_strategy == CheckpointStrategy.EVERY_BATCH

    def test_default_checkpoint_interval_seconds(self):
        """Test default checkpoint_interval_seconds is 5.0."""
        config = SubscriptionConfig()
        assert config.checkpoint_interval_seconds == 5.0

    def test_default_processing_timeout(self):
        """Test default processing_timeout is 30.0."""
        config = SubscriptionConfig()
        assert config.processing_timeout == 30.0

    def test_default_shutdown_timeout(self):
        """Test default shutdown_timeout is 30.0."""
        config = SubscriptionConfig()
        assert config.shutdown_timeout == 30.0

    def test_default_event_types(self):
        """Test default event_types is None."""
        config = SubscriptionConfig()
        assert config.event_types is None

    def test_default_aggregate_types(self):
        """Test default aggregate_types is None."""
        config = SubscriptionConfig()
        assert config.aggregate_types is None

    def test_default_continue_on_error(self):
        """Test default continue_on_error is True."""
        config = SubscriptionConfig()
        assert config.continue_on_error is True


class TestSubscriptionConfigCustomValues:
    """Tests for custom configuration values."""

    def test_start_from_beginning(self):
        """Test start_from='beginning' is valid."""
        config = SubscriptionConfig(start_from="beginning")
        assert config.start_from == "beginning"

    def test_start_from_end(self):
        """Test start_from='end' is valid."""
        config = SubscriptionConfig(start_from="end")
        assert config.start_from == "end"

    def test_start_from_checkpoint(self):
        """Test start_from='checkpoint' is valid."""
        config = SubscriptionConfig(start_from="checkpoint")
        assert config.start_from == "checkpoint"

    def test_start_from_integer_position(self):
        """Test start_from with integer position is valid."""
        config = SubscriptionConfig(start_from=500)
        assert config.start_from == 500

    def test_start_from_zero(self):
        """Test start_from=0 is valid."""
        config = SubscriptionConfig(start_from=0)
        assert config.start_from == 0

    def test_custom_batch_size(self):
        """Test custom batch_size."""
        config = SubscriptionConfig(batch_size=500)
        assert config.batch_size == 500

    def test_custom_max_in_flight(self):
        """Test custom max_in_flight."""
        config = SubscriptionConfig(max_in_flight=2000)
        assert config.max_in_flight == 2000

    def test_custom_checkpoint_strategy_every_event(self):
        """Test checkpoint_strategy=EVERY_EVENT."""
        config = SubscriptionConfig(checkpoint_strategy=CheckpointStrategy.EVERY_EVENT)
        assert config.checkpoint_strategy == CheckpointStrategy.EVERY_EVENT

    def test_custom_checkpoint_strategy_periodic(self):
        """Test checkpoint_strategy=PERIODIC."""
        config = SubscriptionConfig(checkpoint_strategy=CheckpointStrategy.PERIODIC)
        assert config.checkpoint_strategy == CheckpointStrategy.PERIODIC

    def test_custom_checkpoint_interval(self):
        """Test custom checkpoint_interval_seconds."""
        config = SubscriptionConfig(checkpoint_interval_seconds=10.0)
        assert config.checkpoint_interval_seconds == 10.0

    def test_custom_processing_timeout(self):
        """Test custom processing_timeout."""
        config = SubscriptionConfig(processing_timeout=60.0)
        assert config.processing_timeout == 60.0

    def test_custom_shutdown_timeout(self):
        """Test custom shutdown_timeout."""
        config = SubscriptionConfig(shutdown_timeout=45.0)
        assert config.shutdown_timeout == 45.0

    def test_custom_aggregate_types(self):
        """Test custom aggregate_types."""
        config = SubscriptionConfig(aggregate_types=("Order", "User"))
        assert config.aggregate_types == ("Order", "User")

    def test_continue_on_error_false(self):
        """Test continue_on_error=False."""
        config = SubscriptionConfig(continue_on_error=False)
        assert config.continue_on_error is False


class TestSubscriptionConfigValidation:
    """Tests for configuration validation."""

    def test_invalid_batch_size_zero(self):
        """Test batch_size=0 raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            SubscriptionConfig(batch_size=0)
        assert "batch_size must be positive" in str(exc_info.value)
        assert "got 0" in str(exc_info.value)

    def test_invalid_batch_size_negative(self):
        """Test negative batch_size raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            SubscriptionConfig(batch_size=-10)
        assert "batch_size must be positive" in str(exc_info.value)
        assert "got -10" in str(exc_info.value)

    def test_invalid_max_in_flight_zero(self):
        """Test max_in_flight=0 raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            SubscriptionConfig(max_in_flight=0)
        assert "max_in_flight must be positive" in str(exc_info.value)
        assert "got 0" in str(exc_info.value)

    def test_invalid_max_in_flight_negative(self):
        """Test negative max_in_flight raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            SubscriptionConfig(max_in_flight=-100)
        assert "max_in_flight must be positive" in str(exc_info.value)
        assert "got -100" in str(exc_info.value)

    def test_invalid_start_from_negative_integer(self):
        """Test negative integer start_from raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            SubscriptionConfig(start_from=-5)
        assert "start_from position must be >= 0" in str(exc_info.value)
        assert "got -5" in str(exc_info.value)

    def test_invalid_processing_timeout_zero(self):
        """Test processing_timeout=0 raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            SubscriptionConfig(processing_timeout=0)
        assert "processing_timeout must be positive" in str(exc_info.value)
        assert "got 0" in str(exc_info.value)

    def test_invalid_processing_timeout_negative(self):
        """Test negative processing_timeout raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            SubscriptionConfig(processing_timeout=-1.0)
        assert "processing_timeout must be positive" in str(exc_info.value)
        assert "got -1.0" in str(exc_info.value)

    def test_invalid_shutdown_timeout_zero(self):
        """Test shutdown_timeout=0 raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            SubscriptionConfig(shutdown_timeout=0)
        assert "shutdown_timeout must be positive" in str(exc_info.value)
        assert "got 0" in str(exc_info.value)

    def test_invalid_shutdown_timeout_negative(self):
        """Test negative shutdown_timeout raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            SubscriptionConfig(shutdown_timeout=-5.0)
        assert "shutdown_timeout must be positive" in str(exc_info.value)
        assert "got -5.0" in str(exc_info.value)

    def test_invalid_checkpoint_interval_zero(self):
        """Test checkpoint_interval_seconds=0 raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            SubscriptionConfig(checkpoint_interval_seconds=0)
        assert "checkpoint_interval_seconds must be positive" in str(exc_info.value)
        assert "got 0" in str(exc_info.value)

    def test_invalid_checkpoint_interval_negative(self):
        """Test negative checkpoint_interval_seconds raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            SubscriptionConfig(checkpoint_interval_seconds=-2.5)
        assert "checkpoint_interval_seconds must be positive" in str(exc_info.value)
        assert "got -2.5" in str(exc_info.value)


class TestSubscriptionConfigImmutability:
    """Tests for configuration immutability (frozen dataclass)."""

    def test_cannot_modify_batch_size(self):
        """Test that batch_size cannot be modified."""
        config = SubscriptionConfig()
        with pytest.raises(AttributeError):
            config.batch_size = 500  # type: ignore[misc]

    def test_cannot_modify_start_from(self):
        """Test that start_from cannot be modified."""
        config = SubscriptionConfig()
        with pytest.raises(AttributeError):
            config.start_from = "beginning"  # type: ignore[misc]

    def test_cannot_modify_checkpoint_strategy(self):
        """Test that checkpoint_strategy cannot be modified."""
        config = SubscriptionConfig()
        with pytest.raises(AttributeError):
            config.checkpoint_strategy = CheckpointStrategy.EVERY_EVENT  # type: ignore[misc]

    def test_cannot_modify_continue_on_error(self):
        """Test that continue_on_error cannot be modified."""
        config = SubscriptionConfig()
        with pytest.raises(AttributeError):
            config.continue_on_error = False  # type: ignore[misc]


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


class TestConvenienceFunctions:
    """Tests for convenience functions."""

    def test_create_catch_up_config_defaults(self):
        """Test create_catch_up_config with defaults."""
        config = create_catch_up_config()
        assert config.start_from == "checkpoint"
        assert config.batch_size == 1000
        assert config.checkpoint_strategy == CheckpointStrategy.EVERY_BATCH

    def test_create_catch_up_config_custom_batch_size(self):
        """Test create_catch_up_config with custom batch_size."""
        config = create_catch_up_config(batch_size=2000)
        assert config.batch_size == 2000

    def test_create_catch_up_config_periodic_checkpoint(self):
        """Test create_catch_up_config with periodic checkpointing."""
        config = create_catch_up_config(checkpoint_every_batch=False)
        assert config.checkpoint_strategy == CheckpointStrategy.PERIODIC

    def test_create_live_only_config(self):
        """Test create_live_only_config."""
        config = create_live_only_config()
        assert config.start_from == "end"
        assert config.batch_size == 100
        assert config.checkpoint_strategy == CheckpointStrategy.EVERY_EVENT


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

    def test_event_store_connection_error_is_subscription_error(self):
        """Test EventStoreConnectionError is subclass of SubscriptionError."""
        assert issubclass(EventStoreConnectionError, SubscriptionError)

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
            EventStoreConnectionError("connection error"),
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
        from eventsource.subscriptions import (
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
        from eventsource.subscriptions.config import (
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
        """Test direct import from exceptions submodule."""
        from eventsource.subscriptions.exceptions import (
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
        from eventsource.subscriptions import runners

        assert runners is not None
        assert "CatchUpRunner" in runners.__all__
        assert "CatchUpResult" in runners.__all__
