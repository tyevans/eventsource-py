"""Tests for SubscriptionConfig defaults, custom values, validation, and immutability."""

import pytest

from eventsource.application.subscriptions import (
    CheckpointStrategy,
    SubscriptionConfig,
    create_catch_up_config,
    create_live_only_config,
)
from eventsource.ports.positions import Position


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

    def test_start_from_explicit_position(self):
        """Test start_from with an explicit position token is valid."""
        position = Position(store_id="test", key=(500,))
        config = SubscriptionConfig(start_from=position)
        assert config.start_from == position

    def test_start_from_rejects_int_at_type_check_time(self):
        """An int start_from is no longer part of StartPosition.

        There is no runtime rejection to assert: the int member was deleted
        from the type alias, so `SubscriptionConfig(start_from=500)` is a
        mypy error, not a ValueError. A Position validates itself on
        construction, which leaves no negative case to reject here.
        """

    def test_custom_batch_size(self):
        """Test custom batch_size."""
        config = SubscriptionConfig(batch_size=500)
        assert config.batch_size == 500

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

    def test_shutdown_timeout_is_not_a_subscription_config_field(self):
        """Shutdown timeout has exactly one declaration site: SubscriptionManager.

        The config previously declared a `shutdown_timeout` that nothing read --
        the manager's constructor argument is what reaches the shutdown
        coordinator. Re-adding it here must fail loudly (ADR 0062).
        """
        assert not hasattr(SubscriptionConfig(), "shutdown_timeout")
        with pytest.raises(TypeError):
            SubscriptionConfig(shutdown_timeout=45.0)  # type: ignore[call-arg]

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
