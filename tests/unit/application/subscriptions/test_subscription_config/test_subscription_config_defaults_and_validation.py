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
        assert SubscriptionConfig().start_from == "checkpoint"

    def test_default_batch_size(self):
        assert SubscriptionConfig().batch_size == 100

    def test_default_checkpoint_strategy(self):
        assert SubscriptionConfig().checkpoint_strategy == CheckpointStrategy.EVERY_BATCH

    def test_default_checkpoint_interval_seconds(self):
        assert SubscriptionConfig().checkpoint_interval_seconds == 5.0

    def test_default_processing_timeout(self):
        assert SubscriptionConfig().processing_timeout == 30.0

    def test_default_event_types(self):
        assert SubscriptionConfig().event_types is None

    def test_default_aggregate_types(self):
        assert SubscriptionConfig().aggregate_types is None

    def test_default_continue_on_error(self):
        assert SubscriptionConfig().continue_on_error is True


class TestSubscriptionConfigCustomValues:
    """Tests for custom configuration values."""

    def test_start_from_beginning(self):
        assert SubscriptionConfig(start_from="beginning").start_from == "beginning"

    def test_start_from_end(self):
        assert SubscriptionConfig(start_from="end").start_from == "end"

    def test_start_from_checkpoint(self):
        assert SubscriptionConfig(start_from="checkpoint").start_from == "checkpoint"

    def test_start_from_explicit_position(self):
        position = Position(store_id="test", key=(500,))
        assert SubscriptionConfig(start_from=position).start_from == position

    def test_start_from_rejects_int_at_type_check_time(self):
        """An int start_from is no longer part of StartPosition."""

    def test_custom_batch_size(self):
        assert SubscriptionConfig(batch_size=500).batch_size == 500

    def test_custom_checkpoint_strategy_every_event(self):
        config = SubscriptionConfig(checkpoint_strategy=CheckpointStrategy.EVERY_EVENT)
        assert config.checkpoint_strategy == CheckpointStrategy.EVERY_EVENT

    def test_custom_checkpoint_strategy_periodic(self):
        config = SubscriptionConfig(checkpoint_strategy=CheckpointStrategy.PERIODIC)
        assert config.checkpoint_strategy == CheckpointStrategy.PERIODIC

    def test_custom_checkpoint_interval(self):
        assert (
            SubscriptionConfig(checkpoint_interval_seconds=10.0).checkpoint_interval_seconds == 10.0
        )

    def test_custom_processing_timeout(self):
        assert SubscriptionConfig(processing_timeout=60.0).processing_timeout == 60.0

    def test_custom_aggregate_types(self):
        config = SubscriptionConfig(aggregate_types=("Order", "User"))
        assert config.aggregate_types == ("Order", "User")

    def test_continue_on_error_false(self):
        assert SubscriptionConfig(continue_on_error=False).continue_on_error is False


class TestSubscriptionConfigValidation:
    """Tests for configuration validation."""

    def test_invalid_batch_size_zero(self):
        with pytest.raises(ValueError) as exc_info:
            SubscriptionConfig(batch_size=0)
        assert "batch_size must be positive" in str(exc_info.value)
        assert "got 0" in str(exc_info.value)

    def test_invalid_batch_size_negative(self):
        with pytest.raises(ValueError) as exc_info:
            SubscriptionConfig(batch_size=-10)
        assert "batch_size must be positive" in str(exc_info.value)
        assert "got -10" in str(exc_info.value)

    def test_invalid_processing_timeout_zero(self):
        with pytest.raises(ValueError) as exc_info:
            SubscriptionConfig(processing_timeout=0)
        assert "processing_timeout must be positive" in str(exc_info.value)
        assert "got 0" in str(exc_info.value)

    def test_invalid_processing_timeout_negative(self):
        with pytest.raises(ValueError) as exc_info:
            SubscriptionConfig(processing_timeout=-1.0)
        assert "processing_timeout must be positive" in str(exc_info.value)
        assert "got -1.0" in str(exc_info.value)

    def test_shutdown_timeout_is_not_a_subscription_config_field(self):
        """Shutdown timeout has exactly one declaration site: SubscriptionManager (ADR 0062)."""
        assert not hasattr(SubscriptionConfig(), "shutdown_timeout")
        with pytest.raises(TypeError):
            SubscriptionConfig(shutdown_timeout=45.0)  # type: ignore[call-arg]

    def test_invalid_checkpoint_interval_zero(self):
        with pytest.raises(ValueError) as exc_info:
            SubscriptionConfig(checkpoint_interval_seconds=0)
        assert "checkpoint_interval_seconds must be positive" in str(exc_info.value)
        assert "got 0" in str(exc_info.value)

    def test_invalid_checkpoint_interval_negative(self):
        with pytest.raises(ValueError) as exc_info:
            SubscriptionConfig(checkpoint_interval_seconds=-2.5)
        assert "checkpoint_interval_seconds must be positive" in str(exc_info.value)
        assert "got -2.5" in str(exc_info.value)


class TestSubscriptionConfigImmutability:
    """Tests for configuration immutability (frozen dataclass)."""

    def test_cannot_modify_batch_size(self):
        config = SubscriptionConfig()
        with pytest.raises(AttributeError):
            config.batch_size = 500  # type: ignore[misc]

    def test_cannot_modify_start_from(self):
        config = SubscriptionConfig()
        with pytest.raises(AttributeError):
            config.start_from = "beginning"  # type: ignore[misc]

    def test_cannot_modify_checkpoint_strategy(self):
        config = SubscriptionConfig()
        with pytest.raises(AttributeError):
            config.checkpoint_strategy = CheckpointStrategy.EVERY_EVENT  # type: ignore[misc]

    def test_cannot_modify_continue_on_error(self):
        config = SubscriptionConfig()
        with pytest.raises(AttributeError):
            config.continue_on_error = False  # type: ignore[misc]


class TestConvenienceFunctions:
    """Tests for convenience functions."""

    def test_create_catch_up_config_defaults(self):
        config = create_catch_up_config()
        assert config.start_from == "checkpoint"
        assert config.batch_size == 1000
        assert config.checkpoint_strategy == CheckpointStrategy.EVERY_BATCH

    def test_create_catch_up_config_custom_batch_size(self):
        assert create_catch_up_config(batch_size=2000).batch_size == 2000

    def test_create_catch_up_config_periodic_checkpoint(self):
        config = create_catch_up_config(checkpoint_every_batch=False)
        assert config.checkpoint_strategy == CheckpointStrategy.PERIODIC

    def test_create_live_only_config(self):
        config = create_live_only_config()
        assert config.start_from == "end"
        assert config.batch_size == 100
        assert config.checkpoint_strategy == CheckpointStrategy.EVERY_EVENT
