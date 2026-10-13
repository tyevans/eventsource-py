"""Tests for MigrationConfig dataclass."""

import pytest

from eventsource.ports.migration.models import MigrationConfig


class TestMigrationConfig:
    """Tests for MigrationConfig dataclass."""

    def test_default_values(self) -> None:
        """Test default configuration values."""
        config = MigrationConfig()
        assert config.batch_size == 1000
        assert config.max_bulk_copy_rate == 10000
        assert config.cutover_max_lag_events == 0
        assert config.cutover_timeout_ms == 500
        assert config.position_mapping_enabled is True
        assert config.verify_consistency is True
        assert config.migrate_subscriptions is True

    def test_custom_values(self) -> None:
        """Test custom configuration values."""
        config = MigrationConfig(
            batch_size=500,
            max_bulk_copy_rate=5000,
            cutover_max_lag_events=50,
            cutover_timeout_ms=200,
            position_mapping_enabled=False,
            verify_consistency=False,
            migrate_subscriptions=False,
        )
        assert config.batch_size == 500
        assert config.max_bulk_copy_rate == 5000
        assert config.cutover_max_lag_events == 50
        assert config.cutover_timeout_ms == 200
        assert config.position_mapping_enabled is False
        assert config.verify_consistency is False
        assert config.migrate_subscriptions is False

    def test_is_frozen(self) -> None:
        """Test that MigrationConfig is frozen (immutable)."""
        config = MigrationConfig()
        with pytest.raises(AttributeError):
            config.batch_size = 500  # type: ignore[misc]

    def test_validation_batch_size(self) -> None:
        """Test batch_size validation."""
        with pytest.raises(ValueError, match="batch_size must be >= 1"):
            MigrationConfig(batch_size=0)
        with pytest.raises(ValueError, match="batch_size must be >= 1"):
            MigrationConfig(batch_size=-1)

    def test_validation_max_bulk_copy_rate(self) -> None:
        """Test max_bulk_copy_rate validation."""
        with pytest.raises(ValueError, match="max_bulk_copy_rate must be >= 1"):
            MigrationConfig(max_bulk_copy_rate=0)

    def test_validation_cutover_max_lag_events(self) -> None:
        """Test cutover_max_lag_events validation."""
        with pytest.raises(ValueError, match="cutover_max_lag_events must be >= 0"):
            MigrationConfig(cutover_max_lag_events=-1)
        # Zero is valid
        config = MigrationConfig(cutover_max_lag_events=0)
        assert config.cutover_max_lag_events == 0

    def test_validation_cutover_timeout_ms(self) -> None:
        """Test cutover_timeout_ms validation."""
        with pytest.raises(ValueError, match="cutover_timeout_ms must be >= 100"):
            MigrationConfig(cutover_timeout_ms=99)
        # Exactly 100 is valid
        config = MigrationConfig(cutover_timeout_ms=100)
        assert config.cutover_timeout_ms == 100

    def test_to_dict(self) -> None:
        """Test to_dict serialization."""
        config = MigrationConfig(batch_size=500, verify_consistency=False)
        data = config.to_dict()
        assert data["batch_size"] == 500
        assert data["max_bulk_copy_rate"] == 10000
        assert data["verify_consistency"] is False
        assert "position_mapping_enabled" in data

    def test_from_dict(self) -> None:
        """Test from_dict deserialization."""
        data = {
            "batch_size": 500,
            "cutover_max_lag_events": 50,
            "verify_consistency": False,
        }
        config = MigrationConfig.from_dict(data)
        assert config.batch_size == 500
        assert config.cutover_max_lag_events == 50
        assert config.verify_consistency is False
        # Defaults are applied for missing keys
        assert config.max_bulk_copy_rate == 10000

    def test_from_dict_empty(self) -> None:
        """Test from_dict with empty dictionary uses defaults."""
        config = MigrationConfig.from_dict({})
        assert config.batch_size == 1000
        assert config.max_bulk_copy_rate == 10000

    def test_from_dict_defaults_to_strict_zero_lag(self) -> None:
        assert MigrationConfig.from_dict({}).cutover_max_lag_events == 0

    def test_to_dict_from_dict_roundtrip(self) -> None:
        """Test to_dict/from_dict roundtrip."""
        original = MigrationConfig(
            batch_size=500,
            max_bulk_copy_rate=8000,
        )
        data = original.to_dict()
        restored = MigrationConfig.from_dict(data)
        assert original == restored
