"""
Configuration data structures for tenant migration.

This module defines the configuration options for tenant migrations,
controlling batch sizes, rates, timeouts, and verification behavior.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class MigrationConfig:
    """
    Configuration for a tenant migration.

    Controls batch sizes, timeouts, and thresholds for the migration process.
    This class is immutable (frozen) to prevent accidental modification
    during migration.

    Attributes:
        batch_size: Events per batch during bulk copy (default 1000).
        max_bulk_copy_rate: Max events/second during bulk copy (default 10000).
        cutover_max_lag_events: Max lag allowed before cutover (default 0 --
            strict). Any nonzero value permits cutover while that many
            source events are absent from the target; they are never
            copied, because writes are paused for the whole cutover and
            nothing in the sequence copies the residue. Set it only as an
            explicit acceptance of that bounded loss.
        cutover_timeout_ms: Hard timeout for cutover operation (default 500).
        position_mapping_enabled: Whether to record position mappings (default True).
        verify_consistency: Run consistency verification after migration (default True).
        migrate_subscriptions: Migrate subscription checkpoints (default True).

    Example:
        >>> config = MigrationConfig(
        ...     batch_size=500,
        ...     cutover_max_lag_events=50,
        ... )
        >>> config.batch_size
        500
    """

    batch_size: int = 1000
    max_bulk_copy_rate: int = 10000
    cutover_max_lag_events: int = 0
    cutover_timeout_ms: int = 500
    position_mapping_enabled: bool = True
    verify_consistency: bool = True
    migrate_subscriptions: bool = True

    def __post_init__(self) -> None:
        """Validate configuration values."""
        if self.batch_size < 1:
            raise ValueError(f"batch_size must be >= 1, got {self.batch_size}")

        if self.max_bulk_copy_rate < 1:
            raise ValueError(f"max_bulk_copy_rate must be >= 1, got {self.max_bulk_copy_rate}")

        if self.cutover_max_lag_events < 0:
            raise ValueError(
                f"cutover_max_lag_events must be >= 0, got {self.cutover_max_lag_events}"
            )

        if self.cutover_timeout_ms < 100:
            raise ValueError(f"cutover_timeout_ms must be >= 100, got {self.cutover_timeout_ms}")

    def to_dict(self) -> dict[str, Any]:
        """
        Convert to dictionary for JSON storage.

        Returns:
            Dictionary representation suitable for JSON serialization.
        """
        return {
            "batch_size": self.batch_size,
            "max_bulk_copy_rate": self.max_bulk_copy_rate,
            "cutover_max_lag_events": self.cutover_max_lag_events,
            "cutover_timeout_ms": self.cutover_timeout_ms,
            "position_mapping_enabled": self.position_mapping_enabled,
            "verify_consistency": self.verify_consistency,
            "migrate_subscriptions": self.migrate_subscriptions,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> MigrationConfig:
        """
        Create from dictionary.

        Args:
            data: Dictionary containing configuration values.

        Returns:
            MigrationConfig instance.
        """
        return cls(
            batch_size=data.get("batch_size", 1000),
            max_bulk_copy_rate=data.get("max_bulk_copy_rate", 10000),
            cutover_max_lag_events=data.get("cutover_max_lag_events", 0),
            cutover_timeout_ms=data.get("cutover_timeout_ms", 500),
            position_mapping_enabled=data.get("position_mapping_enabled", True),
            verify_consistency=data.get("verify_consistency", True),
            migrate_subscriptions=data.get("migrate_subscriptions", True),
        )


__all__ = [
    "MigrationConfig",
]
