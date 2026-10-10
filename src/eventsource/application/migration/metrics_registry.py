"""
Active migrations tracking and global metrics registry for migration operations.
"""

from __future__ import annotations

from typing import Any

from eventsource.application.migration.metrics_recorder import MigrationMetrics
from eventsource.application.migration.metrics_types import (
    OTEL_METRICS_AVAILABLE,
    _get_meter,
    reset_meter,
)


class ActiveMigrationsTracker:
    """
    Tracks active migrations for the active_migrations gauge.

    This is a singleton that maintains the count of active migrations
    and exposes it as an observable gauge.

    Example:
        >>> tracker = ActiveMigrationsTracker.get_instance()
        >>> tracker.register_migration("migration-123")
        >>> # ... migration runs ...
        >>> tracker.unregister_migration("migration-123")
    """

    _instance: ActiveMigrationsTracker | None = None
    _initialized: bool = False

    def __init__(self) -> None:
        """Initialize tracker."""
        self._active_count: int = 0
        self._migrations: set[str] = set()
        self._setup_gauge()

    @classmethod
    def get_instance(cls) -> ActiveMigrationsTracker:
        """
        Get the singleton instance.

        Returns:
            The singleton ActiveMigrationsTracker instance
        """
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    @classmethod
    def reset(cls) -> None:
        """
        Reset the singleton instance.

        Useful for testing to ensure fresh state.
        """
        cls._instance = None
        cls._initialized = False

    def _setup_gauge(self) -> None:
        """Set up the active migrations gauge."""
        if not OTEL_METRICS_AVAILABLE:
            return

        if ActiveMigrationsTracker._initialized:
            return

        meter = _get_meter()
        if meter is None:
            return

        meter.create_observable_gauge(
            name="migration.active",
            callbacks=[self._observe_active_count],
            unit="migrations",
            description="Number of currently active migrations",
        )
        ActiveMigrationsTracker._initialized = True

    def _observe_active_count(self, options: Any) -> Any:
        """
        Callback for observable active migrations gauge.

        Args:
            options: OpenTelemetry callback options

        Yields:
            Observation with active count
        """
        if OTEL_METRICS_AVAILABLE:
            from opentelemetry.metrics import Observation

            yield Observation(value=self._active_count)

    def register_migration(self, migration_id: str) -> None:
        """
        Register a migration as active.

        Args:
            migration_id: Unique migration identifier
        """
        if migration_id not in self._migrations:
            self._migrations.add(migration_id)
            self._active_count = len(self._migrations)

    def unregister_migration(self, migration_id: str) -> None:
        """
        Unregister a migration (no longer active).

        Args:
            migration_id: Unique migration identifier
        """
        if migration_id in self._migrations:
            self._migrations.discard(migration_id)
            self._active_count = len(self._migrations)

    @property
    def active_count(self) -> int:
        """Get current count of active migrations."""
        return self._active_count

    @property
    def active_migrations(self) -> set[str]:
        """Get set of active migration IDs."""
        return set(self._migrations)


# Global metrics registry for tracking all migration metrics instances
_metrics_registry: dict[str, MigrationMetrics] = {}


def get_migration_metrics(
    migration_id: str,
    tenant_id: str,
    enable_metrics: bool = True,
) -> MigrationMetrics:
    """
    Get or create metrics instance for a migration.

    Creates a new MigrationMetrics instance if one doesn't exist
    for the given migration ID, or returns the existing one.
    Also registers the migration as active.

    Args:
        migration_id: Unique migration identifier
        tenant_id: Tenant identifier
        enable_metrics: Whether to enable metrics (default True)

    Returns:
        MigrationMetrics instance for the migration
    """
    if migration_id not in _metrics_registry:
        _metrics_registry[migration_id] = MigrationMetrics(
            migration_id=migration_id,
            tenant_id=tenant_id,
            enable_metrics=enable_metrics,
        )
        # Register as active
        tracker = ActiveMigrationsTracker.get_instance()
        tracker.register_migration(migration_id)

    return _metrics_registry[migration_id]


def release_migration_metrics(migration_id: str) -> None:
    """
    Release metrics instance for a completed migration.

    Removes the migration from the registry and unregisters it as active.

    Args:
        migration_id: Unique migration identifier
    """
    if migration_id in _metrics_registry:
        del _metrics_registry[migration_id]
        tracker = ActiveMigrationsTracker.get_instance()
        tracker.unregister_migration(migration_id)


def clear_metrics_registry() -> None:
    """
    Clear the metrics registry.

    Useful for testing to reset state between tests.
    """
    global _metrics_registry
    _metrics_registry = {}
    reset_meter()
    ActiveMigrationsTracker.reset()


__all__ = [
    "ActiveMigrationsTracker",
    "clear_metrics_registry",
    "get_migration_metrics",
    "release_migration_metrics",
]
