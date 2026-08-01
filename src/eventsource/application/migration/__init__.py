"""
Multi-tenant live migration system for eventsource.

This module provides zero-downtime migration of tenant event data
between event stores (e.g., shared to dedicated PostgreSQL).

Key Components:
    - MigrationCoordinator: Orchestrates the migration lifecycle
    - TenantStoreRouter: Routes operations based on migration state
    - BulkCopier: Streams historical events to target store
    - DualWriteInterceptor: Transparent dual-write during sync
    - CutoverManager: Atomic switch with minimal pause

Migration Phases:
    1. PENDING: Migration created but not started
    2. BULK_COPY: Historical events being copied to target
    3. DUAL_WRITE: New events written to both stores
    4. CUTOVER: Atomic switch to target store
    5. COMPLETED: Migration finished successfully

Usage:
    >>> from eventsource.application.migration import (
    ...     MigrationCoordinator,
    ...     MigrationConfig,
    ...     TenantStoreRouter,
    ... )
    >>>
    >>> # Set up router
    >>> router = TenantStoreRouter(default_store, routing_repo)
    >>>
    >>> # Create coordinator
    >>> coordinator = MigrationCoordinator(
    ...     source_store, migration_repo, routing_repo, router,
    ... )
    >>>
    >>> # Start migration
    >>> migration = await coordinator.start_migration(
    ...     tenant_id=tenant_id,
    ...     target_store=dedicated_store,
    ... )
    >>>
    >>> # Monitor progress
    >>> async for status in coordinator.stream_status(migration.id):
    ...     print(f"Phase: {status.phase}, Progress: {status.progress_percent}%")

See Also:
    - FRD: docs/tasks/multi-tenant-live-migration/multi-tenant-live-migration.md
"""

from eventsource.application.migration.bulk_copier import BulkCopier
from eventsource.application.migration.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitState,
)
from eventsource.application.migration.consistency import (
    ConsistencyVerifier,
    ConsistencyViolation,
    StreamConsistency,
    VerificationLevel,
    VerificationReport,
)
from eventsource.application.migration.coordinator import MigrationCoordinator
from eventsource.application.migration.cutover import CutoverManager
from eventsource.application.migration.dual_write import DualWriteInterceptor
from eventsource.application.migration.error_classification import (
    CONNECTIVITY_RETRY_CONFIG,
    CUTOVER_RETRY_CONFIG,
    TRANSIENT_RETRY_CONFIG,
    ErrorClassification,
    ErrorRecoverability,
    ErrorSeverity,
    RetryConfig,
)
from eventsource.application.migration.exceptions import (
    CircuitBreakerOpenError,
    # Core exceptions
    ConsistencyError,
    CutoverError,
    CutoverTimeoutError,
    ErrorHandler,
    MigrationAlreadyExistsError,
    MigrationError,
    MigrationNotFoundError,
    MigrationStateError,
    PositionMappingError,
    RoutingError,
    classify_exception,
)
from eventsource.application.migration.metrics import (
    ActiveMigrationsTracker,
    MigrationMetrics,
    MigrationMetricSnapshot,
    clear_metrics_registry,
    get_migration_metrics,
    release_migration_metrics,
)
from eventsource.application.migration.position_mapper import PositionMapper
from eventsource.application.migration.router import TenantStoreRouter
from eventsource.application.migration.status_streamer import StatusStreamer, StatusStreamManager
from eventsource.application.migration.subscription_migrator import (
    MigrationPlan,
    MigrationSummary,
    PlannedMigration,
    SubscriptionMigrationError,
    SubscriptionMigrationResult,
    SubscriptionMigrator,
)
from eventsource.application.migration.sync_lag_tracker import LagSample, LagStats, SyncLagTracker
from eventsource.application.migration.write_pause import (
    PauseMetrics,
    PauseState,
    WritePausedError,
    WritePauseManager,
)
from eventsource.ports.migration.models import (
    AuditEventType,
    CutoverResult,
    Migration,
    MigrationAuditEntry,
    MigrationConfig,
    MigrationPhase,
    MigrationResult,
    MigrationStatus,
    PositionMapping,
    SyncLag,
    TenantMigrationState,
    TenantRouting,
)

__all__ = [
    # Models
    "Migration",
    "MigrationConfig",
    "MigrationPhase",
    "MigrationStatus",
    "MigrationResult",
    "MigrationAuditEntry",
    "AuditEventType",
    "TenantRouting",
    "TenantMigrationState",
    "PositionMapping",
    "SyncLag",
    "CutoverResult",
    # Write Pause
    "WritePauseManager",
    "WritePausedError",
    "PauseMetrics",
    "PauseState",
    # Exceptions
    "MigrationError",
    "MigrationNotFoundError",
    "MigrationAlreadyExistsError",
    "MigrationStateError",
    "CutoverError",
    "CutoverTimeoutError",
    "ConsistencyError",
    "PositionMappingError",
    "RoutingError",
    # Error Classification (P4-004)
    "ErrorSeverity",
    "ErrorRecoverability",
    "ErrorClassification",
    "RetryConfig",
    "CircuitBreaker",
    "CircuitBreakerConfig",
    "CircuitBreakerOpenError",
    "CircuitState",
    "ErrorHandler",
    "classify_exception",
    "TRANSIENT_RETRY_CONFIG",
    "CONNECTIVITY_RETRY_CONFIG",
    "CUTOVER_RETRY_CONFIG",
    # Components
    "MigrationCoordinator",
    "TenantStoreRouter",
    "BulkCopier",
    "DualWriteInterceptor",
    "CutoverManager",
    "PositionMapper",
    "ConsistencyVerifier",
    "SubscriptionMigrator",
    "SubscriptionMigrationResult",
    "SubscriptionMigrationError",
    "PlannedMigration",
    "MigrationPlan",
    "MigrationSummary",
    "SyncLagTracker",
    "LagSample",
    "LagStats",
    # Consistency Verification
    "VerificationLevel",
    "VerificationReport",
    "StreamConsistency",
    "ConsistencyViolation",
    # Status Streaming (P4-002)
    "StatusStreamer",
    "StatusStreamManager",
    # Migration Metrics (P4-003)
    "MigrationMetrics",
    "MigrationMetricSnapshot",
    "ActiveMigrationsTracker",
    "get_migration_metrics",
    "release_migration_metrics",
    "clear_metrics_registry",
]
