"""Multi-tenant live migration system for zero-downtime tenant event store transitions."""

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
from eventsource.application.migration.error_handling import ErrorHandler, classify_exception
from eventsource.application.migration.exceptions import (
    CircuitBreakerOpenError,
    ConsistencyError,
    CutoverError,
    CutoverTimeoutError,
    MigrationAlreadyExistsError,
    MigrationError,
    MigrationNotFoundError,
    MigrationStateError,
    PositionMappingError,
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

__all__ = [  # fmt: skip
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
    "WritePauseManager",
    "WritePausedError",
    "PauseMetrics",
    "PauseState",
    "MigrationError",
    "MigrationNotFoundError",
    "MigrationAlreadyExistsError",
    "MigrationStateError",
    "CutoverError",
    "CutoverTimeoutError",
    "ConsistencyError",
    "PositionMappingError",
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
    "VerificationLevel",
    "VerificationReport",
    "StreamConsistency",
    "ConsistencyViolation",
    "StatusStreamer",
    "StatusStreamManager",
    "MigrationMetrics",
    "MigrationMetricSnapshot",
    "ActiveMigrationsTracker",
    "get_migration_metrics",
    "release_migration_metrics",
    "clear_metrics_registry",
]
