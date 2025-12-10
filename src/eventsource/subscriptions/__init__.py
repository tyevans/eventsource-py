"""
Subscription management for eventsource.

This module provides catch-up subscriptions with seamless transition
to live event streaming.

Example:
    >>> from eventsource.subscriptions import SubscriptionManager, SubscriptionConfig
    >>>
    >>> manager = SubscriptionManager(event_store, event_bus, checkpoint_repo)
    >>> await manager.subscribe(my_projection)
    >>> await manager.start()

Classes:
    SubscriptionConfig: Configuration for subscriptions
    StartPosition: Type for start position options
    CheckpointStrategy: Enum for checkpoint strategies
    SubscriptionState: Enum for subscription states
    SubscriptionStatus: Status snapshot for health checks
    Subscription: Main subscription class with state machine

Protocols:
    Subscriber: Protocol for async event subscribers
    SyncSubscriber: Protocol for sync event subscribers
    BatchSubscriber: Protocol for batch event subscribers

Base Classes:
    BaseSubscriber: Abstract base class for subscribers
    BatchAwareSubscriber: Base class with batch processing support
    FilteringSubscriber: Base class with event filtering support

Type Aliases:
    EventHandler: Async handler for single events
    BatchHandler: Async handler for event batches

Functions:
    is_valid_transition: Check if state transition is valid
    supports_batch_handling: Check if subscriber supports batch processing
    get_subscribed_event_types: Get event types from subscriber

Exceptions:
    SubscriptionError: Base exception
    SubscriptionConfigError: Invalid configuration
    SubscriptionStateError: Invalid state operation
    SubscriptionAlreadyExistsError: Duplicate subscription
    CheckpointNotFoundError: Missing checkpoint
"""

from eventsource.subscriptions.config import (
    CheckpointStrategy,
    StartPosition,
    SubscriptionConfig,
    create_catch_up_config,
    create_live_only_config,
)
from eventsource.subscriptions.coordination import (
    COORDINATION_TOPIC_PREFIX,
    HEARTBEAT_TOPIC,
    SHUTDOWN_NOTIFICATIONS_TOPIC,
    WORK_ASSIGNMENT_TOPIC,
    HeartbeatCallback,
    HeartbeatMessage,
    InMemoryLeaderElector,
    LeaderChangeCallback,
    LeaderElector,
    LeaderElectorWithLease,
    PeerInfo,
    PeerShutdownCallback,
    PeerTimeoutCallback,
    SharedLeaderState,
    ShutdownIntent,
    ShutdownNotification,
    WorkAssignment,
    WorkAssignmentCallback,
    WorkRedistributionCoordinator,
)
from eventsource.subscriptions.error_handling import (
    ErrorCallback,
    ErrorCategory,
    ErrorClassification,
    ErrorClassifier,
    ErrorHandlerRegistry,
    ErrorHandlingConfig,
    ErrorHandlingStrategy,
    ErrorInfo,
    ErrorSeverity,
    ErrorStats,
    SubscriptionErrorHandler,
    SyncErrorCallback,
    get_default_classifier,
)
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
from eventsource.subscriptions.filtering import (
    EventFilter,
    FilterStats,
    matches_event_type,
    matches_pattern,
)
from eventsource.subscriptions.flow_control import (
    FlowControlContext,
    FlowController,
    FlowControlStats,
)
from eventsource.subscriptions.health import (
    HealthCheckConfig,
    HealthCheckResult,
    HealthIndicator,
    HealthStatus,
    LivenessStatus,
    # Health Check API (PHASE3-003)
    ManagerHealth,
    ManagerHealthChecker,
    ReadinessStatus,
    SubscriptionHealth,
    SubscriptionHealthChecker,
)
from eventsource.subscriptions.health_provider import HealthCheckProvider
from eventsource.subscriptions.lifecycle import SubscriptionLifecycleManager
from eventsource.subscriptions.manager import SubscriptionManager
from eventsource.subscriptions.metrics import (
    OTEL_METRICS_AVAILABLE,
    STATE_MAPPING,
    MetricSnapshot,
    NoOpCounter,
    NoOpGauge,
    NoOpHistogram,
    StateValue,
    SubscriptionMetrics,
    clear_metrics_registry,
    get_metrics,
    reset_meter,
)
from eventsource.subscriptions.pause_resume import PauseResumeController
from eventsource.subscriptions.registry import SubscriptionRegistry
from eventsource.subscriptions.retry import (
    TRANSIENT_EXCEPTIONS,
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitBreakerOpenError,
    CircuitState,
    RetryableOperation,
    RetryConfig,
    RetryError,
    RetryStats,
    calculate_backoff,
    is_retryable_exception,
    retry_async,
)
from eventsource.subscriptions.shutdown import (
    OTEL_METRICS_AVAILABLE as SHUTDOWN_OTEL_METRICS_AVAILABLE,
)
from eventsource.subscriptions.shutdown import (
    ShutdownCoordinator,
    ShutdownMetricsSnapshot,
    ShutdownPhase,
    ShutdownResult,
    get_in_flight_at_shutdown,
    record_drain_duration,
    record_events_drained,
    record_in_flight_at_shutdown,
    record_shutdown_completed,
    record_shutdown_initiated,
    reset_shutdown_metrics,
)
from eventsource.subscriptions.subscriber import (
    BaseSubscriber,
    BatchAwareSubscriber,
    BatchSubscriber,
    FilteringSubscriber,
    Subscriber,
    SyncSubscriber,
    get_subscribed_event_types,
    supports_batch_handling,
)
from eventsource.subscriptions.subscription import (
    VALID_TRANSITIONS,
    BatchHandler,
    EventHandler,
    PauseReason,
    RecentErrorInfo,
    Subscription,
    SubscriptionState,
    SubscriptionStatus,
    is_valid_transition,
)
from eventsource.subscriptions.transition import (
    StartFromResolver,
    TransitionCoordinator,
    TransitionPhase,
    TransitionResult,
)

__all__ = [
    # Manager and Components
    "SubscriptionManager",
    "SubscriptionRegistry",
    "SubscriptionLifecycleManager",
    "HealthCheckProvider",
    "PauseResumeController",
    # Configuration
    "SubscriptionConfig",
    "StartPosition",
    "CheckpointStrategy",
    "create_catch_up_config",
    "create_live_only_config",
    # Filtering
    "EventFilter",
    "FilterStats",
    "matches_event_type",
    "matches_pattern",
    # Subscription
    "Subscription",
    "SubscriptionState",
    "SubscriptionStatus",
    "is_valid_transition",
    "VALID_TRANSITIONS",
    "EventHandler",
    "BatchHandler",
    "PauseReason",
    # Flow Control
    "FlowController",
    "FlowControlContext",
    "FlowControlStats",
    # Subscriber Protocols
    "Subscriber",
    "SyncSubscriber",
    "BatchSubscriber",
    # Subscriber Base Classes
    "BaseSubscriber",
    "BatchAwareSubscriber",
    "FilteringSubscriber",
    # Subscriber Utilities
    "supports_batch_handling",
    "get_subscribed_event_types",
    # Transition
    "TransitionCoordinator",
    "TransitionPhase",
    "TransitionResult",
    "StartFromResolver",
    # Shutdown
    "ShutdownCoordinator",
    "ShutdownPhase",
    "ShutdownResult",
    "ShutdownMetricsSnapshot",
    # Shutdown Metrics
    "SHUTDOWN_OTEL_METRICS_AVAILABLE",
    "record_shutdown_initiated",
    "record_shutdown_completed",
    "record_drain_duration",
    "record_events_drained",
    "record_in_flight_at_shutdown",
    "get_in_flight_at_shutdown",
    "reset_shutdown_metrics",
    # Retry and Circuit Breaker
    "RetryConfig",
    "RetryStats",
    "RetryError",
    "RetryableOperation",
    "CircuitBreaker",
    "CircuitBreakerConfig",
    "CircuitBreakerOpenError",
    "CircuitState",
    "TRANSIENT_EXCEPTIONS",
    "calculate_backoff",
    "retry_async",
    "is_retryable_exception",
    # Exceptions
    "SubscriptionError",
    "SubscriptionConfigError",
    "SubscriptionStateError",
    "SubscriptionAlreadyExistsError",
    "CheckpointNotFoundError",
    "EventStoreConnectionError",
    "EventBusConnectionError",
    "TransitionError",
    # Error Handling
    "ErrorCategory",
    "ErrorSeverity",
    "ErrorClassification",
    "ErrorClassifier",
    "ErrorInfo",
    "ErrorStats",
    "ErrorCallback",
    "SyncErrorCallback",
    "ErrorHandlerRegistry",
    "ErrorHandlingStrategy",
    "ErrorHandlingConfig",
    "SubscriptionErrorHandler",
    "get_default_classifier",
    "RecentErrorInfo",
    # Health Checks
    "HealthStatus",
    "HealthIndicator",
    "HealthCheckResult",
    "HealthCheckConfig",
    "SubscriptionHealthChecker",
    "ManagerHealthChecker",
    # Health Check API (PHASE3-003)
    "ManagerHealth",
    "SubscriptionHealth",
    "ReadinessStatus",
    "LivenessStatus",
    # Metrics (PHASE3-004)
    "OTEL_METRICS_AVAILABLE",
    "SubscriptionMetrics",
    "MetricSnapshot",
    "StateValue",
    "STATE_MAPPING",
    "NoOpCounter",
    "NoOpHistogram",
    "NoOpGauge",
    "get_metrics",
    "clear_metrics_registry",
    "reset_meter",
    # Coordination (P3)
    "LeaderElector",
    "LeaderElectorWithLease",
    "LeaderChangeCallback",
    "InMemoryLeaderElector",
    "SharedLeaderState",
    # Work Redistribution Signals (P3-003)
    "COORDINATION_TOPIC_PREFIX",
    "SHUTDOWN_NOTIFICATIONS_TOPIC",
    "HEARTBEAT_TOPIC",
    "WORK_ASSIGNMENT_TOPIC",
    "ShutdownIntent",
    "ShutdownNotification",
    "HeartbeatMessage",
    "WorkAssignment",
    "PeerShutdownCallback",
    "HeartbeatCallback",
    "WorkAssignmentCallback",
    "PeerTimeoutCallback",
    "PeerInfo",
    "WorkRedistributionCoordinator",
]
