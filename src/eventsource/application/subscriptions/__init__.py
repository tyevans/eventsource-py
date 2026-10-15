"""
Subscription management for eventsource.

Provides catch-up subscriptions with seamless transition to live event
streaming, flow control, error classification, health checks, and metrics.
"""

from eventsource.application.subscriptions.config import (
    CheckpointStrategy,
    StartPosition,
    SubscriptionConfig,
    create_catch_up_config,
    create_live_only_config,
)
from eventsource.application.subscriptions.coordination import (
    COORDINATION_TOPIC_PREFIX,
    HEARTBEAT_TOPIC,
    SHUTDOWN_NOTIFICATIONS_TOPIC,
    WORK_ASSIGNMENT_TOPIC,
    HeartbeatCallback,
    HeartbeatMessage,
    PeerInfo,
    PeerShutdownCallback,
    PeerTimeoutCallback,
    ShutdownIntent,
    ShutdownNotification,
    WorkAssignment,
    WorkAssignmentCallback,
    WorkRedistributionCoordinator,
    verify_leadership_lease,
)
from eventsource.application.subscriptions.error_handling import (
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
from eventsource.application.subscriptions.filtering import (
    EventFilter,
    FilterStats,
    matches_event_type,
    matches_pattern,
)
from eventsource.application.subscriptions.flow_control import (
    FlowControlContext,
    FlowController,
    FlowControlStats,
)
from eventsource.application.subscriptions.health import (
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
from eventsource.application.subscriptions.health_provider import HealthCheckProvider
from eventsource.application.subscriptions.lifecycle import SubscriptionLifecycleManager
from eventsource.application.subscriptions.manager import SubscriptionManager
from eventsource.application.subscriptions.metrics import (
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
from eventsource.application.subscriptions.pause_resume import PauseResumeController
from eventsource.application.subscriptions.registry import SubscriptionRegistry
from eventsource.application.subscriptions.retry import (
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
from eventsource.application.subscriptions.shutdown import (
    OTEL_METRICS_AVAILABLE as SHUTDOWN_OTEL_METRICS_AVAILABLE,
)
from eventsource.application.subscriptions.shutdown import (
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
from eventsource.application.subscriptions.subscriber import (
    BaseSubscriber,
    BatchAwareSubscriber,
    BatchSubscriber,
    FilteringSubscriber,
    Subscriber,
    SyncSubscriber,
    get_subscribed_event_types,
    supports_batch_handling,
)
from eventsource.application.subscriptions.subscription import (
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
from eventsource.application.subscriptions.transition import (
    StartFromResolver,
    TransitionCoordinator,
    TransitionPhase,
    TransitionResult,
)
from eventsource.ports.coordination import LeaderChangeCallback, LeaderElector
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

# fmt: off
__all__ = [
    # Manager, Configuration, and Filtering
    "SubscriptionManager", "SubscriptionRegistry", "SubscriptionLifecycleManager",
    "HealthCheckProvider", "PauseResumeController", "SubscriptionConfig",
    "StartPosition", "CheckpointStrategy", "create_catch_up_config",
    "create_live_only_config", "EventFilter", "FilterStats", "matches_event_type",
    "matches_pattern",
    # Subscription, Flow Control, and Subscribers
    "Subscription", "SubscriptionState", "SubscriptionStatus", "is_valid_transition",
    "VALID_TRANSITIONS", "EventHandler", "BatchHandler", "PauseReason",
    "FlowController", "FlowControlContext", "FlowControlStats", "Subscriber",
    "SyncSubscriber", "BatchSubscriber", "BaseSubscriber", "BatchAwareSubscriber",
    "FilteringSubscriber", "supports_batch_handling", "get_subscribed_event_types",
    # Transition and Shutdown
    "TransitionCoordinator", "TransitionPhase", "TransitionResult", "StartFromResolver",
    "ShutdownCoordinator", "ShutdownPhase", "ShutdownResult", "ShutdownMetricsSnapshot",
    "SHUTDOWN_OTEL_METRICS_AVAILABLE", "record_shutdown_initiated",
    "record_shutdown_completed", "record_drain_duration", "record_events_drained",
    "record_in_flight_at_shutdown", "get_in_flight_at_shutdown",
    "reset_shutdown_metrics",
    # Retry, Circuit Breaker, Exceptions, and Error Handling
    "RetryConfig", "RetryStats", "RetryError", "RetryableOperation", "CircuitBreaker",
    "CircuitBreakerConfig", "CircuitBreakerOpenError", "CircuitState",
    "TRANSIENT_EXCEPTIONS", "calculate_backoff", "retry_async",
    "is_retryable_exception", "SubscriptionError", "SubscriptionConfigError",
    "SubscriptionStateError", "SubscriptionAlreadyExistsError",
    "CheckpointNotFoundError", "EventStoreConnectionError", "EventBusConnectionError",
    "TransitionError", "ErrorCategory", "ErrorSeverity", "ErrorClassification",
    "ErrorClassifier", "ErrorInfo", "ErrorStats", "ErrorCallback", "SyncErrorCallback",
    "ErrorHandlerRegistry", "ErrorHandlingStrategy", "ErrorHandlingConfig",
    "SubscriptionErrorHandler", "get_default_classifier", "RecentErrorInfo",
    # Health Checks, Metrics, and Coordination
    "HealthStatus", "HealthIndicator", "HealthCheckResult", "HealthCheckConfig",
    "SubscriptionHealthChecker", "ManagerHealthChecker", "ManagerHealth",
    "SubscriptionHealth", "ReadinessStatus", "LivenessStatus",
    "OTEL_METRICS_AVAILABLE", "SubscriptionMetrics", "MetricSnapshot", "StateValue",
    "STATE_MAPPING", "NoOpCounter", "NoOpHistogram", "NoOpGauge", "get_metrics",
    "clear_metrics_registry", "reset_meter", "LeaderElector", "LeaderChangeCallback",
    "verify_leadership_lease", "COORDINATION_TOPIC_PREFIX",
    "SHUTDOWN_NOTIFICATIONS_TOPIC", "HEARTBEAT_TOPIC", "WORK_ASSIGNMENT_TOPIC",
    "ShutdownIntent", "ShutdownNotification", "HeartbeatMessage", "WorkAssignment",
    "PeerShutdownCallback", "HeartbeatCallback", "WorkAssignmentCallback",
    "PeerTimeoutCallback", "PeerInfo", "WorkRedistributionCoordinator",
]
# fmt: on
