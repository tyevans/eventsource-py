# Subscriptions

Catch-up subscriptions with seamless transition to live event streaming. This package coordinates reading historical events from the event store and transitioning to real-time events from the event bus.

## Key Interfaces

- `Subscription` -- State machine for subscription lifecycle (STARTING → CATCHING_UP → LIVE → PAUSED/STOPPED/ERROR)
- `SubscriptionManager` -- Main API for registering and managing subscriptions
- `Subscriber` / `SyncSubscriber` / `BatchSubscriber` -- Protocols for event subscribers
- `EventFilter` -- Filter events by type, pattern, or custom predicates
- `FlowController` -- Backpressure and flow control
- `RetryConfig` / `CircuitBreaker` -- Retry policies and circuit breaker for transient failures
- `HealthCheckProvider` -- Health monitoring for readiness and liveness probes

## Module Map

- `subscription.py` -- Core `Subscription` class with state machine and position tracking
- `manager.py` -- `SubscriptionManager` (main API) that coordinates all components
- `lifecycle.py` -- `SubscriptionLifecycleManager` for start/stop operations
- `registry.py` -- `SubscriptionRegistry` for subscription CRUD
- `config.py` -- `SubscriptionConfig`, `StartPosition`, `CheckpointStrategy`
- `subscriber.py` -- `Subscriber` protocols and base classes
- `filtering.py` -- `EventFilter` for event type/pattern matching
- `flow_control.py` -- `FlowController` for backpressure and rate limiting
- `retry.py` -- `RetryConfig`, `CircuitBreaker`, exponential backoff
- `transition.py` -- `TransitionCoordinator` for catch-up → live transition
- `pause_resume.py` -- `PauseResumeController` for manual and backpressure pausing
- `shutdown.py` -- `ShutdownCoordinator` for graceful shutdown
- `health.py` / `health_provider.py` -- Health checks (readiness, liveness)
- `coordination.py` -- Leader election and work redistribution (multi-instance)
- `error_handling.py` -- `SubscriptionErrorHandler`, error classification and callbacks
- `metrics.py` -- OpenTelemetry metrics integration
- `exceptions.py` -- All exception types
- `runners/catchup.py` -- `CatchUpRunner` for historical event replay
- `runners/live.py` -- `LiveRunner` for real-time event bus consumption

## Invariants

- **State transitions must be valid**: Use `is_valid_transition()` or `VALID_TRANSITIONS` dict
- **Checkpoint before transition**: Catch-up runner must checkpoint before transitioning to live
- **No duplicate subscriptions**: Registry enforces unique subscription names
- **Graceful shutdown**: Always use `ShutdownCoordinator` to drain in-flight events
- **Tenant filtering respects context**: `DeclarativeProjection` with `tenant_filter` only processes matching events
