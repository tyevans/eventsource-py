# Subscriptions API Reference

This document provides comprehensive API reference documentation for the subscription management module in eventsource. The subscriptions module provides catch-up subscriptions with seamless transition to live event streaming.

## Table of Contents

- [Module Overview](#module-overview)
- [Quick Start](#quick-start)
- [Core Classes](#core-classes)
  - [SubscriptionManager](#subscriptionmanager)
  - [Subscription](#subscription)
  - [SubscriptionConfig](#subscriptionconfig)
  - [SubscriptionStatus](#subscriptionstatus)
- [Runners](#runners)
  - [CatchUpRunner](#catchuprunner)
  - [LiveRunner](#liverunner)
  - [TransitionCoordinator](#transitioncoordinator)
- [Resilience Components](#resilience-components)
  - [FlowController](#flowcontroller)
  - [ShutdownCoordinator](#shutdowncoordinator)
  - [RetryConfig](#retryconfig)
  - [CircuitBreaker](#circuitbreaker)
  - [Error Handling](#error-handling)
- [Advanced Features](#advanced-features)
  - [EventFilter](#eventfilter)
  - [Subscriber Protocol](#subscriber-protocol)
  - [Health Checking](#health-checking)
  - [Metrics](#metrics)
  - [Pause/Resume](#pauseresume)
- [Enumerations](#enumerations)
- [Exceptions](#exceptions)
- [Type Aliases](#type-aliases)

---

## Module Overview

The subscriptions module (`eventsource.subscriptions`) provides a unified API for:

- Registering subscribers (projections) to receive events
- Coordinating catch-up from the event store (historical events)
- Transitioning seamlessly to live events from the event bus
- Managing subscription lifecycle with health monitoring
- Graceful shutdown with signal handling
- Comprehensive error handling with retry and circuit breaker patterns

### Import

```python
from eventsource.subscriptions import (
    SubscriptionManager,
    SubscriptionConfig,
    Subscription,
    SubscriptionState,
    SubscriptionStatus,
    Subscriber,
)
```

---

## Quick Start

```python
from eventsource.subscriptions import SubscriptionManager, SubscriptionConfig

# Create the manager
manager = SubscriptionManager(
    event_store=event_store,
    event_bus=event_bus,
    checkpoint_repo=checkpoint_repo,
)

# Register a subscriber
await manager.subscribe(my_projection)

# Start processing events
await manager.start()

# ... projection now receives events ...

# Graceful shutdown
await manager.stop()
```

For daemon-style operation with signal handling:

```python
manager = SubscriptionManager(event_store, event_bus, checkpoint_repo)
await manager.subscribe(my_projection)
result = await manager.run_until_shutdown()
# Runs until SIGTERM/SIGINT, then gracefully shuts down
```

---

## Core Classes

### SubscriptionManager

The main entry point for managing catch-up and live event subscriptions.

#### Import

```python
from eventsource.subscriptions import SubscriptionManager
```

#### Constructor

```python
SubscriptionManager(
    event_store: EventStore,
    event_bus: EventBus,
    checkpoint_repo: CheckpointRepository,
    shutdown_timeout: float = 30.0,
    drain_timeout: float = 10.0,
    dlq_repo: DLQRepository | None = None,
    error_handling_config: ErrorHandlingConfig | None = None,
    health_check_config: HealthCheckConfig | None = None,
    enable_tracing: bool = True,
) -> None
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `event_store` | `EventStore` | required | Event store for historical events |
| `event_bus` | `EventBus` | required | Event bus for live events |
| `checkpoint_repo` | `CheckpointRepository` | required | Checkpoint repository for position tracking |
| `shutdown_timeout` | `float` | `30.0` | Total shutdown timeout in seconds |
| `drain_timeout` | `float` | `10.0` | Time to wait for in-flight events during shutdown |
| `dlq_repo` | `DLQRepository \| None` | `None` | Optional DLQ repository for dead letter handling |
| `error_handling_config` | `ErrorHandlingConfig \| None` | `None` | Configuration for error handling behavior |
| `health_check_config` | `HealthCheckConfig \| None` | `None` | Configuration for health check thresholds |
| `enable_tracing` | `bool` | `True` | Enable OpenTelemetry tracing for subscription operations |

#### Methods

##### `subscribe`

Register a subscriber for event delivery.

```python
async def subscribe(
    self,
    subscriber: EventSubscriber,
    config: SubscriptionConfig | None = None,
    name: str | None = None,
) -> Subscription
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `subscriber` | `EventSubscriber` | required | The event subscriber (e.g., projection) to register |
| `config` | `SubscriptionConfig \| None` | `None` | Optional configuration for this subscription |
| `name` | `str \| None` | `None` | Optional custom name (defaults to class name) |

**Returns:** `Subscription` - The created subscription object for monitoring

**Raises:** `SubscriptionAlreadyExistsError` - If a subscription with this name exists

**Example:**

```python
subscription = await manager.subscribe(
    my_projection,
    SubscriptionConfig(start_from="beginning", batch_size=500)
)
```

##### `unsubscribe`

Unregister a subscription by name.

```python
async def unsubscribe(self, name: str) -> bool
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `name` | `str` | The subscription name to remove |

**Returns:** `bool` - True if the subscription was found and removed

##### `start`

Start registered subscriptions.

```python
async def start(
    self,
    subscription_names: list[str] | None = None,
    concurrent: bool = True,
) -> dict[str, Exception | None]
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `subscription_names` | `list[str] \| None` | `None` | Optional list of subscription names to start. If None, starts all. |
| `concurrent` | `bool` | `True` | If True, start subscriptions concurrently |

**Returns:** `dict[str, Exception | None]` - Dictionary mapping subscription names to None (success) or Exception (failure)

**Example:**

```python
results = await manager.start()
failures = {k: v for k, v in results.items() if v is not None}
if failures:
    logger.warning(f"Some subscriptions failed: {failures}")
```

##### `stop`

Stop subscriptions gracefully.

```python
async def stop(
    self,
    timeout: float = 30.0,
    subscription_names: list[str] | None = None,
) -> None
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `timeout` | `float` | `30.0` | Maximum seconds to wait for graceful shutdown |
| `subscription_names` | `list[str] \| None` | `None` | Optional list of subscription names to stop |

##### `run_until_shutdown`

Run the manager until a shutdown signal is received.

```python
async def run_until_shutdown(
    self,
    shutdown_timeout: float | None = None,
) -> ShutdownResult
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `shutdown_timeout` | `float \| None` | `None` | Override the default shutdown timeout |

**Returns:** `ShutdownResult` - Shutdown result with details

**Example:**

```python
result = await manager.run_until_shutdown()
if result.forced:
    logger.warning("Shutdown was forced")
```

##### `health_check`

Get comprehensive health status of the subscription manager.

```python
async def health_check(self) -> ManagerHealth
```

**Returns:** `ManagerHealth` - Complete health information

##### `readiness_check`

Check if the manager is ready to accept work (Kubernetes-style probe).

```python
async def readiness_check(self) -> ReadinessStatus
```

**Returns:** `ReadinessStatus` - Readiness state

##### `liveness_check`

Check if the manager is alive and responsive (Kubernetes-style probe).

```python
async def liveness_check(self) -> LivenessStatus
```

**Returns:** `LivenessStatus` - Liveness state

##### `pause_subscription`

Pause a specific subscription by name.

```python
async def pause_subscription(
    self,
    name: str,
    reason: PauseReason | None = None,
) -> bool
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | `str` | required | The subscription name to pause |
| `reason` | `PauseReason \| None` | `None` | Optional reason for pausing |

**Returns:** `bool` - True if subscription was paused

##### `resume_subscription`

Resume a paused subscription by name.

```python
async def resume_subscription(self, name: str) -> bool
```

**Returns:** `bool` - True if subscription was resumed

##### `pause_all`

Pause all running subscriptions.

```python
async def pause_all(
    self,
    reason: PauseReason | None = None,
) -> dict[str, bool]
```

**Returns:** `dict[str, bool]` - Dictionary mapping subscription names to pause result

##### `resume_all`

Resume all paused subscriptions.

```python
async def resume_all(self) -> dict[str, bool]
```

**Returns:** `dict[str, bool]` - Dictionary mapping subscription names to resume result

##### `on_error`

Register a callback for all error notifications.

```python
def on_error(self, callback: ErrorCallback) -> None
```

**Example:**

```python
async def log_error(error_info: ErrorInfo):
    logger.error(f"Error: {error_info.error_message}")
manager.on_error(log_error)
```

##### `on_error_category`

Register a callback for errors of a specific category.

```python
def on_error_category(
    self,
    category: ErrorCategory,
    callback: ErrorCallback,
) -> None
```

##### `on_error_severity`

Register a callback for errors of a specific severity.

```python
def on_error_severity(
    self,
    severity: ErrorSeverity,
    callback: ErrorCallback,
) -> None
```

#### Properties

| Property | Type | Description |
|----------|------|-------------|
| `subscriptions` | `list[Subscription]` | Get all registered subscriptions |
| `subscription_names` | `list[str]` | Get all registered subscription names |
| `subscription_count` | `int` | Get the number of registered subscriptions |
| `is_running` | `bool` | Check if the manager is running |
| `is_shutting_down` | `bool` | Check if shutdown has been requested |
| `shutdown_phase` | `ShutdownPhase` | Get current shutdown phase |
| `is_healthy` | `bool` | Quick health check |
| `uptime_seconds` | `float` | Time since manager was started |
| `total_errors` | `int` | Total error count across all subscriptions |
| `total_dlq_count` | `int` | Total DLQ count across all subscriptions |
| `paused_subscriptions` | `list[Subscription]` | Get all paused subscriptions |
| `paused_subscription_names` | `list[str]` | Get names of all paused subscriptions |

#### Context Manager

The SubscriptionManager supports async context manager usage:

```python
async with SubscriptionManager(event_store, event_bus, checkpoint_repo) as manager:
    await manager.subscribe(my_projection)
    # ... manager automatically starts and stops
```

---

### Subscription

Represents an active subscription to events. Manages state machine, position tracking, and statistics.

#### Import

```python
from eventsource.subscriptions import Subscription, SubscriptionState
```

#### Constructor

```python
@dataclass
class Subscription:
    name: str
    config: SubscriptionConfig
    subscriber: EventSubscriber
```

**Note:** Subscriptions are typically created via `SubscriptionManager.subscribe()`, not directly.

#### Methods

##### `transition_to`

Transition to a new state.

```python
async def transition_to(self, new_state: SubscriptionState) -> None
```

**Raises:** `SubscriptionStateError` - If the transition is not valid

##### `pause`

Pause event processing.

```python
async def pause(
    self,
    reason: PauseReason = PauseReason.MANUAL,
) -> None
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `reason` | `PauseReason` | `MANUAL` | The reason for pausing |

**Raises:** `SubscriptionStateError` - If not in a pausable state

##### `resume`

Resume event processing.

```python
async def resume(self) -> None
```

**Raises:** `SubscriptionStateError` - If not in PAUSED state

##### `wait_if_paused`

Wait if the subscription is paused.

```python
async def wait_if_paused(self) -> bool
```

**Returns:** `bool` - True if was paused and resumed, False if not paused

##### `get_status`

Get a status snapshot for health checks.

```python
def get_status(self) -> SubscriptionStatus
```

**Returns:** `SubscriptionStatus` - Current state and statistics

##### `record_event_processed`

Record that an event was successfully processed.

```python
async def record_event_processed(
    self,
    position: int,
    event_id: UUID,
    event_type: str,
) -> None
```

##### `record_event_failed`

Record that an event processing failed.

```python
async def record_event_failed(self, error: Exception) -> None
```

##### `set_error`

Set error state with the given exception.

```python
async def set_error(self, error: Exception) -> None
```

#### Properties

| Property | Type | Description |
|----------|------|-------------|
| `state` | `SubscriptionState` | Current subscription state |
| `name` | `str` | Subscription name |
| `last_processed_position` | `int` | Last processed global position |
| `events_processed` | `int` | Total events successfully processed |
| `events_failed` | `int` | Total events that failed processing |
| `lag` | `int` | Events behind the current max position |
| `is_running` | `bool` | True if catching up or live |
| `is_terminal` | `bool` | True if stopped or in error state |
| `is_paused` | `bool` | True if in PAUSED state |
| `pause_reason` | `PauseReason \| None` | Reason for current pause |
| `uptime_seconds` | `float` | Seconds since subscription started |
| `previous_state` | `SubscriptionState \| None` | Previous state before last transition |
| `last_error` | `Exception \| None` | Most recent error |
| `dlq_count` | `int` | Events sent to DLQ |
| `recent_errors` | `list[RecentErrorInfo]` | List of recent errors |

---

### SubscriptionConfig

Configuration for a subscription. Controls how the subscription reads events, manages checkpoints, and handles backpressure.

#### Import

```python
from eventsource.subscriptions import SubscriptionConfig, CheckpointStrategy, StartPosition
```

#### Definition

```python
@dataclass(frozen=True)
class SubscriptionConfig:
    # Starting position
    start_from: StartPosition = "checkpoint"

    # Batch processing settings
    batch_size: int = 100
    max_in_flight: int = 1000

    # Backpressure settings
    backpressure_threshold: float = 0.8

    # Checkpoint behavior
    checkpoint_strategy: CheckpointStrategy = CheckpointStrategy.EVERY_BATCH
    checkpoint_interval_seconds: float = 5.0

    # Timeouts
    processing_timeout: float = 30.0
    shutdown_timeout: float = 30.0

    # Filtering (optional)
    event_types: tuple[type[DomainEvent], ...] | None = None
    aggregate_types: tuple[str, ...] | None = None

    # Error handling
    continue_on_error: bool = True

    # Retry settings
    max_retries: int = 5
    initial_retry_delay: float = 1.0
    max_retry_delay: float = 60.0
    retry_exponential_base: float = 2.0
    retry_jitter: float = 0.1

    # Circuit breaker settings
    circuit_breaker_enabled: bool = True
    circuit_breaker_failure_threshold: int = 5
    circuit_breaker_recovery_timeout: float = 30.0
```

**Fields:**

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `start_from` | `StartPosition` | `"checkpoint"` | Where to start reading events |
| `batch_size` | `int` | `100` | Number of events to read in each batch |
| `max_in_flight` | `int` | `1000` | Maximum events being processed concurrently |
| `backpressure_threshold` | `float` | `0.8` | Fraction (0-1) at which to signal backpressure |
| `checkpoint_strategy` | `CheckpointStrategy` | `EVERY_BATCH` | When to persist checkpoint updates |
| `checkpoint_interval_seconds` | `float` | `5.0` | Interval for PERIODIC strategy |
| `processing_timeout` | `float` | `30.0` | Max seconds to wait for event processing |
| `shutdown_timeout` | `float` | `30.0` | Max seconds to wait during graceful shutdown |
| `event_types` | `tuple[type, ...] \| None` | `None` | Event types to filter |
| `aggregate_types` | `tuple[str, ...] \| None` | `None` | Aggregate types to filter |
| `continue_on_error` | `bool` | `True` | Whether to continue after DLQ'd events |
| `max_retries` | `int` | `5` | Maximum retry attempts |
| `initial_retry_delay` | `float` | `1.0` | Initial delay before first retry |
| `max_retry_delay` | `float` | `60.0` | Maximum delay between retries |
| `retry_exponential_base` | `float` | `2.0` | Base for exponential backoff |
| `retry_jitter` | `float` | `0.1` | Fraction of delay to add as jitter |
| `circuit_breaker_enabled` | `bool` | `True` | Enable circuit breaker protection |
| `circuit_breaker_failure_threshold` | `int` | `5` | Failures before opening circuit |
| `circuit_breaker_recovery_timeout` | `float` | `30.0` | Seconds before testing recovery |

**StartPosition Values:**

- `"beginning"`: Start from global position 0
- `"end"`: Start from current end (live-only)
- `"checkpoint"`: Resume from last checkpoint (default)
- `int`: Start from specific global position

**Example:**

```python
config = SubscriptionConfig(
    start_from="beginning",
    batch_size=500,
    max_in_flight=2000,
    backpressure_threshold=0.8,
    checkpoint_strategy=CheckpointStrategy.EVERY_BATCH,
)
```

#### Factory Functions

##### `create_catch_up_config`

Create a configuration optimized for catch-up scenarios.

```python
def create_catch_up_config(
    batch_size: int = 1000,
    checkpoint_every_batch: bool = True,
) -> SubscriptionConfig
```

##### `create_live_only_config`

Create a configuration for live-only subscriptions.

```python
def create_live_only_config() -> SubscriptionConfig
```

---

### SubscriptionStatus

Status snapshot for health checks and monitoring.

#### Import

```python
from eventsource.subscriptions import SubscriptionStatus
```

#### Definition

```python
@dataclass(frozen=True)
class SubscriptionStatus:
    name: str
    state: str
    position: int
    lag_events: int
    events_processed: int
    events_failed: int
    last_processed_at: str | None
    started_at: str | None
    uptime_seconds: float
    error: str | None = None
    events_dlq: int = 0
    recent_errors_count: int = 0
```

**Fields:**

| Field | Type | Description |
|-------|------|-------------|
| `name` | `str` | Subscription name |
| `state` | `str` | Current state as string |
| `position` | `int` | Last processed global position |
| `lag_events` | `int` | Number of events behind |
| `events_processed` | `int` | Total events successfully processed |
| `events_failed` | `int` | Total events that failed processing |
| `events_dlq` | `int` | Total events sent to dead letter queue |
| `last_processed_at` | `str \| None` | ISO timestamp of last processed event |
| `started_at` | `str \| None` | ISO timestamp when subscription started |
| `uptime_seconds` | `float` | Time since subscription started |
| `error` | `str \| None` | Error message if in error state |
| `recent_errors_count` | `int` | Number of recent errors in buffer |

#### Methods

##### `to_dict`

Convert to dictionary for JSON serialization.

```python
def to_dict(self) -> dict[str, Any]
```

---

## Runners

### CatchUpRunner

Reads historical events from the event store and delivers them to the subscriber.

#### Import

```python
from eventsource.subscriptions.runners import CatchUpRunner, CatchUpResult
```

#### Constructor

```python
CatchUpRunner(
    event_store: EventStore,
    checkpoint_repo: CheckpointRepository,
    subscription: Subscription,
    event_filter: EventFilter | None = None,
    enable_metrics: bool = True,
    enable_tracing: bool = True,
) -> None
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `event_store` | `EventStore` | required | Event store to read from |
| `checkpoint_repo` | `CheckpointRepository` | required | Checkpoint repository |
| `subscription` | `Subscription` | required | The subscription being processed |
| `event_filter` | `EventFilter \| None` | `None` | Optional event filter |
| `enable_metrics` | `bool` | `True` | Enable OpenTelemetry metrics |
| `enable_tracing` | `bool` | `True` | Enable OpenTelemetry tracing |

#### Methods

##### `run_until_position`

Run catch-up until reaching the target position.

```python
async def run_until_position(
    self,
    target_position: int,
) -> CatchUpResult
```

**Returns:** `CatchUpResult` - Processing statistics

##### `stop`

Request the runner to stop gracefully.

```python
async def stop(self) -> None
```

#### Properties

| Property | Type | Description |
|----------|------|-------------|
| `is_running` | `bool` | Check if runner is currently processing |
| `stop_requested` | `bool` | Check if stop has been requested |
| `flow_controller` | `FlowController` | Get the flow controller |
| `circuit_breaker` | `CircuitBreaker \| None` | Get the circuit breaker |
| `event_filter` | `EventFilter` | Get the event filter |
| `metrics` | `SubscriptionMetrics` | Get the metrics instance |

#### CatchUpResult

```python
@dataclass
class CatchUpResult:
    events_processed: int
    final_position: int
    completed: bool
    error: Exception | None = None

    @property
    def success(self) -> bool: ...
```

---

### LiveRunner

Receives real-time events from the event bus and delivers them to the subscriber.

#### Import

```python
from eventsource.subscriptions.runners import LiveRunner, LiveRunnerStats
```

#### Constructor

```python
@dataclass
class LiveRunner:
    event_bus: EventBus
    checkpoint_repo: CheckpointRepository
    subscription: Subscription
    enable_metrics: bool = True
    enable_tracing: bool = True
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `event_bus` | `EventBus` | required | Event bus to subscribe to |
| `checkpoint_repo` | `CheckpointRepository` | required | Checkpoint repository |
| `subscription` | `Subscription` | required | The subscription being processed |
| `enable_metrics` | `bool` | `True` | Enable OpenTelemetry metrics |
| `enable_tracing` | `bool` | `True` | Enable OpenTelemetry tracing |

#### Methods

##### `start`

Start receiving live events.

```python
async def start(self, buffer_events: bool = False) -> None
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `buffer_events` | `bool` | `False` | If True, buffer events instead of processing immediately |

##### `stop`

Stop the live runner.

```python
async def stop(self) -> None
```

##### `process_buffer`

Process all buffered events.

```python
async def process_buffer(self) -> int
```

**Returns:** `int` - Number of events processed from buffer

##### `disable_buffer`

Disable buffering and switch to direct processing.

```python
async def disable_buffer(self) -> None
```

##### `process_pause_buffer`

Process all events buffered during pause.

```python
async def process_pause_buffer(self) -> int
```

**Returns:** `int` - Number of events processed from pause buffer

#### Properties

| Property | Type | Description |
|----------|------|-------------|
| `is_running` | `bool` | Check if runner is active |
| `buffer_size` | `int` | Get current buffer size |
| `pause_buffer_size` | `int` | Get current pause buffer size |
| `stats` | `LiveRunnerStats` | Get processing statistics |
| `flow_controller` | `FlowController` | Get the flow controller |
| `circuit_breaker` | `CircuitBreaker \| None` | Get the circuit breaker |
| `event_filter` | `EventFilter` | Get the event filter |
| `metrics` | `SubscriptionMetrics` | Get the metrics instance |

#### LiveRunnerStats

```python
@dataclass
class LiveRunnerStats:
    events_received: int = 0
    events_processed: int = 0
    events_skipped_duplicate: int = 0
    events_skipped_filtered: int = 0
    events_failed: int = 0
```

---

### TransitionCoordinator

Coordinates the transition from catch-up to live event processing.

#### Import

```python
from eventsource.subscriptions import (
    TransitionCoordinator,
    TransitionPhase,
    TransitionResult,
)
```

#### Constructor

```python
TransitionCoordinator(
    event_store: EventStore,
    event_bus: EventBus,
    checkpoint_repo: CheckpointRepository,
    subscription: Subscription,
    enable_tracing: bool = True,
) -> None
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `event_store` | `EventStore` | required | Event store for catch-up and position lookup |
| `event_bus` | `EventBus` | required | Event bus for live subscription |
| `checkpoint_repo` | `CheckpointRepository` | required | Checkpoint repository |
| `subscription` | `Subscription` | required | The subscription being transitioned |
| `enable_tracing` | `bool` | `True` | Enable OpenTelemetry tracing |

#### Methods

##### `execute`

Execute the catch-up to live transition.

```python
async def execute(self) -> TransitionResult
```

**Returns:** `TransitionResult` - Transition statistics and outcome

##### `stop`

Stop the transition and all runners.

```python
async def stop(self) -> None
```

#### Properties

| Property | Type | Description |
|----------|------|-------------|
| `phase` | `TransitionPhase` | Current transition phase |
| `watermark` | `int` | Watermark position |
| `live_runner` | `LiveRunner \| None` | Live runner (after transition) |
| `catchup_runner` | `CatchUpRunner \| None` | Catch-up runner (during catch-up) |

#### TransitionPhase

```python
class TransitionPhase(Enum):
    NOT_STARTED = "not_started"
    INITIAL_CATCHUP = "initial_catchup"
    LIVE_SUBSCRIBED = "live_subscribed"
    FINAL_CATCHUP = "final_catchup"
    PROCESSING_BUFFER = "processing_buffer"
    LIVE = "live"
    FAILED = "failed"
```

#### TransitionResult

```python
@dataclass(frozen=True)
class TransitionResult:
    success: bool
    catchup_events_processed: int
    buffer_events_processed: int
    buffer_events_skipped: int
    final_position: int
    phase_reached: TransitionPhase
    error: Exception | None = None
```

---

## Resilience Components

### FlowController

Controls event processing rate using backpressure.

#### Import

```python
from eventsource.subscriptions import FlowController, FlowControlStats
```

#### Constructor

```python
FlowController(
    max_in_flight: int = 1000,
    backpressure_threshold: float = 0.8,
) -> None
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `max_in_flight` | `int` | `1000` | Maximum concurrent events allowed |
| `backpressure_threshold` | `float` | `0.8` | Fraction (0-1) at which to signal backpressure |

#### Methods

##### `acquire`

Acquire a slot for processing.

```python
async def acquire(self) -> FlowControlContext
```

**Returns:** `FlowControlContext` - Context manager that releases slot on exit

**Example:**

```python
controller = FlowController(max_in_flight=1000)
async with await controller.acquire():
    await process_event(event)
# Slot automatically released
```

##### `release`

Release a processing slot.

```python
async def release(self) -> None
```

##### `wait_for_capacity`

Wait until at least min_capacity slots are available.

```python
async def wait_for_capacity(self, min_capacity: int = 1) -> None
```

#### Properties

| Property | Type | Description |
|----------|------|-------------|
| `is_paused` | `bool` | Check if processing is paused |
| `is_backpressured` | `bool` | Check if experiencing backpressure |
| `in_flight` | `int` | Current in-flight event count |
| `available_capacity` | `int` | Available capacity for new events |
| `utilization` | `float` | Current utilization (0.0 to 1.0) |
| `stats` | `FlowControlStats` | Flow control statistics |

#### FlowControlStats

```python
@dataclass
class FlowControlStats:
    events_in_flight: int = 0
    peak_in_flight: int = 0
    pause_count: int = 0
    total_pause_time_seconds: float = 0.0
    total_acquisitions: int = 0
    total_releases: int = 0
```

---

### ShutdownCoordinator

Coordinates graceful shutdown of subscriptions.

#### Import

```python
from eventsource.subscriptions import (
    ShutdownCoordinator,
    ShutdownPhase,
    ShutdownResult,
)
```

#### Constructor

```python
@dataclass
class ShutdownCoordinator:
    timeout: float = 30.0
    drain_timeout: float = 10.0
    checkpoint_timeout: float = 5.0
```

#### Methods

##### `register_signals`

Register signal handlers for graceful shutdown (SIGTERM, SIGINT).

```python
def register_signals(
    self, loop: asyncio.AbstractEventLoop | None = None
) -> None
```

##### `unregister_signals`

Unregister signal handlers.

```python
def unregister_signals(
    self, loop: asyncio.AbstractEventLoop | None = None
) -> None
```

##### `wait_for_shutdown`

Wait for shutdown signal.

```python
async def wait_for_shutdown(self) -> None
```

##### `request_shutdown`

Programmatically request shutdown.

```python
def request_shutdown(self) -> None
```

##### `on_shutdown`

Register a callback to be invoked on shutdown signal.

```python
def on_shutdown(self, callback: Callable[[], Awaitable[None]]) -> None
```

##### `shutdown`

Execute graceful shutdown sequence.

```python
async def shutdown(
    self,
    stop_func: Callable[[], Awaitable[None]],
    drain_func: Callable[[], Awaitable[int]] | None = None,
    checkpoint_func: Callable[[], Awaitable[int]] | None = None,
    close_func: Callable[[], Awaitable[None]] | None = None,
) -> ShutdownResult
```

#### Properties

| Property | Type | Description |
|----------|------|-------------|
| `is_shutting_down` | `bool` | Check if shutdown has been requested |
| `phase` | `ShutdownPhase` | Current shutdown phase |
| `is_forced` | `bool` | Check if shutdown was forced |

#### ShutdownPhase

```python
class ShutdownPhase(Enum):
    RUNNING = "running"
    STOPPING = "stopping"
    DRAINING = "draining"
    CHECKPOINTING = "checkpointing"
    STOPPED = "stopped"
    FORCED = "forced"
```

#### ShutdownResult

```python
@dataclass(frozen=True)
class ShutdownResult:
    phase: ShutdownPhase
    duration_seconds: float
    subscriptions_stopped: int
    events_drained: int
    checkpoints_saved: int = 0
    forced: bool = False
    error: str | None = None
```

---

### RetryConfig

Configuration for retry behavior with exponential backoff.

#### Import

```python
from eventsource.subscriptions import RetryConfig, RetryStats, RetryError
```

#### Definition

```python
@dataclass
class RetryConfig:
    max_retries: int = 5
    initial_delay: float = 1.0
    max_delay: float = 60.0
    exponential_base: float = 2.0
    jitter: float = 0.1
```

**Fields:**

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `max_retries` | `int` | `5` | Maximum number of retry attempts (0 = no retries) |
| `initial_delay` | `float` | `1.0` | Initial delay in seconds before first retry |
| `max_delay` | `float` | `60.0` | Maximum delay in seconds between retries |
| `exponential_base` | `float` | `2.0` | Base for exponential backoff calculation |
| `jitter` | `float` | `0.1` | Fraction of delay to add as random jitter (0-1) |

#### Functions

##### `calculate_backoff`

Calculate backoff delay with exponential growth and jitter.

```python
def calculate_backoff(
    attempt: int,
    config: RetryConfig,
) -> float
```

##### `retry_async`

Retry an async operation with exponential backoff.

```python
async def retry_async(
    operation: Callable[[], Awaitable[T]],
    config: RetryConfig | None = None,
    retryable_exceptions: tuple[type[Exception], ...] = TRANSIENT_EXCEPTIONS,
    operation_name: str = "operation",
) -> T
```

**Example:**

```python
async def fetch_data():
    return await http_client.get("/data")

data = await retry_async(fetch_data, operation_name="fetch_data")
```

#### RetryStats

```python
@dataclass
class RetryStats:
    attempts: int = 0
    successes: int = 0
    failures: int = 0
    total_delay_seconds: float = 0.0
    last_error: str | None = None
```

---

### CircuitBreaker

Circuit breaker pattern for preventing cascading failures.

#### Import

```python
from eventsource.subscriptions import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitBreakerOpenError,
    CircuitState,
)
```

#### Constructor

```python
CircuitBreaker(config: CircuitBreakerConfig | None = None) -> None
```

#### CircuitBreakerConfig

```python
@dataclass
class CircuitBreakerConfig:
    failure_threshold: int = 5
    recovery_timeout: float = 30.0
    half_open_max_calls: int = 1
```

#### Methods

##### `execute`

Execute an operation through the circuit breaker.

```python
async def execute(
    self,
    operation: Callable[[], Awaitable[T]],
    operation_name: str = "operation",
) -> T
```

**Raises:** `CircuitBreakerOpenError` - If circuit is open

##### `record_success`

Record a successful operation.

```python
async def record_success(self) -> None
```

##### `record_failure`

Record a failed operation.

```python
async def record_failure(self) -> None
```

##### `reset`

Reset the circuit breaker to closed state.

```python
def reset(self) -> None
```

#### Properties

| Property | Type | Description |
|----------|------|-------------|
| `state` | `CircuitState` | Current circuit state |
| `failure_count` | `int` | Current failure count |
| `is_closed` | `bool` | Check if circuit is closed |
| `is_open` | `bool` | Check if circuit is open |
| `is_half_open` | `bool` | Check if circuit is half-open |

#### CircuitState

```python
class CircuitState(Enum):
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Blocking requests
    HALF_OPEN = "half_open"  # Testing recovery
```

---

### Error Handling

#### ErrorHandlerRegistry

Registry for error callbacks and handlers.

```python
from eventsource.subscriptions import (
    ErrorHandlerRegistry,
    ErrorCategory,
    ErrorSeverity,
    ErrorClassification,
    ErrorInfo,
    ErrorStats,
)
```

#### SubscriptionErrorHandler

Unified error handler for subscription event processing.

```python
from eventsource.subscriptions import (
    SubscriptionErrorHandler,
    ErrorHandlingConfig,
    ErrorHandlingStrategy,
)
```

##### Constructor

```python
SubscriptionErrorHandler(
    subscription_name: str,
    config: ErrorHandlingConfig | None = None,
    dlq_repo: DLQRepository | None = None,
    classifier: ErrorClassifier | None = None,
) -> None
```

##### Methods

```python
def on_error(self, callback: ErrorCallback) -> None
def on_error_sync(self, callback: SyncErrorCallback) -> None
def on_category(self, category: ErrorCategory, callback: ErrorCallback) -> None
def on_severity(self, severity: ErrorSeverity, callback: ErrorCallback) -> None
async def handle_error(
    self,
    error: Exception,
    stored_event: StoredEvent | None = None,
    event: DomainEvent | None = None,
    retry_count: int = 0,
) -> ErrorInfo
def should_continue(self) -> bool
def should_retry(self, error: Exception) -> bool
def get_health_status(self) -> dict[str, Any]
async def clear_stats(self) -> None
```

##### Properties

| Property | Type | Description |
|----------|------|-------------|
| `stats` | `ErrorStats` | Get error statistics |
| `recent_errors` | `list[ErrorInfo]` | Get list of recent errors |
| `dlq_count` | `int` | Get count of events sent to DLQ |
| `total_errors` | `int` | Get total error count |

#### ErrorCategory

```python
class ErrorCategory(Enum):
    TRANSIENT = "transient"        # Temporary failures
    PERMANENT = "permanent"        # Won't succeed on retry
    INFRASTRUCTURE = "infrastructure"  # Database down, etc.
    APPLICATION = "application"    # Business logic failures
    UNKNOWN = "unknown"           # Fallback
```

#### ErrorSeverity

```python
class ErrorSeverity(Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"
```

#### ErrorHandlingStrategy

```python
class ErrorHandlingStrategy(Enum):
    STOP = "stop"                    # Stop on first error
    CONTINUE = "continue"            # Log and continue
    RETRY_THEN_CONTINUE = "retry_then_continue"  # Retry, then continue
    RETRY_THEN_DLQ = "retry_then_dlq"  # Retry, then send to DLQ
    DLQ_ONLY = "dlq_only"            # Send to DLQ immediately
```

#### ErrorHandlingConfig

```python
@dataclass(frozen=True)
class ErrorHandlingConfig:
    strategy: ErrorHandlingStrategy = ErrorHandlingStrategy.RETRY_THEN_CONTINUE
    max_recent_errors: int = 100
    max_errors_before_stop: int | None = None
    error_rate_threshold: float | None = None
    dlq_enabled: bool = True
    notify_on_severity: ErrorSeverity = ErrorSeverity.HIGH
```

---

## Advanced Features

### EventFilter

Filters events based on type and aggregate criteria.

#### Import

```python
from eventsource.subscriptions import EventFilter, FilterStats
```

#### Constructor

```python
@dataclass
class EventFilter:
    event_types: tuple[type[DomainEvent], ...] | None = None
    event_type_patterns: tuple[str, ...] | None = None
    aggregate_types: tuple[str, ...] | None = None
```

#### Factory Methods

##### `from_config`

Create filter from subscription configuration.

```python
@classmethod
def from_config(cls, config: SubscriptionConfig) -> EventFilter
```

##### `from_subscriber`

Create filter from subscriber's `subscribed_to()` method.

```python
@classmethod
def from_subscriber(cls, subscriber: Subscriber) -> EventFilter
```

##### `from_config_and_subscriber`

Create filter from config, falling back to subscriber.

```python
@classmethod
def from_config_and_subscriber(
    cls,
    config: SubscriptionConfig,
    subscriber: Subscriber,
) -> EventFilter
```

##### `from_patterns`

Create filter from event type name patterns.

```python
@classmethod
def from_patterns(cls, *patterns: str) -> EventFilter
```

**Example:**

```python
filter = EventFilter.from_patterns("Order*", "Payment*")
```

#### Methods

##### `matches`

Check if an event matches all filter criteria.

```python
def matches(self, event: DomainEvent) -> bool
```

#### Properties

| Property | Type | Description |
|----------|------|-------------|
| `is_configured` | `bool` | Check if any filter criteria is set |
| `stats` | `FilterStats` | Get filter statistics |
| `event_type_names` | `list[str] \| None` | Get event type names for query optimization |

#### FilterStats

```python
@dataclass
class FilterStats:
    events_evaluated: int = 0
    events_matched: int = 0
    events_skipped: int = 0

    @property
    def match_rate(self) -> float: ...
```

---

### Subscriber Protocol

The Subscriber protocol defines the interface for event subscribers.

#### Import

```python
from eventsource.subscriptions import (
    Subscriber,
    SyncSubscriber,
    BatchSubscriber,
    BaseSubscriber,
    BatchAwareSubscriber,
    FilteringSubscriber,
)
```

#### Subscriber Protocol

```python
@runtime_checkable
class Subscriber(Protocol):
    def subscribed_to(self) -> list[type[DomainEvent]]:
        """Return list of event types this subscriber handles."""
        ...

    async def handle(self, event: DomainEvent) -> None:
        """Handle a single domain event asynchronously."""
        ...
```

**Example:**

```python
class OrderProjection:
    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [OrderCreated, OrderShipped]

    async def handle(self, event: DomainEvent) -> None:
        if isinstance(event, OrderCreated):
            await self._handle_created(event)

# Runtime check
assert isinstance(OrderProjection(), Subscriber)
```

#### SyncSubscriber Protocol

For synchronous event subscribers.

```python
@runtime_checkable
class SyncSubscriber(Protocol):
    def subscribed_to(self) -> list[type[DomainEvent]]: ...
    def handle(self, event: DomainEvent) -> None: ...
```

#### BatchSubscriber Protocol

For subscribers supporting batch event processing.

```python
@runtime_checkable
class BatchSubscriber(Protocol):
    def subscribed_to(self) -> list[type[DomainEvent]]: ...
    async def handle_batch(self, events: Sequence[DomainEvent]) -> None: ...
```

#### BaseSubscriber

Abstract base class for event subscribers.

```python
class BaseSubscriber(ABC):
    @abstractmethod
    def subscribed_to(self) -> list[type[DomainEvent]]: ...

    @abstractmethod
    async def handle(self, event: DomainEvent) -> None: ...

    def can_handle(self, event: DomainEvent) -> bool: ...
```

#### BatchAwareSubscriber

Base class with batch processing support.

```python
class BatchAwareSubscriber(BaseSubscriber):
    async def handle_batch(self, events: Sequence[DomainEvent]) -> None: ...

    async def handle_batch_with_error_tracking(
        self,
        events: Sequence[DomainEvent],
    ) -> tuple[int, list[tuple[DomainEvent, Exception]]]: ...
```

#### FilteringSubscriber

Subscriber with built-in event filtering capabilities.

```python
class FilteringSubscriber(BatchAwareSubscriber):
    def should_handle(self, event: DomainEvent) -> bool: ...

    @abstractmethod
    async def _process_event(self, event: DomainEvent) -> None: ...
```

#### Utility Functions

##### `supports_batch_handling`

Check if a subscriber supports batch event handling.

```python
def supports_batch_handling(subscriber: object) -> bool
```

##### `get_subscribed_event_types`

Get the event types a subscriber handles.

```python
def get_subscribed_event_types(subscriber: Subscriber) -> list[type[DomainEvent]]
```

---

### Health Checking

#### ManagerHealth

Health status of the subscription manager.

```python
from eventsource.subscriptions import (
    ManagerHealth,
    SubscriptionHealth,
    ReadinessStatus,
    LivenessStatus,
)
```

#### ManagerHealth

```python
@dataclass
class ManagerHealth:
    status: str                    # "healthy", "degraded", "unhealthy", "critical"
    running: bool
    subscription_count: int
    healthy_count: int
    degraded_count: int
    unhealthy_count: int
    total_events_processed: int
    total_events_failed: int
    total_lag_events: int
    uptime_seconds: float
    timestamp: datetime
    subscriptions: list[SubscriptionHealth]

    def to_dict(self) -> dict[str, Any]: ...
```

#### SubscriptionHealth

```python
@dataclass
class SubscriptionHealth:
    name: str
    status: str
    state: str
    events_processed: int
    events_failed: int
    lag_events: int
    error_rate: float
    last_error: str | None
    uptime_seconds: float

    def to_dict(self) -> dict[str, Any]: ...
```

#### ReadinessStatus

Kubernetes-style readiness probe.

```python
@dataclass
class ReadinessStatus:
    ready: bool
    reason: str
    details: dict[str, Any]
    timestamp: datetime

    def to_dict(self) -> dict[str, Any]: ...
```

#### LivenessStatus

Kubernetes-style liveness probe.

```python
@dataclass
class LivenessStatus:
    alive: bool
    reason: str
    details: dict[str, Any]
    timestamp: datetime

    def to_dict(self) -> dict[str, Any]: ...
```

#### HealthCheckConfig

```python
@dataclass
class HealthCheckConfig:
    max_error_rate_per_minute: float = 10.0
    max_errors_warning: int = 10
    max_errors_critical: int = 100
    max_lag_events_warning: int = 1000
    max_lag_events_critical: int = 10000
    backpressure_warning_duration_seconds: float = 60.0
    backpressure_critical_duration_seconds: float = 300.0
    circuit_open_is_unhealthy: bool = True
    max_dlq_events_warning: int = 10
    max_dlq_events_critical: int = 100
```

---

### Metrics

OpenTelemetry metrics for subscription management.

#### Import

```python
from eventsource.subscriptions import (
    SubscriptionMetrics,
    MetricSnapshot,
    get_metrics,
    clear_metrics_registry,
)
```

#### SubscriptionMetrics

```python
@dataclass
class SubscriptionMetrics:
    subscription_name: str
    enable_metrics: bool = True
```

**Metrics Exposed:**

| Metric | Type | Description |
|--------|------|-------------|
| `subscription.events.processed` | Counter | Total events processed |
| `subscription.events.failed` | Counter | Total events failed |
| `subscription.processing.duration` | Histogram | Processing time in milliseconds |
| `subscription.lag` | Gauge | Current event lag |
| `subscription.state` | Gauge | Current subscription state (numeric) |

##### Methods

```python
def record_event_processed(
    self,
    event_type: str,
    duration_ms: float,
    status: str = "success",
) -> None

def record_event_failed(
    self,
    event_type: str,
    error_type: str,
    duration_ms: float | None = None,
) -> None

def record_lag(self, lag_events: int) -> None

def record_state(self, state: str) -> None

@contextmanager
def time_processing(self): ...

def get_snapshot(self) -> MetricSnapshot
```

**Example:**

```python
metrics = SubscriptionMetrics("OrderProjection")

# Record a successful event
start = time.perf_counter()
process_event(event)
duration_ms = (time.perf_counter() - start) * 1000
metrics.record_event_processed("OrderCreated", duration_ms)

# Using context manager
with metrics.time_processing() as timer:
    process_event(event)
metrics.record_event_processed("OrderCreated", timer.duration_ms)
```

#### MetricSnapshot

```python
@dataclass
class MetricSnapshot:
    events_processed: int = 0
    events_failed: int = 0
    total_processing_time_ms: float = 0.0
    current_lag: int = 0
    current_state: int = 0
    current_state_name: str = "unknown"

    def to_dict(self) -> dict[str, Any]: ...
```

---

### Pause/Resume

#### PauseReason

```python
class PauseReason(Enum):
    MANUAL = "manual"          # User-initiated pause
    BACKPRESSURE = "backpressure"  # Automatic pause due to flow control
    MAINTENANCE = "maintenance"    # Pause for maintenance
```

**Example:**

```python
# Pause a subscription
await manager.pause_subscription("OrderProjection", reason=PauseReason.MAINTENANCE)

# ... do maintenance ...

# Resume
await manager.resume_subscription("OrderProjection")

# Or pause/resume all
await manager.pause_all()
await manager.resume_all()
```

---

## Enumerations

### SubscriptionState

```python
class SubscriptionState(Enum):
    STARTING = "starting"      # Initial state while reading checkpoint
    CATCHING_UP = "catching_up"  # Reading historical events
    LIVE = "live"              # Receiving real-time events
    PAUSED = "paused"          # Temporarily paused
    STOPPED = "stopped"        # Cleanly shut down
    ERROR = "error"            # Failed with unrecoverable error
```

**State Transitions:**

```
STARTING -> CATCHING_UP | LIVE | STOPPED | ERROR
CATCHING_UP -> LIVE | PAUSED | STOPPED | ERROR
LIVE -> CATCHING_UP | PAUSED | STOPPED | ERROR
PAUSED -> CATCHING_UP | LIVE | STOPPED | ERROR
STOPPED -> (terminal)
ERROR -> STARTING (restart)
```

### CheckpointStrategy

```python
class CheckpointStrategy(Enum):
    EVERY_EVENT = "every_event"    # Safest, slowest
    EVERY_BATCH = "every_batch"    # Default, balanced
    PERIODIC = "periodic"          # Fastest, riskiest
```

---

## Exceptions

### SubscriptionError

Base exception for subscription-related errors.

```python
class SubscriptionError(Exception):
    pass
```

### SubscriptionConfigError

Raised when subscription configuration is invalid.

```python
class SubscriptionConfigError(SubscriptionError):
    pass
```

### SubscriptionStateError

Raised when an operation is invalid for the current state.

```python
class SubscriptionStateError(SubscriptionError):
    pass
```

### SubscriptionAlreadyExistsError

Raised when trying to register a duplicate subscription.

```python
class SubscriptionAlreadyExistsError(SubscriptionError):
    def __init__(self, name: str) -> None:
        self.name = name
```

### CheckpointNotFoundError

Raised when checkpoint is required but not found.

```python
class CheckpointNotFoundError(SubscriptionError):
    def __init__(self, projection_name: str) -> None:
        self.projection_name = projection_name
```

### TransitionError

Raised when catch-up to live transition fails.

```python
class TransitionError(SubscriptionError):
    pass
```

### RetryError

Raised when all retry attempts fail.

```python
class RetryError(Exception):
    def __init__(self, message: str, attempts: int, last_error: Exception) -> None:
        self.attempts = attempts
        self.last_error = last_error
```

### CircuitBreakerOpenError

Raised when the circuit breaker is open.

```python
class CircuitBreakerOpenError(Exception):
    def __init__(self, message: str, recovery_time: float) -> None:
        self.recovery_time = recovery_time
```

---

## Type Aliases

### StartPosition

```python
StartPosition = Literal["beginning", "end", "checkpoint"] | int
```

### EventHandler

```python
EventHandler = Callable[[DomainEvent], Awaitable[None]]
```

### BatchHandler

```python
BatchHandler = Callable[[Sequence[DomainEvent]], Awaitable[None]]
```

### ErrorCallback

```python
ErrorCallback = Callable[[ErrorInfo], Awaitable[None]]
```

### SyncErrorCallback

```python
SyncErrorCallback = Callable[[ErrorInfo], None]
```

### TRANSIENT_EXCEPTIONS

```python
TRANSIENT_EXCEPTIONS: tuple[type[Exception], ...] = (
    ConnectionError,
    TimeoutError,
    asyncio.TimeoutError,
    OSError,
)
```

---

## Related Documentation

- [Events API Reference](./events.md) - DomainEvent and EventRegistry
- [Bus API Reference](./bus.md) - Event bus for publishing events
- [Stores API Reference](./stores.md) - Event store interfaces

---

## See Also

- **Source Code:** `src/eventsource/subscriptions/`
- **Tests:** `tests/unit/subscriptions/`
- **Examples:** `examples/subscriptions/`
