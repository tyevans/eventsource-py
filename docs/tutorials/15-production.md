# Tutorial 15: Production Deployment

**Difficulty:** Advanced

## Prerequisites

- [Tutorial 1: Introduction to Event Sourcing](01-introduction.md)
- [Tutorial 11: PostgreSQL Event Store](11-postgresql.md)
- [Tutorial 13: Subscription Management](13-subscriptions.md)
- [Tutorial 14: Optimizing with Aggregate Snapshotting](14-snapshotting.md)
- Python 3.10 or higher
- Understanding of async/await
- Basic Docker and database administration knowledge
- Understanding of production deployment concepts

## Learning Objectives

By the end of this tutorial, you will be able to:

1. Configure production-ready database connection pools
2. Implement comprehensive health checks for Kubernetes deployments
3. Set up structured logging for production observability
4. Integrate OpenTelemetry tracing for distributed systems
5. Configure graceful shutdown with signal handling
6. Tune event processing for production workloads
7. Implement monitoring and alerting strategies
8. Apply security best practices for production deployments

---

## Production Readiness Checklist

Before deploying to production, verify these key areas:

### Infrastructure

- PostgreSQL configured with proper connection limits
- Database indices created for event queries
- Connection pooling configured based on instance count
- Backup and point-in-time recovery (PITR) enabled
- Secrets management for credentials
- Network security and firewall rules

### Application

- Health check endpoints (liveness, readiness) implemented
- Graceful shutdown with signal handling configured
- Structured logging for aggregation
- Error monitoring and alerting
- Checkpoint strategy selected and configured
- Snapshot thresholds tuned for workload
- OpenTelemetry tracing enabled (optional)

### Operations

- Deployment runbook documented
- Monitoring dashboards configured
- Alert thresholds defined
- Capacity planning completed
- Disaster recovery plan tested
- On-call rotation established

---

## Database Connection Pool Configuration

Connection pooling is critical for production performance. Proper configuration prevents connection exhaustion and optimizes database resource usage.

### Basic Connection Pool Setup

```python
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

def create_production_engine(
    database_url: str,
    pool_size: int = 10,
    max_overflow: int = 20,
):
    """
    Create production-ready database engine.

    Args:
        database_url: PostgreSQL connection URL
        pool_size: Number of persistent connections
        max_overflow: Additional connections under load
    """
    return create_async_engine(
        database_url,
        pool_size=pool_size,           # Persistent connections
        max_overflow=max_overflow,      # Temporary overflow connections
        pool_timeout=30.0,              # Wait time for connection
        pool_recycle=1800,              # Recycle connections after 30 min
        pool_pre_ping=True,             # Verify connection before use
        echo=False,                     # Disable SQL logging
        connect_args={
            "server_settings": {
                "statement_timeout": "30000",  # 30s query timeout
            }
        },
    )

# Create session factory
engine = create_production_engine(
    "postgresql+asyncpg://user:pass@localhost:5432/mydb"
)
session_factory = async_sessionmaker(
    engine,
    expire_on_commit=False,  # Performance optimization
)
```

### Connection Pool Sizing

**Formula:** `total_connections = (pool_size + max_overflow) * instance_count`

**Constraint:** Must not exceed PostgreSQL `max_connections` (default: 100)

| Workload | Instances | pool_size | max_overflow | Total |
|----------|-----------|-----------|--------------|-------|
| Development | 1 | 5 | 5 | 10 |
| Light | 2 | 5 | 10 | 30 |
| Medium | 4 | 10 | 15 | 100 |
| Heavy | 8 | 5 | 7 | 96 |

**Example calculation:**
```python
import os

# Environment configuration
instance_count = int(os.getenv("INSTANCE_COUNT", "1"))
max_db_connections = int(os.getenv("MAX_DB_CONNECTIONS", "100"))

# Reserve 10% for admin connections
usable_connections = int(max_db_connections * 0.9)

# Divide across instances
connections_per_instance = usable_connections // instance_count

# Split into pool_size and max_overflow (2:1 ratio)
pool_size = max(2, connections_per_instance // 3)
max_overflow = connections_per_instance - pool_size

print(f"Pool config: pool_size={pool_size}, max_overflow={max_overflow}")
```

### Common Connection Pool Issues

**Problem:** Connection pool exhausted

**Symptoms:** `TimeoutError: QueuePool limit exceeded`

**Solutions:**
- Increase `pool_size` and `max_overflow`
- Reduce `max_in_flight` in subscription configs
- Check for connection leaks (unclosed sessions)

**Problem:** Too many database connections

**Symptoms:** PostgreSQL `FATAL: too many connections`

**Solutions:**
- Reduce `pool_size + max_overflow` per instance
- Increase PostgreSQL `max_connections`
- Use connection pooler like PgBouncer

---

## Event Processing Configuration

Configure event stores and subscription managers for production workloads.

### Production Event Store

```python
from eventsource import PostgreSQLEventStore

event_store = PostgreSQLEventStore(
    session_factory,
    enable_tracing=True,       # OpenTelemetry tracing
    auto_detect_uuid=True,     # Automatically parse UUID strings
)
```

### Production Subscription Manager

```python
from eventsource.subscriptions import (
    SubscriptionManager,
    SubscriptionConfig,
    CheckpointStrategy,
    HealthCheckConfig,
)

# Health check thresholds
health_config = HealthCheckConfig(
    max_error_rate_per_minute=10.0,    # Degraded at 10 errors/min
    max_errors_warning=50,              # Warning threshold
    max_errors_critical=200,            # Critical threshold
    max_lag_events_warning=5000,        # Warn if 5k events behind
    max_lag_events_critical=50000,      # Critical if 50k behind
)

# Create manager
manager = SubscriptionManager(
    event_store=event_store,
    event_bus=event_bus,
    checkpoint_repo=checkpoint_repo,
    shutdown_timeout=60.0,              # 60s graceful shutdown
    drain_timeout=30.0,                 # 30s to drain in-flight
    health_check_config=health_config,
    enable_tracing=True,                # OpenTelemetry tracing
)

# Production subscription config
config = SubscriptionConfig(
    start_from="checkpoint",            # Resume from last position
    batch_size=100,                     # Events per batch
    max_in_flight=1000,                 # Concurrent processing limit
    checkpoint_strategy=CheckpointStrategy.EVERY_BATCH,
    processing_timeout=30.0,            # Max time per event
    continue_on_error=True,             # Continue after DLQ
    max_retries=5,                      # Retry attempts
    circuit_breaker_enabled=True,       # Enable circuit breaker
)

# Subscribe projections
await manager.subscribe(projection, config=config, name="orders")
```

### Tuning Guidelines

**Batch Size:**
- **Small (10-50):** Low latency, higher database load
- **Medium (50-200):** Balanced (recommended)
- **Large (200-1000):** High throughput, higher latency

**Max In-Flight:**
- Formula: `max_in_flight = batch_size * 10`
- Higher values = more concurrency, more memory
- Lower values = less memory, slower catch-up

**Checkpoint Strategy:**
- `EVERY_EVENT`: Maximum durability, slowest
- `EVERY_BATCH`: Recommended for production
- `PERIODIC`: Fastest, suitable for analytics

---

## Health Check Endpoints

Implement Kubernetes-compatible health checks for production deployments.

### FastAPI Health Endpoints

```python
from fastapi import FastAPI, Response
from contextlib import asynccontextmanager

# Global manager reference
manager: SubscriptionManager | None = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle."""
    global manager

    # Startup
    event_store = PostgreSQLEventStore(session_factory)
    event_bus = InMemoryEventBus()
    checkpoint_repo = PostgreSQLCheckpointRepository(session_factory)

    manager = SubscriptionManager(
        event_store=event_store,
        event_bus=event_bus,
        checkpoint_repo=checkpoint_repo,
        shutdown_timeout=60.0,
        drain_timeout=30.0,
    )

    # Subscribe projections
    await manager.subscribe(order_projection, name="orders")
    await manager.subscribe(customer_projection, name="customers")

    # Start manager
    await manager.start()

    yield

    # Shutdown
    if manager:
        await manager.stop()

app = FastAPI(lifespan=lifespan)

@app.get("/health")
async def health_check():
    """
    Overall health check endpoint.

    Returns 200 if healthy/degraded, 503 if unhealthy/critical.
    """
    if not manager:
        return Response(
            content='{"status":"unhealthy","reason":"manager not initialized"}',
            status_code=503,
            media_type="application/json",
        )

    health = await manager.health_check()

    # Return 503 for unhealthy or critical
    status_code = 200 if health.status in ("healthy", "degraded") else 503

    return Response(
        content=health.model_dump_json(),
        status_code=status_code,
        media_type="application/json",
    )

@app.get("/health/ready")
async def readiness_check():
    """
    Readiness probe for Kubernetes.

    Returns 200 if ready to receive traffic, 503 otherwise.
    """
    if not manager:
        return Response(
            content='{"ready":false,"reason":"manager not initialized"}',
            status_code=503,
            media_type="application/json",
        )

    readiness = await manager.readiness_check()
    status_code = 200 if readiness.ready else 503

    return Response(
        content=readiness.model_dump_json(),
        status_code=status_code,
        media_type="application/json",
    )

@app.get("/health/live")
async def liveness_check():
    """
    Liveness probe for Kubernetes.

    Returns 200 if alive, 503 if needs restart.
    """
    if not manager:
        return Response(
            content='{"alive":false,"reason":"manager not initialized"}',
            status_code=503,
            media_type="application/json",
        )

    liveness = await manager.liveness_check()
    status_code = 200 if liveness.alive else 503

    return Response(
        content=liveness.model_dump_json(),
        status_code=status_code,
        media_type="application/json",
    )
```

### Kubernetes Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: eventsource-app
spec:
  replicas: 3
  template:
    spec:
      containers:
      - name: app
        image: myapp:latest
        ports:
        - containerPort: 8000

        # Liveness probe - restart if fails
        livenessProbe:
          httpGet:
            path: /health/live
            port: 8000
          initialDelaySeconds: 30
          periodSeconds: 10
          timeoutSeconds: 5
          failureThreshold: 3

        # Readiness probe - stop sending traffic if fails
        readinessProbe:
          httpGet:
            path: /health/ready
            port: 8000
          initialDelaySeconds: 10
          periodSeconds: 5
          timeoutSeconds: 3
          failureThreshold: 2
```

### Health Check Response Examples

**Healthy:**
```json
{
  "status": "healthy",
  "running": true,
  "subscription_count": 2,
  "healthy_count": 2,
  "degraded_count": 0,
  "unhealthy_count": 0,
  "total_events_processed": 125000,
  "total_events_failed": 0,
  "total_lag_events": 0,
  "uptime_seconds": 3600.5,
  "timestamp": "2024-01-15T12:00:00Z",
  "subscriptions": [...]
}
```

**Degraded:**
```json
{
  "status": "degraded",
  "running": true,
  "subscription_count": 2,
  "healthy_count": 1,
  "degraded_count": 1,
  "unhealthy_count": 0,
  "total_events_processed": 125000,
  "total_events_failed": 15,
  "total_lag_events": 3000,
  "uptime_seconds": 3600.5
}
```

---

## Structured Logging

Production systems require structured logging for aggregation and analysis.

### JSON Logging Configuration

```python
import logging
import json
import sys
from datetime import datetime, UTC

class JSONFormatter(logging.Formatter):
    """Format log records as JSON for structured logging."""

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON."""
        log_data = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        # Add custom fields from extra parameter
        for key, value in record.__dict__.items():
            if key not in {
                "name", "msg", "args", "created", "filename", "funcName",
                "levelname", "levelno", "lineno", "module", "msecs",
                "message", "pathname", "process", "processName",
                "relativeCreated", "thread", "threadName", "exc_info",
                "exc_text", "stack_info",
            }:
                log_data[key] = value

        return json.dumps(log_data)

def configure_production_logging(level: str = "INFO") -> logging.Logger:
    """
    Configure production logging with JSON format.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)

    Returns:
        Configured root logger
    """
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, level.upper()))

    # Remove existing handlers
    root_logger.handlers.clear()

    # Add JSON handler
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JSONFormatter())
    root_logger.addHandler(handler)

    # Reduce SQLAlchemy noise
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)

    return root_logger

# Usage
logger = configure_production_logging("INFO")

# Log with structured data
logger.info(
    "Order processed successfully",
    extra={
        "order_id": "550e8400-e29b-41d4-a716-446655440000",
        "customer_id": "123",
        "total": 99.99,
        "duration_ms": 45.2,
    }
)
```

**Output:**
```json
{
  "timestamp": "2024-01-15T12:00:00.123456Z",
  "level": "INFO",
  "logger": "__main__",
  "message": "Order processed successfully",
  "order_id": "550e8400-e29b-41d4-a716-446655440000",
  "customer_id": "123",
  "total": 99.99,
  "duration_ms": 45.2
}
```

### Logging Best Practices

**DO:**
- Use structured logging with JSON format
- Include correlation IDs for request tracing
- Log at appropriate levels (INFO for events, ERROR for failures)
- Include context (aggregate_id, event_type, etc.)
- Use `extra={}` for structured fields

**DON'T:**
- Log sensitive data (passwords, credit cards, PII)
- Log at DEBUG level in production
- Use string concatenation for log messages
- Log the same event multiple times

---

## OpenTelemetry Tracing

Distributed tracing helps debug issues in production systems.

### Tracing Configuration

```python
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource, SERVICE_NAME, SERVICE_VERSION
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

def configure_tracing(
    service_name: str,
    service_version: str = "1.0.0",
    otlp_endpoint: str = "localhost:4317",
) -> TracerProvider:
    """
    Configure OpenTelemetry tracing.

    Args:
        service_name: Name of the service
        service_version: Version of the service
        otlp_endpoint: OTLP collector endpoint

    Returns:
        Configured tracer provider
    """
    # Create resource with service metadata
    resource = Resource.create({
        SERVICE_NAME: service_name,
        SERVICE_VERSION: service_version,
    })

    # Create tracer provider
    provider = TracerProvider(resource=resource)

    # Configure OTLP exporter
    exporter = OTLPSpanExporter(
        endpoint=otlp_endpoint,
        insecure=True,  # Use TLS in production
    )

    # Add batch span processor
    processor = BatchSpanProcessor(exporter)
    provider.add_span_processor(processor)

    # Set as global provider
    trace.set_tracer_provider(provider)

    return provider

# Usage
import os

configure_tracing(
    service_name=os.getenv("SERVICE_NAME", "eventsource-app"),
    service_version=os.getenv("SERVICE_VERSION", "1.0.0"),
    otlp_endpoint=os.getenv("OTLP_ENDPOINT", "localhost:4317"),
)

# Enable tracing in components
event_store = PostgreSQLEventStore(
    session_factory,
    enable_tracing=True,  # Traces database operations
)

manager = SubscriptionManager(
    event_store=event_store,
    event_bus=event_bus,
    checkpoint_repo=checkpoint_repo,
    enable_tracing=True,  # Traces subscription operations
)

repo = AggregateRepository(
    event_store=event_store,
    aggregate_factory=OrderAggregate,
    aggregate_type="Order",
    enable_tracing=True,  # Traces aggregate operations
)
```

### Trace Attributes

eventsource-py automatically adds these attributes to spans:

- `event.id`: Event UUID
- `event.type`: Event type name
- `aggregate.id`: Aggregate UUID
- `aggregate.type`: Aggregate type name
- `subscription.name`: Subscription name
- `subscription.state`: Current state
- `db.system`: Database type (postgresql)
- `db.operation`: Database operation (select, insert, etc.)

---

## Graceful Shutdown

Production systems must handle shutdown signals gracefully.

### Signal Handling with SubscriptionManager

```python
import asyncio
import signal
import logging
from typing import NoReturn

logger = logging.getLogger(__name__)

async def run_production_service() -> NoReturn:
    """
    Run production service with graceful shutdown.

    Handles SIGTERM and SIGINT signals for graceful shutdown.
    """
    # Create infrastructure
    engine = create_production_engine(
        os.getenv("DATABASE_URL", "postgresql+asyncpg://..."),
    )
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    event_store = PostgreSQLEventStore(session_factory, enable_tracing=True)
    event_bus = InMemoryEventBus()
    checkpoint_repo = PostgreSQLCheckpointRepository(session_factory)

    manager = SubscriptionManager(
        event_store=event_store,
        event_bus=event_bus,
        checkpoint_repo=checkpoint_repo,
        shutdown_timeout=60.0,   # Max shutdown time
        drain_timeout=30.0,      # Max drain time
    )

    # Subscribe projections
    await manager.subscribe(order_projection, name="orders")
    await manager.subscribe(customer_projection, name="customers")

    logger.info("Starting subscription manager")

    # Run until signal received
    result = await manager.run_until_shutdown()

    # Log shutdown details
    logger.info(
        "Shutdown complete",
        extra={
            "phase": result.phase.value,
            "duration_seconds": result.duration_seconds,
            "forced": result.forced,
            "events_in_flight": result.events_in_flight,
            "checkpoints_saved": result.checkpoints_saved,
        }
    )

    # Cleanup
    await engine.dispose()

if __name__ == "__main__":
    # Configure logging
    configure_production_logging("INFO")

    # Run service
    asyncio.run(run_production_service())
```

### Shutdown Sequence

1. **Signal received** (SIGTERM or SIGINT)
2. **Stop accepting new events** - Coordinators stop reading
3. **Drain in-flight events** - Wait up to `drain_timeout` for processing to complete
4. **Save checkpoints** - Persist final positions
5. **Cleanup resources** - Close database connections

### Manual Shutdown Trigger

```python
# Programmatically request shutdown
manager.request_shutdown()

# Check shutdown status
if manager.is_shutting_down:
    logger.info(f"Shutdown phase: {manager.shutdown_phase.value}")
```

---

## Error Monitoring

Production systems require comprehensive error monitoring and alerting.

### Error Callbacks

```python
from eventsource.subscriptions import ErrorInfo, ErrorSeverity

async def log_all_errors(error_info: ErrorInfo) -> None:
    """Log all errors with structured data."""
    logger.error(
        "Subscription error",
        extra={
            "subscription": error_info.subscription_name,
            "event_id": str(error_info.event_id),
            "event_type": error_info.event_type,
            "error_message": error_info.error_message,
            "severity": error_info.classification.severity.value,
            "category": error_info.classification.category.value,
            "retry_count": error_info.retry_count,
        }
    )

async def alert_critical_errors(error_info: ErrorInfo) -> None:
    """Send alerts for critical errors."""
    logger.critical(
        f"CRITICAL ERROR in {error_info.subscription_name}",
        extra={
            "event_id": str(error_info.event_id),
            "error_message": error_info.error_message,
        }
    )

    # Integration points for alerting
    # await send_pagerduty_alert(error_info)
    # await send_slack_notification(error_info)
    # await send_opsgenie_alert(error_info)

# Register error callbacks
manager.on_error(log_all_errors)
manager.on_error_severity(ErrorSeverity.CRITICAL, alert_critical_errors)
```

### Error Statistics

```python
# Get error statistics
stats = manager.get_error_stats()

logger.info(
    "Error statistics",
    extra={
        "total_errors": stats["total_errors"],
        "total_dlq_count": stats["total_dlq_count"],
        "subscriptions": stats["subscriptions"],
    }
)
```

---

## Scaling Considerations

### Horizontal Scaling

Run multiple instances with partitioned event processing:

```python
import hashlib
import os

def get_partition_for_aggregate(
    aggregate_id: str,
    num_partitions: int,
) -> int:
    """Use consistent hashing to assign aggregates to partitions."""
    hash_value = int(hashlib.sha256(aggregate_id.encode()).hexdigest(), 16)
    return hash_value % num_partitions

class PartitionedProjection:
    """Projection that processes only its assigned partition."""

    def __init__(self, partition_id: int, num_partitions: int):
        self.partition_id = partition_id
        self.num_partitions = num_partitions

    def should_handle(self, aggregate_id: str) -> bool:
        """Check if this instance should handle the aggregate."""
        partition = get_partition_for_aggregate(aggregate_id, self.num_partitions)
        return partition == self.partition_id

    async def handle(self, event: DomainEvent) -> None:
        """Handle event if it belongs to this partition."""
        if self.should_handle(str(event.aggregate_id)):
            await self._process_event(event)

    async def _process_event(self, event: DomainEvent) -> None:
        """Process the event."""
        # Implementation here
        pass

# Configure from environment
partition_id = int(os.getenv("PARTITION_ID", "0"))
num_partitions = int(os.getenv("NUM_PARTITIONS", "1"))

projection = PartitionedProjection(partition_id, num_partitions)
```

### Connection Pool Scaling

```python
# Calculate pool size based on instance count
instance_count = int(os.getenv("INSTANCE_COUNT", "1"))
max_db_connections = int(os.getenv("MAX_DB_CONNECTIONS", "100"))

# Reserve connections for admin
usable = int(max_db_connections * 0.9)
per_instance = usable // instance_count

pool_size = max(2, per_instance // 3)
max_overflow = per_instance - pool_size

logger.info(
    "Connection pool configuration",
    extra={
        "instance_count": instance_count,
        "pool_size": pool_size,
        "max_overflow": max_overflow,
        "total_per_instance": pool_size + max_overflow,
    }
)
```

---

## Security Best Practices

### Database Connection Security

```python
def create_secure_engine(
    host: str,
    port: int,
    database: str,
    user: str,
    password: str,
    ssl_mode: str = "require",
    ssl_root_cert: str | None = None,
):
    """Create database engine with SSL/TLS."""
    url = f"postgresql+asyncpg://{user}:{password}@{host}:{port}/{database}"

    connect_args = {"ssl": ssl_mode}
    if ssl_root_cert:
        connect_args["ssl"] = {"ca": ssl_root_cert}

    return create_async_engine(
        url,
        connect_args=connect_args,
        pool_pre_ping=True,
    )
```

### Secrets Management

```python
import os

# DO: Use environment variables
database_url = os.getenv("DATABASE_URL")

# DO: Use secrets management systems
# - Kubernetes Secrets
# - AWS Secrets Manager
# - HashiCorp Vault
# - Azure Key Vault

# DON'T: Hard-code credentials
# database_url = "postgresql://user:password@localhost/db"  # WRONG!
```

---

## Complete Production Example

```python
"""
Tutorial 15: Production Deployment

Production-ready event-sourced application with:
- Connection pooling
- Health checks
- Structured logging
- Graceful shutdown
- Error monitoring

Run with: python tutorial_15_production.py
"""

import asyncio
import logging
import os
import json
import sys
from datetime import datetime, UTC
from uuid import UUID, uuid4

from pydantic import BaseModel
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from eventsource import (
    AggregateRepository,
    AggregateRoot,
    DomainEvent,
    InMemoryEventBus,
    PostgreSQLEventStore,
    PostgreSQLCheckpointRepository,
    register_event,
)
from eventsource.subscriptions import (
    CheckpointStrategy,
    ErrorInfo,
    ErrorSeverity,
    HealthCheckConfig,
    SubscriptionConfig,
    SubscriptionManager,
)


# =============================================================================
# Logging Configuration
# =============================================================================

class JSONFormatter(logging.Formatter):
    """Format logs as JSON for production."""

    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        # Add extra fields
        for key, value in record.__dict__.items():
            if key not in {
                "name", "msg", "args", "created", "filename", "funcName",
                "levelname", "levelno", "lineno", "module", "msecs",
                "message", "pathname", "process", "processName",
                "relativeCreated", "thread", "threadName", "exc_info",
                "exc_text", "stack_info",
            }:
                log_data[key] = value

        return json.dumps(log_data)

def configure_logging(level: str = "INFO") -> logging.Logger:
    """Configure production logging."""
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, level.upper()))
    root_logger.handlers.clear()

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JSONFormatter())
    root_logger.addHandler(handler)

    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)

    return root_logger


# =============================================================================
# Application Configuration
# =============================================================================

class AppConfig:
    """Production application configuration."""

    def __init__(self):
        self.database_url = os.getenv(
            "DATABASE_URL",
            "postgresql+asyncpg://postgres:postgres@localhost:5432/eventsource_prod"
        )

        # Connection pool sizing
        instance_count = int(os.getenv("INSTANCE_COUNT", "1"))
        max_db_connections = int(os.getenv("MAX_DB_CONNECTIONS", "100"))
        usable = int(max_db_connections * 0.9)
        per_instance = usable // instance_count

        self.pool_size = max(2, per_instance // 3)
        self.max_overflow = per_instance - self.pool_size

        # Timeouts
        self.shutdown_timeout = float(os.getenv("SHUTDOWN_TIMEOUT", "60"))
        self.drain_timeout = float(os.getenv("DRAIN_TIMEOUT", "30"))

        # Logging
        self.log_level = os.getenv("LOG_LEVEL", "INFO")


# =============================================================================
# Domain Model
# =============================================================================

@register_event
class OrderCreated(DomainEvent):
    event_type: str = "OrderCreated"
    aggregate_type: str = "Order"
    customer_id: str
    total: float

@register_event
class OrderShipped(DomainEvent):
    event_type: str = "OrderShipped"
    aggregate_type: str = "Order"
    tracking_number: str

class OrderState(BaseModel):
    order_id: UUID
    customer_id: str | None = None
    total: float = 0.0
    status: str = "draft"
    tracking_number: str | None = None

class OrderAggregate(AggregateRoot[OrderState]):
    aggregate_type = "Order"

    def _get_initial_state(self) -> OrderState:
        return OrderState(order_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, OrderCreated):
            self._state = OrderState(
                order_id=self.aggregate_id,
                customer_id=event.customer_id,
                total=event.total,
                status="created",
            )
        elif isinstance(event, OrderShipped):
            if self._state:
                self._state = self._state.model_copy(
                    update={
                        "status": "shipped",
                        "tracking_number": event.tracking_number,
                    }
                )

    def create(self, customer_id: str, total: float) -> None:
        """Create a new order."""
        if self.version > 0:
            raise ValueError("Order already exists")
        self.apply_event(
            OrderCreated(
                aggregate_id=self.aggregate_id,
                customer_id=customer_id,
                total=total,
                aggregate_version=self.get_next_version(),
            )
        )

    def ship(self, tracking_number: str) -> None:
        """Ship the order."""
        if not self.state or self.state.status != "created":
            raise ValueError("Cannot ship order in current state")
        self.apply_event(
            OrderShipped(
                aggregate_id=self.aggregate_id,
                tracking_number=tracking_number,
                aggregate_version=self.get_next_version(),
            )
        )


# =============================================================================
# Error Monitoring
# =============================================================================

async def log_all_errors(error_info: ErrorInfo) -> None:
    """Log all subscription errors."""
    logger = logging.getLogger("eventsource.errors")
    logger.error(
        "Subscription error",
        extra={
            "subscription": error_info.subscription_name,
            "event_id": str(error_info.event_id),
            "error_message": error_info.error_message,
            "severity": error_info.classification.severity.value,
        }
    )

async def alert_critical_errors(error_info: ErrorInfo) -> None:
    """Alert on critical errors."""
    logger = logging.getLogger("eventsource.alerts")
    logger.critical(
        f"CRITICAL: {error_info.error_message}",
        extra={"subscription": error_info.subscription_name}
    )


# =============================================================================
# Production Application
# =============================================================================

class ProductionApp:
    """Production application with full production features."""

    def __init__(self, config: AppConfig):
        self.config = config
        self.logger = configure_logging(config.log_level)
        self.engine = None
        self.manager = None
        self.repo = None

    async def initialize(self) -> None:
        """Initialize application infrastructure."""
        self.logger.info("Initializing application")

        # Create database engine
        self.engine = create_async_engine(
            self.config.database_url,
            pool_size=self.config.pool_size,
            max_overflow=self.config.max_overflow,
            pool_timeout=30.0,
            pool_recycle=1800,
            pool_pre_ping=True,
        )

        self.logger.info(
            "Database engine created",
            extra={
                "pool_size": self.config.pool_size,
                "max_overflow": self.config.max_overflow,
            }
        )

        session_factory = async_sessionmaker(
            self.engine,
            expire_on_commit=False,
        )

        # Create infrastructure
        event_store = PostgreSQLEventStore(session_factory, enable_tracing=False)
        event_bus = InMemoryEventBus()
        checkpoint_repo = PostgreSQLCheckpointRepository(session_factory)

        # Health check config
        health_config = HealthCheckConfig(
            max_error_rate_per_minute=10.0,
            max_errors_warning=50,
            max_errors_critical=200,
            max_lag_events_warning=5000,
            max_lag_events_critical=50000,
        )

        # Create subscription manager
        self.manager = SubscriptionManager(
            event_store=event_store,
            event_bus=event_bus,
            checkpoint_repo=checkpoint_repo,
            shutdown_timeout=self.config.shutdown_timeout,
            drain_timeout=self.config.drain_timeout,
            health_check_config=health_config,
            enable_tracing=False,
        )

        # Register error callbacks
        self.manager.on_error(log_all_errors)
        self.manager.on_error_severity(ErrorSeverity.CRITICAL, alert_critical_errors)

        # Create repository
        self.repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=OrderAggregate,
            aggregate_type="Order",
            event_publisher=event_bus,
            enable_tracing=False,
        )

        self.logger.info("Application initialized successfully")

    async def start(self) -> None:
        """Start the application."""
        self.logger.info("Starting application")
        await self.manager.start()
        self.logger.info("Application started")

    async def stop(self) -> None:
        """Stop the application gracefully."""
        self.logger.info("Stopping application")

        if self.manager:
            result = await self.manager.stop()
            self.logger.info(
                "Manager stopped",
                extra={
                    "phase": result.phase.value,
                    "duration_seconds": result.duration_seconds,
                    "forced": result.forced,
                }
            )

        if self.engine:
            await self.engine.dispose()

        self.logger.info("Application stopped")

    async def health_check(self) -> dict:
        """Get application health status."""
        if not self.manager:
            return {"status": "unhealthy", "reason": "manager not initialized"}

        health = await self.manager.health_check()
        return health.to_dict()


# =============================================================================
# Main Entry Point
# =============================================================================

async def main():
    """Run production application."""
    config = AppConfig()
    app = ProductionApp(config)

    try:
        # Initialize
        await app.initialize()
        await app.start()

        # Demo: Create an order
        order_id = uuid4()
        order = app.repo.create_new(order_id)
        order.create(customer_id="cust-prod-001", total=499.99)
        await app.repo.save(order)

        app.logger.info(
            "Order created",
            extra={"order_id": str(order_id), "total": 499.99}
        )

        # Check health
        health = await app.health_check()
        app.logger.info(
            "Health check",
            extra={"status": health["status"]}
        )

        # Run for a bit
        app.logger.info("Application running. Press Ctrl+C to stop.")
        await asyncio.sleep(5)

    finally:
        await app.stop()

if __name__ == "__main__":
    asyncio.run(main())
```

---

## Key Takeaways

1. **Connection pools must be sized correctly**: Calculate based on instance count and database limits
2. **Health checks are essential**: Implement liveness and readiness for Kubernetes
3. **Structured logging enables observability**: Use JSON format for log aggregation
4. **Graceful shutdown prevents data loss**: Handle signals and drain in-flight events
5. **Error monitoring catches issues early**: Register callbacks for critical errors
6. **OpenTelemetry provides visibility**: Enable tracing for distributed debugging
7. **Security is non-negotiable**: Use SSL/TLS, secrets management, and proper authentication
8. **Tune for your workload**: Adjust batch sizes, checkpoint strategies, and pool sizes based on metrics

---

## Next Steps

Continue to [Tutorial 16: Multi-Tenancy](16-multi-tenancy.md) to learn about:
- Tenant isolation patterns
- Tenant-scoped event stores
- Multi-tenant projections
- Tenant migration strategies

For production deployment examples, see:
- Examples: `examples/projection_example.py`
- Tests: `tests/integration/subscriptions/test_resilience.py`
- Documentation: Production deployment guides
