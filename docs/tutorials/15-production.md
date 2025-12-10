# Tutorial 15: Production Deployment Guide

**Estimated Time:** 90-120 minutes
**Difficulty:** Advanced
**Progress:** Tutorial 15 of 21 | Phase 3: Production Readiness

---

## Prerequisites

Before starting this tutorial, ensure you have:

- Completed [Tutorial 14: Optimizing with Aggregate Snapshotting](14-snapshotting.md)
- Completed [Tutorial 13: Subscription Management](13-subscriptions.md)
- Understanding of PostgreSQL configuration from [Tutorial 11: PostgreSQL](11-postgresql.md)
- Experience with Docker and container orchestration basics
- Python 3.11+ installed
- eventsource-py with PostgreSQL support installed:

```bash
pip install eventsource-py[postgresql]
```

This tutorial provides a comprehensive guide to deploying event-sourced applications in production environments.

---

## Learning Objectives

By the end of this tutorial, you will be able to:

1. Complete a production deployment checklist for event-sourced applications
2. Configure performance tuning for high-throughput scenarios
3. Implement monitoring and observability using health checks and metrics
4. Design scaling strategies for event stores and projections
5. Plan backup and recovery procedures for event data
6. Apply security best practices for production deployments

---

## Production Deployment Overview

Deploying event-sourced applications to production requires careful attention to several interconnected concerns:

```
Production Deployment Pillars
=============================

    +-------------------+
    |   Application     |
    +--------+----------+
             |
    +--------v----------+
    |  Performance      |  <-- Connection pools, batching, caching
    +--------+----------+
             |
    +--------v----------+
    |  Reliability      |  <-- Health checks, graceful shutdown, retries
    +--------+----------+
             |
    +--------v----------+
    |  Observability    |  <-- Metrics, tracing, logging
    +--------+----------+
             |
    +--------v----------+
    |  Scalability      |  <-- Horizontal scaling, partitioning
    +--------+----------+
             |
    +--------v----------+
    |  Security         |  <-- Authentication, encryption, audit
    +-------------------+
```

This tutorial covers each pillar systematically, providing practical configurations and code examples you can apply to your deployments.

---

## Production Checklist

Before deploying to production, verify each item on this checklist:

### Infrastructure Checklist

| Item | Status | Notes |
|------|--------|-------|
| PostgreSQL configured with appropriate resources | [ ] | See performance tuning section |
| Connection pooling configured | [ ] | Pool size based on workload |
| Database schema migrations applied | [ ] | Use Alembic or similar |
| Event indexes created | [ ] | Essential for query performance |
| Backup strategy implemented | [ ] | Point-in-time recovery enabled |
| Monitoring endpoints exposed | [ ] | Health, readiness, liveness |
| Log aggregation configured | [ ] | Structured JSON logging |
| Secrets management in place | [ ] | Environment variables or vault |

### Application Checklist

| Item | Status | Notes |
|------|--------|-------|
| Health checks implemented | [ ] | Readiness and liveness probes |
| Graceful shutdown configured | [ ] | Signal handlers registered |
| Error handling with DLQ | [ ] | Failed events captured |
| Checkpointing strategy selected | [ ] | Balance durability vs performance |
| Snapshot strategy configured | [ ] | For high-event aggregates |
| Tracing enabled | [ ] | OpenTelemetry integration |
| Retry policies defined | [ ] | Transient error handling |
| Rate limiting considered | [ ] | Protect against overload |

### Operational Checklist

| Item | Status | Notes |
|------|--------|-------|
| Runbook documented | [ ] | Common operations and troubleshooting |
| Alerting rules configured | [ ] | Critical errors, lag thresholds |
| Capacity planning completed | [ ] | Storage growth projections |
| Disaster recovery tested | [ ] | Restore procedures verified |
| Performance baselines established | [ ] | Normal operating metrics |
| On-call rotation defined | [ ] | Incident response process |

---

## Performance Tuning

### Connection Pool Configuration

Connection pooling is critical for async applications. Configure pools based on your workload characteristics:

```python
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession

def create_production_engine(database_url: str, workload: str = "medium"):
    """
    Create a production-configured async engine.

    Args:
        database_url: PostgreSQL connection URL
        workload: "light", "medium", or "heavy"
    """
    # Pool configurations by workload
    pool_configs = {
        "light": {"pool_size": 3, "max_overflow": 5},
        "medium": {"pool_size": 10, "max_overflow": 15},
        "heavy": {"pool_size": 20, "max_overflow": 30},
    }

    config = pool_configs.get(workload, pool_configs["medium"])

    engine = create_async_engine(
        database_url,
        # Connection pool sizing
        pool_size=config["pool_size"],
        max_overflow=config["max_overflow"],

        # Pool maintenance
        pool_timeout=30,        # Wait time for available connection
        pool_recycle=1800,      # Recycle connections after 30 minutes
        pool_pre_ping=True,     # Verify connections before use

        # Performance settings
        echo=False,             # Disable SQL logging in production

        # Connection args for PostgreSQL
        connect_args={
            "server_settings": {
                "application_name": "eventsource-app",
                "statement_timeout": "30000",  # 30 second query timeout
            }
        },
    )

    return engine


def create_production_session_factory(engine):
    """Create a production-configured session factory."""
    return async_sessionmaker(
        engine,
        expire_on_commit=False,  # Required for async
        class_=AsyncSession,
    )
```

**Pool Sizing Guidelines:**

| Metric | Light | Medium | Heavy |
|--------|-------|--------|-------|
| Concurrent requests | < 50 | 50-500 | > 500 |
| pool_size | 3-5 | 10-15 | 20-30 |
| max_overflow | 5-10 | 15-20 | 30-50 |
| Total connections | 8-15 | 25-35 | 50-80 |

**Important:** Total connections across all application instances must not exceed PostgreSQL's `max_connections` setting (default: 100).

### Event Store Optimization

Configure the PostgreSQL event store for production workloads:

```python
from eventsource import PostgreSQLEventStore

def create_production_event_store(session_factory):
    """Create a production-configured event store."""
    return PostgreSQLEventStore(
        session_factory,
        # Outbox pattern for reliable publishing
        outbox_enabled=True,

        # Enable OpenTelemetry tracing
        enable_tracing=True,

        # UUID field detection
        auto_detect_uuid=True,

        # Exclude external IDs from UUID conversion
        string_id_fields={"stripe_id", "external_ref"},
    )
```

### Batch Processing Configuration

For high-throughput scenarios, tune batch processing in subscription managers:

```python
from eventsource.subscriptions import (
    SubscriptionManager,
    SubscriptionConfig,
    CheckpointStrategy,
    HealthCheckConfig,
)

def create_production_subscription_config():
    """Create production-optimized subscription configuration."""
    return SubscriptionConfig(
        # Resume from last checkpoint
        start_from="checkpoint",

        # Batch processing for throughput
        batch_size=100,          # Events per batch
        max_in_flight=1000,      # Concurrent event limit

        # Balanced checkpoint strategy
        checkpoint_strategy=CheckpointStrategy.EVERY_BATCH,

        # Error handling
        continue_on_error=True,  # Don't stop on individual failures
        processing_timeout=30.0, # Max seconds per event

        # Backpressure threshold (80% of max_in_flight)
        backpressure_threshold=0.8,
    )


def create_production_manager(
    event_store,
    event_bus,
    checkpoint_repo,
    dlq_repo=None,
):
    """Create a production-configured subscription manager."""
    health_config = HealthCheckConfig(
        # Error thresholds
        max_error_rate_per_minute=10.0,
        max_errors_warning=10,
        max_errors_critical=100,

        # Lag thresholds
        max_lag_events_warning=1000,
        max_lag_events_critical=10000,

        # DLQ thresholds
        max_dlq_events_warning=10,
        max_dlq_events_critical=100,
    )

    return SubscriptionManager(
        event_store=event_store,
        event_bus=event_bus,
        checkpoint_repo=checkpoint_repo,

        # Graceful shutdown timing
        shutdown_timeout=30.0,   # Total shutdown timeout
        drain_timeout=10.0,      # Time to drain in-flight events

        # Dead letter queue for failed events
        dlq_repo=dlq_repo,

        # Health monitoring configuration
        health_check_config=health_config,

        # Tracing
        enable_tracing=True,
    )
```

### Snapshot Configuration for Performance

For aggregates with many events, configure snapshotting to optimize load times:

```python
from eventsource import AggregateRepository
from eventsource.snapshots import PostgreSQLSnapshotStore

def create_production_repository(
    event_store,
    snapshot_session_factory,
    aggregate_factory,
    aggregate_type: str,
    event_publisher=None,
):
    """Create a repository with production snapshot configuration."""
    # Create snapshot store
    snapshot_store = PostgreSQLSnapshotStore(snapshot_session_factory)

    return AggregateRepository(
        event_store=event_store,
        aggregate_factory=aggregate_factory,
        aggregate_type=aggregate_type,
        event_publisher=event_publisher,

        # Snapshot configuration
        snapshot_store=snapshot_store,
        snapshot_threshold=100,      # Snapshot every 100 events
        snapshot_mode="background",  # Non-blocking snapshot creation

        # Tracing
        enable_tracing=True,
    )
```

**Snapshot Mode Comparison:**

| Mode | Latency Impact | Complexity | Use Case |
|------|---------------|------------|----------|
| `sync` | Higher | Low | Simple applications, lower throughput |
| `background` | Minimal | Medium | High-throughput production |
| `manual` | None | High | Full control over snapshot timing |

---

## Monitoring and Observability

### Health Check Endpoints

Implement health check endpoints for container orchestration:

```python
"""
Production health check implementation for FastAPI.
"""
import asyncio
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Response
from pydantic import BaseModel

from eventsource import InMemoryEventStore, InMemoryEventBus, InMemoryCheckpointRepository
from eventsource.subscriptions import SubscriptionManager


class HealthResponse(BaseModel):
    status: str
    details: dict[str, Any] | None = None


class ReadinessResponse(BaseModel):
    ready: bool
    reason: str


class LivenessResponse(BaseModel):
    alive: bool
    reason: str


# Global manager reference (initialized in lifespan)
manager: SubscriptionManager | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    global manager

    # Initialize infrastructure
    event_store = InMemoryEventStore()
    event_bus = InMemoryEventBus()
    checkpoint_repo = InMemoryCheckpointRepository()

    # Create subscription manager
    manager = SubscriptionManager(
        event_store=event_store,
        event_bus=event_bus,
        checkpoint_repo=checkpoint_repo,
        shutdown_timeout=30.0,
        drain_timeout=10.0,
    )

    # Start manager
    await manager.start()

    yield

    # Graceful shutdown
    await manager.stop()


app = FastAPI(lifespan=lifespan)


@app.get("/health", response_model=HealthResponse)
async def health_check():
    """
    Comprehensive health check endpoint.

    Returns overall application health status.
    """
    if manager is None:
        return Response(
            content='{"status": "unhealthy", "details": {"reason": "Manager not initialized"}}',
            status_code=503,
            media_type="application/json",
        )

    health = await manager.health_check()

    status_code = 200 if health.status in ("healthy", "degraded") else 503

    return Response(
        content=HealthResponse(
            status=health.status,
            details={
                "running": health.running,
                "subscription_count": health.subscription_count,
                "healthy_count": health.healthy_count,
                "degraded_count": health.degraded_count,
                "unhealthy_count": health.unhealthy_count,
                "total_events_processed": health.total_events_processed,
                "total_lag_events": health.total_lag_events,
                "uptime_seconds": health.uptime_seconds,
            },
        ).model_dump_json(),
        status_code=status_code,
        media_type="application/json",
    )


@app.get("/health/ready", response_model=ReadinessResponse)
async def readiness_check():
    """
    Kubernetes readiness probe endpoint.

    Returns whether the application is ready to receive traffic.
    Used by load balancers to route traffic.
    """
    if manager is None:
        return Response(
            content='{"ready": false, "reason": "Manager not initialized"}',
            status_code=503,
            media_type="application/json",
        )

    readiness = await manager.readiness_check()

    return Response(
        content=ReadinessResponse(
            ready=readiness.ready,
            reason=readiness.reason,
        ).model_dump_json(),
        status_code=200 if readiness.ready else 503,
        media_type="application/json",
    )


@app.get("/health/live", response_model=LivenessResponse)
async def liveness_check():
    """
    Kubernetes liveness probe endpoint.

    Returns whether the application is alive and responsive.
    Failed liveness causes container restart.
    """
    if manager is None:
        return Response(
            content='{"alive": false, "reason": "Manager not initialized"}',
            status_code=503,
            media_type="application/json",
        )

    liveness = await manager.liveness_check()

    return Response(
        content=LivenessResponse(
            alive=liveness.alive,
            reason=liveness.reason,
        ).model_dump_json(),
        status_code=200 if liveness.alive else 503,
        media_type="application/json",
    )


@app.get("/health/subscriptions/{name}")
async def subscription_health(name: str):
    """Get health status for a specific subscription."""
    if manager is None:
        return Response(
            content='{"error": "Manager not initialized"}',
            status_code=503,
            media_type="application/json",
        )

    health = manager.get_subscription_health(name)

    if health is None:
        return Response(
            content='{"error": "Subscription not found"}',
            status_code=404,
            media_type="application/json",
        )

    return {
        "name": health.name,
        "status": health.status,
        "state": health.state,
        "events_processed": health.events_processed,
        "events_failed": health.events_failed,
        "lag_events": health.lag_events,
        "error_rate": health.error_rate,
    }
```

### Structured Logging Configuration

Configure structured JSON logging for log aggregation:

```python
"""
Production logging configuration with structured JSON output.
"""
import logging
import json
import sys
from datetime import datetime, UTC
from typing import Any


class JSONFormatter(logging.Formatter):
    """JSON formatter for structured logging."""

    def format(self, record: logging.LogRecord) -> str:
        log_data: dict[str, Any] = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        # Add extra fields
        for key, value in record.__dict__.items():
            if key not in (
                "name", "msg", "args", "created", "filename", "funcName",
                "levelname", "levelno", "lineno", "module", "msecs",
                "pathname", "process", "processName", "relativeCreated",
                "stack_info", "exc_info", "exc_text", "thread", "threadName",
                "message",
            ):
                log_data[key] = value

        return json.dumps(log_data)


def configure_production_logging(level: str = "INFO"):
    """Configure structured logging for production."""
    # Create root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, level.upper()))

    # Remove existing handlers
    root_logger.handlers.clear()

    # Create console handler with JSON formatter
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JSONFormatter())
    root_logger.addHandler(handler)

    # Configure library loggers
    logging.getLogger("eventsource").setLevel(logging.INFO)
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)

    return root_logger


# Usage
logger = configure_production_logging("INFO")

# Log with extra fields
logger.info(
    "Order processed",
    extra={
        "order_id": "12345",
        "customer_id": "cust-789",
        "total": 299.99,
        "event_type": "OrderCreated",
    },
)
```

### OpenTelemetry Integration

Enable distributed tracing with OpenTelemetry:

```python
"""
OpenTelemetry configuration for production tracing.
"""
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource, SERVICE_NAME, SERVICE_VERSION
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor


def configure_tracing(
    service_name: str,
    service_version: str,
    otlp_endpoint: str = "localhost:4317",
):
    """
    Configure OpenTelemetry tracing for production.

    Args:
        service_name: Name of your service
        service_version: Version of your service
        otlp_endpoint: OTLP collector endpoint
    """
    # Create resource with service info
    resource = Resource.create({
        SERVICE_NAME: service_name,
        SERVICE_VERSION: service_version,
    })

    # Create tracer provider
    provider = TracerProvider(resource=resource)

    # Configure OTLP exporter
    exporter = OTLPSpanExporter(
        endpoint=otlp_endpoint,
        insecure=True,  # Set False for production with TLS
    )

    # Add batch processor for efficient export
    processor = BatchSpanProcessor(exporter)
    provider.add_span_processor(processor)

    # Set as global provider
    trace.set_tracer_provider(provider)

    return provider


# Usage in application startup
def setup_observability():
    """Initialize all observability components."""
    import os

    # Configure tracing
    configure_tracing(
        service_name=os.getenv("SERVICE_NAME", "eventsource-app"),
        service_version=os.getenv("SERVICE_VERSION", "1.0.0"),
        otlp_endpoint=os.getenv("OTLP_ENDPOINT", "localhost:4317"),
    )

    # Configure logging
    configure_production_logging(
        level=os.getenv("LOG_LEVEL", "INFO"),
    )
```

### Error Monitoring and Alerting

Set up error callbacks for monitoring and alerting:

```python
"""
Error monitoring setup for production subscriptions.
"""
import logging
from typing import Callable, Awaitable

from eventsource.subscriptions import (
    SubscriptionManager,
    ErrorInfo,
    ErrorSeverity,
    ErrorCategory,
)

logger = logging.getLogger(__name__)


async def log_all_errors(error_info: ErrorInfo) -> None:
    """Log all subscription errors with context."""
    logger.error(
        "Subscription error",
        extra={
            "subscription_name": error_info.subscription_name,
            "event_id": str(error_info.event_id),
            "event_type": error_info.event_type,
            "error_type": error_info.error_type,
            "error_message": error_info.error_message,
            "severity": error_info.classification.severity.value,
            "category": error_info.classification.category.value,
            "retry_count": error_info.retry_count,
        },
    )


async def alert_critical_errors(error_info: ErrorInfo) -> None:
    """Send alerts for critical errors."""
    # Integration point for PagerDuty, OpsGenie, Slack, etc.
    logger.critical(
        "CRITICAL: Subscription error requires immediate attention",
        extra={
            "subscription_name": error_info.subscription_name,
            "error_message": error_info.error_message,
            "alert_destination": "pagerduty",
        },
    )

    # Example: Send to PagerDuty
    # await pagerduty_client.create_incident(
    #     title=f"Critical error in {error_info.subscription_name}",
    #     details=error_info.to_dict(),
    # )


async def track_transient_errors(error_info: ErrorInfo) -> None:
    """Track transient errors for retry monitoring."""
    logger.warning(
        "Transient error detected",
        extra={
            "subscription_name": error_info.subscription_name,
            "event_id": str(error_info.event_id),
            "retry_count": error_info.retry_count,
            "will_retry": error_info.retry_count < 3,
        },
    )


def configure_error_monitoring(manager: SubscriptionManager) -> None:
    """Configure comprehensive error monitoring for a subscription manager."""
    # Log all errors
    manager.on_error(log_all_errors)

    # Alert on critical errors
    manager.on_error_severity(ErrorSeverity.CRITICAL, alert_critical_errors)

    # Track transient errors for patterns
    manager.on_error_category(ErrorCategory.TRANSIENT, track_transient_errors)

    logger.info("Error monitoring configured")
```

---

## Scaling Strategies

### Horizontal Scaling for Projections

Event-sourced projections can be scaled horizontally with partition-based assignment:

```python
"""
Horizontal scaling strategies for projections.
"""
from typing import List
import hashlib


def get_partition_for_aggregate(aggregate_id: str, num_partitions: int) -> int:
    """
    Determine which partition should handle an aggregate.

    Uses consistent hashing to ensure the same aggregate always
    goes to the same partition.
    """
    hash_value = int(hashlib.sha256(aggregate_id.encode()).hexdigest(), 16)
    return hash_value % num_partitions


def filter_events_for_partition(
    events: List,
    partition_id: int,
    num_partitions: int,
) -> List:
    """
    Filter events to only those belonging to this partition.

    Use this in scaled projection workers to process only
    their assigned events.
    """
    return [
        event for event in events
        if get_partition_for_aggregate(
            str(event.aggregate_id),
            num_partitions,
        ) == partition_id
    ]


class PartitionedProjection:
    """
    Base class for projections that run in partitioned mode.

    Each instance handles a subset of aggregates based on
    consistent hashing of aggregate IDs.
    """

    def __init__(self, partition_id: int, num_partitions: int):
        self.partition_id = partition_id
        self.num_partitions = num_partitions

    def should_handle(self, aggregate_id: str) -> bool:
        """Check if this partition should handle the given aggregate."""
        return (
            get_partition_for_aggregate(aggregate_id, self.num_partitions)
            == self.partition_id
        )

    async def handle_event(self, event) -> None:
        """Process event if it belongs to this partition."""
        if self.should_handle(str(event.aggregate_id)):
            await self._process_event(event)

    async def _process_event(self, event) -> None:
        """Override in subclass to implement event processing."""
        raise NotImplementedError
```

### Database Connection Scaling

Configure connection management for scaled deployments:

```python
"""
Database connection management for scaled deployments.
"""
import os
from dataclasses import dataclass


@dataclass
class ConnectionConfig:
    """Database connection configuration."""
    host: str
    port: int
    database: str
    user: str
    password: str
    pool_size: int
    max_overflow: int

    @classmethod
    def from_environment(cls) -> "ConnectionConfig":
        """Load configuration from environment variables."""
        # Get instance count for pool sizing
        instance_count = int(os.getenv("INSTANCE_COUNT", "1"))
        max_db_connections = int(os.getenv("MAX_DB_CONNECTIONS", "100"))

        # Calculate per-instance pool size
        connections_per_instance = max_db_connections // instance_count
        pool_size = max(2, connections_per_instance // 2)
        max_overflow = connections_per_instance - pool_size

        return cls(
            host=os.getenv("DB_HOST", "localhost"),
            port=int(os.getenv("DB_PORT", "5432")),
            database=os.getenv("DB_NAME", "eventsource"),
            user=os.getenv("DB_USER", "postgres"),
            password=os.getenv("DB_PASSWORD", ""),
            pool_size=pool_size,
            max_overflow=max_overflow,
        )

    @property
    def url(self) -> str:
        """Get SQLAlchemy connection URL."""
        return (
            f"postgresql+asyncpg://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )


def log_connection_config(config: ConnectionConfig) -> None:
    """Log connection configuration for debugging."""
    import logging
    logger = logging.getLogger(__name__)

    logger.info(
        "Database connection configured",
        extra={
            "host": config.host,
            "port": config.port,
            "database": config.database,
            "pool_size": config.pool_size,
            "max_overflow": config.max_overflow,
            "total_connections": config.pool_size + config.max_overflow,
        },
    )
```

### Event Store Partitioning

For very high event volumes, implement table partitioning:

```sql
-- Partitioned events table by time range
-- Create monthly partitions automatically

CREATE TABLE events (
    event_id UUID NOT NULL,
    aggregate_id UUID NOT NULL,
    aggregate_type VARCHAR(255) NOT NULL,
    event_type VARCHAR(255) NOT NULL,
    tenant_id UUID,
    actor_id VARCHAR(255),
    version INTEGER NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_events PRIMARY KEY (event_id, created_at)
) PARTITION BY RANGE (created_at);

-- Create partition for current month
CREATE TABLE events_2024_01 PARTITION OF events
    FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');

-- Create partition for next month
CREATE TABLE events_2024_02 PARTITION OF events
    FOR VALUES FROM ('2024-02-01') TO ('2024-03-01');

-- Automate partition creation with pg_partman extension
-- or create partitions via scheduled job

-- Create indexes on partitions (inherited from parent)
CREATE INDEX idx_events_aggregate_id ON events (aggregate_id);
CREATE INDEX idx_events_aggregate_type ON events (aggregate_type);
CREATE INDEX idx_events_timestamp ON events (timestamp);
```

---

## Backup and Recovery

### Event Store Backup Strategy

Events are the source of truth - protect them accordingly:

```python
"""
Backup and recovery utilities for event stores.
"""
import asyncio
import json
from datetime import datetime, UTC
from pathlib import Path
from typing import AsyncIterator
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


async def export_events_to_jsonl(
    session: AsyncSession,
    output_path: Path,
    batch_size: int = 1000,
) -> int:
    """
    Export all events to JSONL format for backup.

    Args:
        session: Database session
        output_path: Path to output file
        batch_size: Number of events per batch

    Returns:
        Total number of events exported
    """
    total_exported = 0
    offset = 0

    with open(output_path, "w") as f:
        while True:
            result = await session.execute(
                text("""
                    SELECT
                        event_id::text,
                        aggregate_id::text,
                        aggregate_type,
                        event_type,
                        tenant_id::text,
                        actor_id,
                        version,
                        timestamp,
                        payload,
                        created_at
                    FROM events
                    ORDER BY created_at, event_id
                    LIMIT :limit OFFSET :offset
                """),
                {"limit": batch_size, "offset": offset},
            )

            rows = result.fetchall()
            if not rows:
                break

            for row in rows:
                event_data = {
                    "event_id": row[0],
                    "aggregate_id": row[1],
                    "aggregate_type": row[2],
                    "event_type": row[3],
                    "tenant_id": row[4],
                    "actor_id": row[5],
                    "version": row[6],
                    "timestamp": row[7].isoformat(),
                    "payload": row[8],
                    "created_at": row[9].isoformat(),
                }
                f.write(json.dumps(event_data) + "\n")
                total_exported += 1

            offset += batch_size

    return total_exported


async def import_events_from_jsonl(
    session: AsyncSession,
    input_path: Path,
    batch_size: int = 1000,
) -> int:
    """
    Import events from JSONL backup.

    Args:
        session: Database session
        input_path: Path to input file
        batch_size: Number of events per transaction

    Returns:
        Total number of events imported
    """
    total_imported = 0
    batch = []

    with open(input_path, "r") as f:
        for line in f:
            event_data = json.loads(line.strip())
            batch.append(event_data)

            if len(batch) >= batch_size:
                await _insert_event_batch(session, batch)
                total_imported += len(batch)
                batch = []

        # Insert remaining events
        if batch:
            await _insert_event_batch(session, batch)
            total_imported += len(batch)

    return total_imported


async def _insert_event_batch(
    session: AsyncSession,
    batch: list,
) -> None:
    """Insert a batch of events."""
    for event_data in batch:
        await session.execute(
            text("""
                INSERT INTO events (
                    event_id, aggregate_id, aggregate_type, event_type,
                    tenant_id, actor_id, version, timestamp, payload, created_at
                ) VALUES (
                    :event_id::uuid, :aggregate_id::uuid, :aggregate_type,
                    :event_type, :tenant_id::uuid, :actor_id, :version,
                    :timestamp::timestamptz, :payload::jsonb, :created_at::timestamptz
                )
                ON CONFLICT (event_id) DO NOTHING
            """),
            event_data,
        )
    await session.commit()
```

### Point-in-Time Recovery Configuration

Configure PostgreSQL for point-in-time recovery:

```bash
# postgresql.conf settings for PITR

# Enable WAL archiving
archive_mode = on
archive_command = 'cp %p /path/to/archive/%f'

# Set appropriate WAL level
wal_level = replica

# Keep enough WAL for recovery window
wal_keep_size = 1GB

# Configure checkpoint behavior
checkpoint_timeout = 15min
max_wal_size = 4GB
min_wal_size = 1GB
```

### Projection Rebuild Capability

Projections can be rebuilt from events at any time:

```python
"""
Projection rebuild utilities.
"""
import asyncio
import logging
from typing import TypeVar

from eventsource import DeclarativeProjection
from eventsource.subscriptions import SubscriptionManager, SubscriptionConfig

logger = logging.getLogger(__name__)

TProjection = TypeVar("TProjection", bound=DeclarativeProjection)


async def rebuild_projection(
    manager: SubscriptionManager,
    projection: TProjection,
    name: str,
) -> TProjection:
    """
    Rebuild a projection from the beginning of the event stream.

    Args:
        manager: Subscription manager
        projection: Projection instance to rebuild
        name: Subscription name

    Returns:
        The rebuilt projection
    """
    logger.info(f"Starting projection rebuild: {name}")

    # Truncate existing read models
    await projection._truncate_read_models()
    logger.info(f"Truncated read models for: {name}")

    # Subscribe with start_from="beginning" to replay all events
    await manager.subscribe(
        projection,
        name=name,
        config=SubscriptionConfig(
            start_from="beginning",
            batch_size=500,  # Larger batches for rebuild
            checkpoint_strategy="every_batch",
        ),
    )

    # Start processing
    await manager.start(subscription_names=[name])

    # Wait for catch-up to complete
    while True:
        health = manager.get_subscription_health(name)
        if health and health.lag_events == 0:
            break
        await asyncio.sleep(1.0)

    logger.info(f"Projection rebuild complete: {name}")
    return projection
```

---

## Security Considerations

### Connection Security

Always use TLS for database connections in production:

```python
"""
Secure database connection configuration.
"""
from sqlalchemy.ext.asyncio import create_async_engine


def create_secure_engine(
    host: str,
    port: int,
    database: str,
    user: str,
    password: str,
    ssl_mode: str = "require",
    ssl_root_cert: str | None = None,
):
    """
    Create a secure database engine with TLS.

    Args:
        host: Database host
        port: Database port
        database: Database name
        user: Database user
        password: Database password
        ssl_mode: SSL mode (require, verify-ca, verify-full)
        ssl_root_cert: Path to CA certificate for verification
    """
    # Build connection URL
    url = f"postgresql+asyncpg://{user}:{password}@{host}:{port}/{database}"

    # SSL configuration
    connect_args = {
        "ssl": ssl_mode,
    }

    if ssl_root_cert:
        connect_args["ssl"] = {
            "ca": ssl_root_cert,
        }

    return create_async_engine(
        url,
        connect_args=connect_args,
        pool_pre_ping=True,
    )
```

### Event Data Protection

Protect sensitive data in events:

```python
"""
Event data protection utilities.
"""
import base64
import hashlib
from cryptography.fernet import Fernet
from typing import Any


class EventDataProtector:
    """
    Utilities for protecting sensitive event data.

    Use encryption for data that must be recoverable,
    hashing for data that only needs verification.
    """

    def __init__(self, encryption_key: bytes):
        """
        Initialize with encryption key.

        Generate key with: Fernet.generate_key()
        """
        self.fernet = Fernet(encryption_key)

    def encrypt_field(self, value: str) -> str:
        """Encrypt a field value."""
        return self.fernet.encrypt(value.encode()).decode()

    def decrypt_field(self, encrypted: str) -> str:
        """Decrypt a field value."""
        return self.fernet.decrypt(encrypted.encode()).decode()

    @staticmethod
    def hash_field(value: str, salt: str = "") -> str:
        """
        Create a one-way hash of a field value.

        Use for PII that doesn't need to be recovered
        but may need to be verified.
        """
        salted = f"{salt}{value}"
        return hashlib.sha256(salted.encode()).hexdigest()

    @staticmethod
    def mask_field(value: str, visible_chars: int = 4) -> str:
        """
        Mask a field value for display.

        Example: "1234567890" -> "******7890"
        """
        if len(value) <= visible_chars:
            return "*" * len(value)
        masked_len = len(value) - visible_chars
        return "*" * masked_len + value[-visible_chars:]


# Usage example
class SecureOrderCreated:
    """Example of an event with protected fields."""

    def __init__(
        self,
        order_id: str,
        customer_email: str,
        credit_card_last_four: str,
        protector: EventDataProtector,
    ):
        self.order_id = order_id
        # Hash email for verification without storing PII
        self.customer_email_hash = protector.hash_field(customer_email)
        # Store only last 4 digits
        self.credit_card_last_four = credit_card_last_four
```

### Audit Logging

Implement comprehensive audit logging:

```python
"""
Audit logging for event-sourced systems.
"""
import logging
from datetime import datetime, UTC
from typing import Any
from uuid import UUID


class AuditLogger:
    """
    Audit logger for tracking access and modifications.
    """

    def __init__(self, logger_name: str = "audit"):
        self.logger = logging.getLogger(logger_name)

    def log_event_access(
        self,
        actor_id: str,
        aggregate_id: UUID,
        aggregate_type: str,
        action: str,
        ip_address: str | None = None,
        user_agent: str | None = None,
    ) -> None:
        """Log access to event data."""
        self.logger.info(
            "Event data access",
            extra={
                "audit_type": "access",
                "timestamp": datetime.now(UTC).isoformat(),
                "actor_id": actor_id,
                "aggregate_id": str(aggregate_id),
                "aggregate_type": aggregate_type,
                "action": action,
                "ip_address": ip_address,
                "user_agent": user_agent,
            },
        )

    def log_event_modification(
        self,
        actor_id: str,
        event_id: UUID,
        event_type: str,
        aggregate_id: UUID,
        aggregate_type: str,
        changes: dict[str, Any],
    ) -> None:
        """Log modifications to event data (should be rare/never)."""
        self.logger.warning(
            "Event data modification",
            extra={
                "audit_type": "modification",
                "timestamp": datetime.now(UTC).isoformat(),
                "actor_id": actor_id,
                "event_id": str(event_id),
                "event_type": event_type,
                "aggregate_id": str(aggregate_id),
                "aggregate_type": aggregate_type,
                "changes": changes,
            },
        )

    def log_admin_action(
        self,
        actor_id: str,
        action: str,
        target: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Log administrative actions."""
        self.logger.info(
            "Administrative action",
            extra={
                "audit_type": "admin",
                "timestamp": datetime.now(UTC).isoformat(),
                "actor_id": actor_id,
                "action": action,
                "target": target,
                "details": details or {},
            },
        )


# Usage
audit = AuditLogger()

# Log event access
audit.log_event_access(
    actor_id="user-123",
    aggregate_id=UUID("..."),
    aggregate_type="Order",
    action="read",
    ip_address="192.168.1.1",
)
```

---

## Complete Production Example

Here is a complete production-ready application setup:

```python
"""
Tutorial 15: Production Deployment Example

A complete production-ready event-sourced application setup.
Run with: python tutorial_15_production.py
"""
import asyncio
import logging
import os
import signal
from contextlib import asynccontextmanager
from datetime import datetime, UTC
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession

from eventsource import (
    DomainEvent,
    register_event,
    AggregateRoot,
    PostgreSQLEventStore,
    AggregateRepository,
    InMemoryEventBus,
)
from eventsource import PostgreSQLCheckpointRepository
from eventsource.subscriptions import (
    SubscriptionManager,
    SubscriptionConfig,
    CheckpointStrategy,
    HealthCheckConfig,
    ErrorInfo,
    ErrorSeverity,
)
from eventsource.snapshots import PostgreSQLSnapshotStore


# =============================================================================
# Configuration
# =============================================================================

class AppConfig:
    """Application configuration from environment."""

    def __init__(self):
        self.database_url = os.getenv(
            "DATABASE_URL",
            "postgresql+asyncpg://postgres:postgres@localhost:5432/eventsource_prod"
        )
        self.pool_size = int(os.getenv("DB_POOL_SIZE", "10"))
        self.max_overflow = int(os.getenv("DB_MAX_OVERFLOW", "15"))
        self.log_level = os.getenv("LOG_LEVEL", "INFO")
        self.shutdown_timeout = float(os.getenv("SHUTDOWN_TIMEOUT", "30"))
        self.drain_timeout = float(os.getenv("DRAIN_TIMEOUT", "10"))


# =============================================================================
# Events
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


# =============================================================================
# Aggregate
# =============================================================================

class OrderState(BaseModel):
    order_id: str
    customer_id: str | None = None
    total: float = 0.0
    status: str = "draft"
    tracking_number: str | None = None


class OrderAggregate(AggregateRoot[OrderState]):
    aggregate_type = "Order"

    def _get_initial_state(self) -> OrderState:
        return OrderState(order_id=str(self.aggregate_id))

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, OrderCreated):
            self._state = OrderState(
                order_id=str(self.aggregate_id),
                customer_id=event.customer_id,
                total=event.total,
                status="created",
            )
        elif isinstance(event, OrderShipped):
            if self._state:
                self._state = self._state.model_copy(update={
                    "status": "shipped",
                    "tracking_number": event.tracking_number,
                })

    def create(self, customer_id: str, total: float) -> None:
        if self.version > 0:
            raise ValueError("Order already exists")
        self.apply_event(OrderCreated(
            aggregate_id=self.aggregate_id,
            customer_id=customer_id,
            total=total,
            aggregate_version=self.get_next_version(),
        ))

    def ship(self, tracking_number: str) -> None:
        if not self.state or self.state.status != "created":
            raise ValueError("Cannot ship order in current state")
        self.apply_event(OrderShipped(
            aggregate_id=self.aggregate_id,
            tracking_number=tracking_number,
            aggregate_version=self.get_next_version(),
        ))


# =============================================================================
# Logging Setup
# =============================================================================

def setup_logging(level: str = "INFO") -> logging.Logger:
    """Configure production logging."""
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format='{"timestamp":"%(asctime)s","level":"%(levelname)s","logger":"%(name)s","message":"%(message)s"}',
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )

    # Reduce noise from libraries
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)

    return logging.getLogger("eventsource.app")


# =============================================================================
# Error Handling
# =============================================================================

async def handle_error(error_info: ErrorInfo) -> None:
    """Handle subscription errors."""
    logger = logging.getLogger("eventsource.errors")
    logger.error(
        f"Subscription error: {error_info.error_message}",
        extra={
            "subscription": error_info.subscription_name,
            "event_id": str(error_info.event_id),
            "event_type": error_info.event_type,
            "severity": error_info.classification.severity.value,
        },
    )


async def handle_critical_error(error_info: ErrorInfo) -> None:
    """Handle critical errors with alerting."""
    logger = logging.getLogger("eventsource.alerts")
    logger.critical(
        f"CRITICAL: {error_info.error_message} in {error_info.subscription_name}"
    )
    # Integration point for alerting system


# =============================================================================
# Application Factory
# =============================================================================

class ProductionApp:
    """Production application with all components."""

    def __init__(self, config: AppConfig):
        self.config = config
        self.logger = setup_logging(config.log_level)
        self.engine = None
        self.session_factory = None
        self.event_store = None
        self.event_bus = None
        self.checkpoint_repo = None
        self.snapshot_store = None
        self.manager = None
        self.repo = None

    async def initialize(self) -> None:
        """Initialize all components."""
        self.logger.info("Initializing application...")

        # Create database engine
        self.engine = create_async_engine(
            self.config.database_url,
            pool_size=self.config.pool_size,
            max_overflow=self.config.max_overflow,
            pool_timeout=30,
            pool_recycle=1800,
            pool_pre_ping=True,
        )

        self.session_factory = async_sessionmaker(
            self.engine,
            expire_on_commit=False,
            class_=AsyncSession,
        )

        # Create event store
        self.event_store = PostgreSQLEventStore(
            self.session_factory,
            enable_tracing=True,
        )

        # Create event bus
        self.event_bus = InMemoryEventBus()

        # Create checkpoint repository
        self.checkpoint_repo = PostgreSQLCheckpointRepository(
            self.session_factory,
        )

        # Create snapshot store
        self.snapshot_store = PostgreSQLSnapshotStore(
            self.session_factory,
        )

        # Create subscription manager
        health_config = HealthCheckConfig(
            max_error_rate_per_minute=10.0,
            max_errors_warning=10,
            max_errors_critical=100,
            max_lag_events_warning=1000,
            max_lag_events_critical=10000,
        )

        self.manager = SubscriptionManager(
            event_store=self.event_store,
            event_bus=self.event_bus,
            checkpoint_repo=self.checkpoint_repo,
            shutdown_timeout=self.config.shutdown_timeout,
            drain_timeout=self.config.drain_timeout,
            health_check_config=health_config,
            enable_tracing=True,
        )

        # Configure error handling
        self.manager.on_error(handle_error)
        self.manager.on_error_severity(ErrorSeverity.CRITICAL, handle_critical_error)

        # Create repository
        self.repo = AggregateRepository(
            event_store=self.event_store,
            aggregate_factory=OrderAggregate,
            aggregate_type="Order",
            event_publisher=self.event_bus,
            snapshot_store=self.snapshot_store,
            snapshot_threshold=100,
            snapshot_mode="background",
            enable_tracing=True,
        )

        self.logger.info("Application initialized successfully")

    async def start(self) -> None:
        """Start the application."""
        self.logger.info("Starting application...")
        await self.manager.start()
        self.logger.info("Application started")

    async def stop(self) -> None:
        """Stop the application gracefully."""
        self.logger.info("Stopping application...")

        if self.manager:
            result = await self.manager.stop()
            self.logger.info(
                f"Manager stopped: phase={result.phase.value}, "
                f"duration={result.duration_seconds:.2f}s, "
                f"events_drained={result.events_drained}"
            )

        if self.engine:
            await self.engine.dispose()
            self.logger.info("Database connections closed")

        self.logger.info("Application stopped")

    async def health_check(self) -> dict[str, Any]:
        """Get application health status."""
        if not self.manager:
            return {"status": "unhealthy", "reason": "Not initialized"}

        health = await self.manager.health_check()
        return {
            "status": health.status,
            "running": health.running,
            "subscription_count": health.subscription_count,
            "healthy_count": health.healthy_count,
            "total_events_processed": health.total_events_processed,
            "uptime_seconds": health.uptime_seconds,
        }


# =============================================================================
# Main Entry Point
# =============================================================================

async def main():
    """Production application entry point."""
    config = AppConfig()
    app = ProductionApp(config)

    # Handle shutdown signals
    shutdown_event = asyncio.Event()

    def signal_handler(signum, frame):
        app.logger.info(f"Received signal {signum}, initiating shutdown...")
        shutdown_event.set()

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    try:
        await app.initialize()
        await app.start()

        # Demo: Create and process an order
        order_id = uuid4()
        order = app.repo.create_new(order_id)
        order.create(customer_id="cust-production", total=999.99)
        await app.repo.save(order)
        app.logger.info(f"Created order: {order_id}")

        # Check health
        health = await app.health_check()
        app.logger.info(f"Health status: {health['status']}")

        # Wait for shutdown signal
        app.logger.info("Application running. Press Ctrl+C to stop.")
        await shutdown_event.wait()

    finally:
        await app.stop()


if __name__ == "__main__":
    asyncio.run(main())
```

Save this as `tutorial_15_production.py` and run it:

```bash
# Set environment variables
export DATABASE_URL="postgresql+asyncpg://postgres:postgres@localhost:5432/eventsource_prod"
export LOG_LEVEL="INFO"

# Run the application
python tutorial_15_production.py
```

---

## Exercises

### Exercise 1: Production Deployment Checklist

**Objective:** Create a production deployment verification script.

**Time:** 30-40 minutes

**Requirements:**

1. Create a script that verifies all production checklist items
2. Check database connectivity and schema
3. Verify health check endpoints work
4. Confirm graceful shutdown behavior
5. Output a deployment readiness report

**Starter Code:**

```python
"""
Tutorial 15 - Exercise 1: Production Deployment Checklist

Your task: Create a verification script that checks all
production deployment requirements.
"""
import asyncio
import sys
from dataclasses import dataclass
from typing import List, Tuple


@dataclass
class CheckResult:
    name: str
    passed: bool
    message: str


class DeploymentChecker:
    """Verify production deployment requirements."""

    def __init__(self, database_url: str):
        self.database_url = database_url
        self.results: List[CheckResult] = []

    async def check_database_connectivity(self) -> CheckResult:
        """TODO: Verify database connection."""
        pass

    async def check_schema_exists(self) -> CheckResult:
        """TODO: Verify events table and indexes exist."""
        pass

    async def check_health_endpoints(self) -> CheckResult:
        """TODO: Verify health check methods work."""
        pass

    async def check_graceful_shutdown(self) -> CheckResult:
        """TODO: Verify graceful shutdown completes."""
        pass

    async def run_all_checks(self) -> List[CheckResult]:
        """Run all deployment checks."""
        checks = [
            self.check_database_connectivity,
            self.check_schema_exists,
            self.check_health_endpoints,
            self.check_graceful_shutdown,
        ]

        for check in checks:
            result = await check()
            self.results.append(result)

        return self.results

    def print_report(self) -> None:
        """Print deployment readiness report."""
        print("\n" + "=" * 60)
        print("PRODUCTION DEPLOYMENT CHECKLIST")
        print("=" * 60 + "\n")

        passed = 0
        failed = 0

        for result in self.results:
            status = "PASS" if result.passed else "FAIL"
            icon = "[+]" if result.passed else "[-]"
            print(f"{icon} {result.name}: {status}")
            print(f"    {result.message}\n")

            if result.passed:
                passed += 1
            else:
                failed += 1

        print("=" * 60)
        print(f"Results: {passed} passed, {failed} failed")
        print("=" * 60)

        if failed > 0:
            print("\nDEPLOYMENT NOT READY - Fix failures before deploying")
        else:
            print("\nDEPLOYMENT READY - All checks passed")


async def main():
    database_url = "postgresql+asyncpg://postgres:postgres@localhost:5432/eventsource_prod"

    checker = DeploymentChecker(database_url)
    await checker.run_all_checks()
    checker.print_report()


if __name__ == "__main__":
    asyncio.run(main())
```

**Hints:**

- Use `create_async_engine` with `pool_pre_ping=True` for connectivity check
- Query `information_schema.tables` to verify schema
- Create a minimal `SubscriptionManager` to test health checks
- Use `manager.stop()` with a short timeout to test shutdown

---

## Phase 3 Complete: Production Readiness Achieved

Congratulations on completing Phase 3 of the eventsource-py tutorial series!

You have now mastered the production readiness skills needed to deploy event-sourced applications:

### Phase 3 Accomplishments

| Tutorial | Topic | Key Skills |
|----------|-------|------------|
| 11 | PostgreSQL | Production database configuration |
| 12 | SQLite | Development and testing backends |
| 13 | Subscriptions | Event subscription management |
| 14 | Snapshotting | Performance optimization |
| **15** | **Production** | **Complete deployment guide** |

### What You Can Now Build

With Phase 3 complete, you can confidently:

- Deploy event-sourced applications to production environments
- Configure appropriate performance tuning for your workload
- Implement comprehensive monitoring and observability
- Design scalable architectures for growth
- Plan and execute backup and recovery procedures
- Apply security best practices

### Next Phase: Advanced Patterns

Phase 4 introduces advanced patterns and techniques:

- **Tutorial 16**: Multi-tenancy for SaaS applications
- **Tutorial 17**: Process managers for complex workflows
- **Tutorial 18**: CQRS patterns and optimizations
- **Tutorial 19**: Testing strategies for event sourcing
- **Tutorial 20**: Migration strategies
- **Tutorial 21**: Troubleshooting guide

---

## Summary

In this tutorial, you learned:

- **Production Checklist**: Comprehensive verification for deployment readiness
- **Performance Tuning**: Connection pools, batching, and snapshot configuration
- **Monitoring**: Health checks, structured logging, and OpenTelemetry integration
- **Scaling**: Horizontal projection scaling and database partitioning
- **Backup/Recovery**: Event export, PITR, and projection rebuilds
- **Security**: TLS connections, data protection, and audit logging

---

## Key Takeaways

!!! note "Remember"
    - Complete the production checklist before every deployment
    - Configure connection pools based on your workload and instance count
    - Implement health checks for container orchestration
    - Enable structured logging for production observability

!!! tip "Best Practice"
    Use the `background` snapshot mode for high-throughput applications to minimize latency impact while maintaining the performance benefits of snapshotting.

!!! warning "Common Mistake"
    Do not forget to configure `pool_pre_ping=True` for database connections. Without this, your application may use stale connections that have been terminated by the database or network.

---

## Next Steps

Continue to [Tutorial 16: Multi-Tenancy](16-multi-tenancy.md) to learn how to build multi-tenant event-sourced applications for SaaS platforms.

---

## Related Documentation

- [Production Deployment Guide](../guides/production.md) - Reference guide for production
- [Kubernetes Deployment Guide](../guides/kubernetes-deployment.md) - Container orchestration
- [Tutorial 13: Subscriptions](13-subscriptions.md) - Subscription management
- [Tutorial 14: Snapshotting](14-snapshotting.md) - Performance optimization
