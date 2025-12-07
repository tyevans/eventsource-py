# Production Deployment Guide

This guide covers deploying eventsource-based applications to production, including database setup, configuration, monitoring, and operational best practices.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Database Setup](#database-setup)
3. [Connection Configuration](#connection-configuration)
4. [Event Store Setup](#event-store-setup)
5. [Performance Tuning](#performance-tuning)
6. [Monitoring and Observability](#monitoring-and-observability)
7. [Health Checks](#health-checks)
8. [High Availability](#high-availability)
9. [Backup and Recovery](#backup-and-recovery)
10. [Security Hardening](#security-hardening)
11. [Scaling Considerations](#scaling-considerations)
12. [Operational Procedures](#operational-procedures)
13. [Docker Compose for Production](#docker-compose-for-production)
14. [Troubleshooting](#troubleshooting)

---

## Prerequisites

### Software Requirements

| Component | Minimum Version | Recommended Version |
|-----------|-----------------|---------------------|
| Python | 3.11+ | 3.12+ |
| PostgreSQL | 12 | 14+ |
| Redis (optional) | 6 | 7+ |
| asyncpg | 0.27.0 | Latest |
| SQLAlchemy | 2.0 | Latest |

### Required Extensions

PostgreSQL must have the following extension enabled:

```sql
-- Required for UUID generation
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
```

Optional extensions for high-volume deployments:

```sql
-- Automatic partition management (high-volume)
CREATE EXTENSION IF NOT EXISTS pg_partman;

-- Scheduled maintenance tasks
CREATE EXTENSION IF NOT EXISTS pg_cron;
```

### Python Dependencies

Install the library with production dependencies:

```bash
# Basic installation
pip install eventsource

# With PostgreSQL support
pip install eventsource[postgresql]

# With Redis event bus
pip install eventsource[redis]

# With OpenTelemetry tracing
pip install eventsource[telemetry]

# All optional dependencies
pip install eventsource[all]
```

---

## Database Setup

### PostgreSQL Configuration

Recommended PostgreSQL settings for event sourcing workloads. Adjust based on available RAM and expected load.

**postgresql.conf recommendations:**

```ini
# Memory Settings (adjust based on available RAM)
shared_buffers = 256MB              # 25% of RAM, up to 8GB
effective_cache_size = 768MB        # 75% of RAM
work_mem = 16MB                     # Per-operation sort/hash memory
maintenance_work_mem = 128MB        # For VACUUM, CREATE INDEX

# Write-Ahead Log (WAL) - Critical for event sourcing
wal_level = replica                 # Enable replication
max_wal_size = 2GB                  # Allow larger transactions
min_wal_size = 80MB
wal_buffers = 16MB

# Checkpoints
checkpoint_completion_target = 0.9  # Spread checkpoint I/O

# Query Planner
random_page_cost = 1.1              # SSD optimization
effective_io_concurrency = 200      # SSD optimization

# Connections
max_connections = 200               # Adjust based on pool settings

# Logging (for production debugging)
log_min_duration_statement = 1000   # Log queries over 1 second
log_checkpoints = on
log_connections = off               # Reduce noise
log_disconnections = off
log_lock_waits = on                 # Important for concurrency debugging
```

### Schema Creation

The eventsource library requires four tables. Create them using the provided SQL:

```bash
# Using psql directly
psql -h localhost -U your_user -d your_database \
    -f src/eventsource/migrations/schemas/all.sql
```

Or programmatically in Python:

```python
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text

async def create_schema(database_url: str) -> None:
    """Create eventsource schema."""
    engine = create_async_engine(database_url)

    # Read schema SQL
    from pathlib import Path
    schema_path = Path(__file__).parent / "migrations/schemas/all.sql"
    schema_sql = schema_path.read_text()

    async with engine.begin() as conn:
        await conn.execute(text(schema_sql))

    await engine.dispose()
```

### Partitioned Events Table (High-Volume)

For deployments expecting more than 10 million events per month, use the partitioned events table:

```bash
# Use partitioned schema instead of standard
psql -h localhost -U your_user -d your_database \
    -f src/eventsource/migrations/templates/events_partitioned.sql
```

Create partitions for expected time ranges:

```sql
-- Create partitions for the year
SELECT create_events_partitions_for_year(2025);

-- Or create individual monthly partitions
SELECT create_events_partition(2025, 1);  -- January 2025
SELECT create_events_partition(2025, 2);  -- February 2025
```

For automatic partition management with pg_partman:

```sql
-- Enable pg_partman
CREATE EXTENSION IF NOT EXISTS pg_partman;

-- Configure automatic partition management
SELECT partman.create_parent(
    p_parent_table => 'public.events'::text,
    p_control => 'timestamp'::text,
    p_interval => '1 month'::text,
    p_type => 'range'::text,
    p_premake => 6,              -- Create 6 future partitions
    p_automatic_maintenance => 'on'::text
);

-- Configure retention (optional - detach old partitions)
UPDATE partman.part_config
SET
    infinite_time_partitions = true,
    retention = '24 months'::text,
    retention_keep_table = true  -- Keep tables for archival
WHERE parent_table = 'public.events';
```

### Index Recommendations

The schema includes optimized indexes for common access patterns. For specific workloads, consider additional indexes:

```sql
-- For heavy tenant-based queries
CREATE INDEX CONCURRENTLY idx_events_tenant_timestamp
    ON events (tenant_id, timestamp)
    WHERE tenant_id IS NOT NULL;

-- For correlation/causation tracking
CREATE INDEX CONCURRENTLY idx_events_correlation
    ON events ((payload->>'correlation_id'))
    WHERE payload->>'correlation_id' IS NOT NULL;

-- Partial index for recent events only (rolling window queries)
CREATE INDEX CONCURRENTLY idx_events_recent
    ON events (timestamp, event_type)
    WHERE timestamp > NOW() - INTERVAL '7 days';
```

---

## Connection Configuration

### SQLAlchemy Async Engine

Configure the async engine with production-appropriate settings:

```python
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
import os

def create_production_engine():
    """Create a production-configured async engine."""
    database_url = os.environ["DATABASE_URL"]

    engine = create_async_engine(
        database_url,

        # Connection Pool Settings
        pool_size=10,           # Base pool size (workers * 2-3)
        max_overflow=20,        # Additional connections under load
        pool_timeout=30,        # Seconds to wait for connection
        pool_recycle=1800,      # Recycle connections every 30 min
        pool_pre_ping=True,     # Verify connection before use

        # Echo SQL for debugging (disable in production)
        echo=False,

        # Execution options
        execution_options={
            "isolation_level": "READ COMMITTED",
        },
    )

    return engine


def create_session_factory(engine):
    """Create a session factory for the event store."""
    return async_sessionmaker(
        engine,
        expire_on_commit=False,  # Important for event sourcing
        autoflush=False,
    )
```

### Environment Variables

Configure the application using environment variables:

```bash
# Database Configuration
DATABASE_URL=postgresql+asyncpg://user:password@host:5432/database
DATABASE_POOL_SIZE=10
DATABASE_MAX_OVERFLOW=20
DATABASE_POOL_TIMEOUT=30
DATABASE_POOL_RECYCLE=1800

# Redis Configuration (for Redis event bus)
REDIS_URL=redis://localhost:6379/0
REDIS_MAX_CONNECTIONS=10

# Application Settings
LOG_LEVEL=INFO
ENABLE_TRACING=true

# Security
DATABASE_SSL_MODE=require
DATABASE_SSL_CA=/path/to/ca-certificate.crt
```

### Configuration Helper

```python
import os
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class DatabaseConfig:
    """Database configuration from environment."""

    url: str
    pool_size: int = 10
    max_overflow: int = 20
    pool_timeout: int = 30
    pool_recycle: int = 1800
    ssl_mode: Optional[str] = None
    ssl_ca: Optional[str] = None

    @classmethod
    def from_env(cls) -> "DatabaseConfig":
        """Load configuration from environment variables."""
        return cls(
            url=os.environ["DATABASE_URL"],
            pool_size=int(os.environ.get("DATABASE_POOL_SIZE", 10)),
            max_overflow=int(os.environ.get("DATABASE_MAX_OVERFLOW", 20)),
            pool_timeout=int(os.environ.get("DATABASE_POOL_TIMEOUT", 30)),
            pool_recycle=int(os.environ.get("DATABASE_POOL_RECYCLE", 1800)),
            ssl_mode=os.environ.get("DATABASE_SSL_MODE"),
            ssl_ca=os.environ.get("DATABASE_SSL_CA"),
        )


def create_engine_from_config(config: DatabaseConfig):
    """Create engine from configuration."""
    connect_args = {}

    if config.ssl_mode:
        import ssl
        ssl_context = ssl.create_default_context(cafile=config.ssl_ca)
        connect_args["ssl"] = ssl_context

    return create_async_engine(
        config.url,
        pool_size=config.pool_size,
        max_overflow=config.max_overflow,
        pool_timeout=config.pool_timeout,
        pool_recycle=config.pool_recycle,
        pool_pre_ping=True,
        connect_args=connect_args,
    )
```

---

## Event Store Setup

### Basic Setup

```python
from eventsource import PostgreSQLEventStore

# Create event store
engine = create_production_engine()
session_factory = create_session_factory(engine)

store = PostgreSQLEventStore(
    session_factory,
    outbox_enabled=True,      # Enable transactional outbox
    enable_tracing=True,      # Enable OpenTelemetry tracing
)
```

### With Custom Event Registry

For applications with many event types, use a custom registry:

```python
from eventsource import EventRegistry, PostgreSQLEventStore

# Create custom registry
registry = EventRegistry()

# Register all event types
from myapp.events import (
    OrderCreated,
    OrderShipped,
    OrderDelivered,
    PaymentReceived,
    # ... all your events
)

for event_class in [OrderCreated, OrderShipped, OrderDelivered, PaymentReceived]:
    registry.register(event_class)

# Create store with custom registry
store = PostgreSQLEventStore(
    session_factory,
    event_registry=registry,
    outbox_enabled=True,
)
```

---

## Performance Tuning

### Connection Pool Sizing

The optimal pool size depends on your workload:

```python
# Formula: pool_size = (core_count * 2) + spinning_disk_count
# For async applications, connections can be shared efficiently

# Light workload (few aggregates, low concurrency)
pool_size = 5
max_overflow = 10

# Medium workload (typical web application)
pool_size = 10
max_overflow = 20

# Heavy workload (high concurrency, many projections)
pool_size = 20
max_overflow = 40

# Rule of thumb: Don't exceed max_connections / workers
```

### Batch Sizes for Event Reading

Configure batch sizes for projection processing:

```python
from eventsource.projections import ProjectionCoordinator

coordinator = ProjectionCoordinator(
    registry=projection_registry,
    batch_size=100,             # Events per batch
    poll_interval_seconds=1.0,  # Polling frequency
)

# For high-throughput projections
coordinator = ProjectionCoordinator(
    registry=projection_registry,
    batch_size=500,             # Larger batches
    poll_interval_seconds=0.5,  # More frequent polling
)
```

### Projection Retry Configuration

Configure retry behavior for projections:

```python
from eventsource.projections import DeclarativeProjection


class HighThroughputProjection(DeclarativeProjection):
    """Projection optimized for high throughput."""

    # Fewer retries, faster failure
    MAX_RETRIES = 2
    RETRY_BACKOFF_BASE = 1  # 1s, 2s backoff

    # ...handlers...


class CriticalProjection(DeclarativeProjection):
    """Critical projection that retries more aggressively."""

    # More retries for critical data
    MAX_RETRIES = 5
    RETRY_BACKOFF_BASE = 2  # 2s, 4s, 8s, 16s, 32s backoff

    # ...handlers...
```

### PostgreSQL Query Optimization

Ensure queries use indexes effectively:

```sql
-- Check if aggregate loading uses index
EXPLAIN ANALYZE
SELECT * FROM events
WHERE aggregate_id = '550e8400-e29b-41d4-a716-446655440000'
  AND aggregate_type = 'Order'
ORDER BY version ASC;

-- Check projection query plan (with partition pruning)
EXPLAIN ANALYZE
SELECT * FROM events
WHERE event_type IN ('OrderCreated', 'OrderShipped')
  AND timestamp >= '2025-01-01'::timestamptz
ORDER BY timestamp ASC;
```

---

## Monitoring and Observability

### Key Metrics to Track

| Metric | Description | Alert Threshold |
|--------|-------------|-----------------|
| Events appended/sec | Event write throughput | Baseline dependent |
| Event append latency (p99) | Write performance | > 100ms |
| Projection lag (seconds) | Time behind event stream | > 60s warning, > 300s critical |
| DLQ depth | Failed events awaiting resolution | > 0 |
| Connection pool utilization | Pool saturation | > 80% |
| Optimistic lock conflicts/min | Concurrency contention | High rate indicates hotspots |

### Lag Monitoring

```python
from eventsource.projections.base import CheckpointTrackingProjection
import logging

logger = logging.getLogger(__name__)


async def monitor_projection_lag(
    projections: list[CheckpointTrackingProjection],
    warning_threshold_seconds: float = 60.0,
    critical_threshold_seconds: float = 300.0,
) -> dict[str, dict]:
    """Monitor projection lag and return health status."""
    results = {}

    for projection in projections:
        name = projection.projection_name
        metrics = await projection.get_lag_metrics()

        if metrics is None:
            results[name] = {
                "status": "unknown",
                "message": "No checkpoint found",
            }
            continue

        lag = metrics.get("lag_seconds", 0)

        if lag >= critical_threshold_seconds:
            status = "critical"
            logger.critical(
                "Projection %s critical lag: %.1f seconds",
                name, lag,
                extra={"projection": name, "lag_seconds": lag},
            )
        elif lag >= warning_threshold_seconds:
            status = "warning"
            logger.warning(
                "Projection %s elevated lag: %.1f seconds",
                name, lag,
                extra={"projection": name, "lag_seconds": lag},
            )
        else:
            status = "healthy"
            logger.debug(
                "Projection %s lag: %.1f seconds",
                name, lag,
            )

        results[name] = {
            "status": status,
            "lag_seconds": lag,
            "events_processed": metrics.get("events_processed", 0),
            "last_processed_at": metrics.get("last_processed_at"),
        }

    return results
```

### DLQ Monitoring

```python
from eventsource.repositories import PostgreSQLDLQRepository


async def check_dlq_health(
    dlq_repo: PostgreSQLDLQRepository,
) -> dict:
    """Check DLQ health and return statistics."""
    stats = await dlq_repo.get_failure_stats()

    # Alert if any failures exist
    if stats["total_failed"] > 0:
        logger.warning(
            "DLQ contains %d failed events across %d projections",
            stats["total_failed"],
            stats["affected_projections"],
            extra=stats,
        )

    # Get per-projection breakdown
    projection_stats = await dlq_repo.get_projection_failure_counts()

    return {
        "total_failed": stats["total_failed"],
        "total_retrying": stats["total_retrying"],
        "affected_projections": stats["affected_projections"],
        "oldest_failure": stats["oldest_failure"],
        "by_projection": projection_stats,
    }
```

### Logging Configuration

Configure structured logging for production:

```python
import logging
import json
from datetime import datetime


class JSONFormatter(logging.Formatter):
    """JSON formatter for structured logging."""

    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Add extra fields
        if hasattr(record, "projection"):
            log_data["projection"] = record.projection
        if hasattr(record, "event_id"):
            log_data["event_id"] = record.event_id
        if hasattr(record, "aggregate_id"):
            log_data["aggregate_id"] = record.aggregate_id
        if hasattr(record, "lag_seconds"):
            log_data["lag_seconds"] = record.lag_seconds

        # Add exception info
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_data)


def configure_logging(level: str = "INFO") -> None:
    """Configure production logging."""
    handler = logging.StreamHandler()
    handler.setFormatter(JSONFormatter())

    # Configure eventsource logger
    logger = logging.getLogger("eventsource")
    logger.setLevel(getattr(logging, level))
    logger.addHandler(handler)

    # Configure application logger
    app_logger = logging.getLogger("myapp")
    app_logger.setLevel(getattr(logging, level))
    app_logger.addHandler(handler)
```

### OpenTelemetry Integration

Enable distributed tracing:

```python
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

def configure_tracing(service_name: str, otlp_endpoint: str) -> None:
    """Configure OpenTelemetry tracing."""
    provider = TracerProvider()

    exporter = OTLPSpanExporter(endpoint=otlp_endpoint)
    processor = BatchSpanProcessor(exporter)
    provider.add_span_processor(processor)

    trace.set_tracer_provider(provider)


# The PostgreSQLEventStore will automatically emit traces when enabled
store = PostgreSQLEventStore(
    session_factory,
    enable_tracing=True,  # Traces append_events, get_events, etc.
)
```

---

## Health Checks

### Database Health Check

```python
from sqlalchemy import text


async def check_database_health(session_factory) -> dict:
    """Check database connectivity and basic health."""
    try:
        async with session_factory() as session:
            # Basic connectivity
            result = await session.execute(text("SELECT 1"))
            result.fetchone()

            # Check table existence
            tables_query = text("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_name IN ('events', 'projection_checkpoints', 'dead_letter_queue')
            """)
            result = await session.execute(tables_query)
            tables = [row[0] for row in result.fetchall()]

            if len(tables) < 3:
                return {
                    "status": "degraded",
                    "message": f"Missing tables: expected 3, found {len(tables)}",
                    "tables": tables,
                }

            return {
                "status": "healthy",
                "tables": tables,
            }

    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e),
        }
```

### Projection Health Check

```python
async def check_projection_health(
    projection: CheckpointTrackingProjection,
    max_lag_seconds: float = 60.0,
) -> dict:
    """Check individual projection health."""
    try:
        metrics = await projection.get_lag_metrics()

        if metrics is None:
            return {
                "projection": projection.projection_name,
                "status": "unknown",
                "message": "No checkpoint - projection may not have started",
            }

        lag = metrics.get("lag_seconds", 0)
        status = "healthy" if lag < max_lag_seconds else "degraded"

        return {
            "projection": projection.projection_name,
            "status": status,
            "lag_seconds": lag,
            "events_processed": metrics.get("events_processed"),
            "last_processed_at": metrics.get("last_processed_at"),
        }

    except Exception as e:
        return {
            "projection": projection.projection_name,
            "status": "unhealthy",
            "error": str(e),
        }
```

### Combined Health Endpoint

```python
from fastapi import FastAPI
from fastapi.responses import JSONResponse


app = FastAPI()


@app.get("/health")
async def health_check():
    """Combined health check endpoint."""
    db_health = await check_database_health(session_factory)
    dlq_health = await check_dlq_health(dlq_repo)

    projection_health = {}
    for projection in projections:
        projection_health[projection.projection_name] = await check_projection_health(
            projection
        )

    # Determine overall status
    all_healthy = (
        db_health["status"] == "healthy"
        and dlq_health["total_failed"] == 0
        and all(p["status"] == "healthy" for p in projection_health.values())
    )

    any_unhealthy = (
        db_health["status"] == "unhealthy"
        or any(p["status"] == "unhealthy" for p in projection_health.values())
    )

    if any_unhealthy:
        overall_status = "unhealthy"
        status_code = 503
    elif not all_healthy:
        overall_status = "degraded"
        status_code = 200
    else:
        overall_status = "healthy"
        status_code = 200

    return JSONResponse(
        status_code=status_code,
        content={
            "status": overall_status,
            "database": db_health,
            "dlq": dlq_health,
            "projections": projection_health,
        },
    )


@app.get("/ready")
async def readiness_check():
    """Kubernetes readiness probe."""
    db_health = await check_database_health(session_factory)

    if db_health["status"] == "unhealthy":
        return JSONResponse(
            status_code=503,
            content={"ready": False, "reason": "Database unavailable"},
        )

    return {"ready": True}


@app.get("/live")
async def liveness_check():
    """Kubernetes liveness probe."""
    return {"alive": True}
```

---

## High Availability

### Database Replication

For production deployments, use PostgreSQL streaming replication:

```
Primary (writes) -----> Standby 1 (reads)
                  \---> Standby 2 (reads)
```

**Primary PostgreSQL configuration (postgresql.conf):**

```ini
wal_level = replica
max_wal_senders = 5
max_replication_slots = 5
wal_keep_size = 1GB
```

**Application configuration for read replicas:**

```python
from sqlalchemy.ext.asyncio import create_async_engine

# Write engine (primary)
write_engine = create_async_engine(
    "postgresql+asyncpg://user:pass@primary:5432/db",
    pool_size=10,
)

# Read engine (replicas)
read_engine = create_async_engine(
    "postgresql+asyncpg://user:pass@replica:5432/db",
    pool_size=20,  # More connections for reads
)

# Event store uses write engine
store = PostgreSQLEventStore(create_session_factory(write_engine))

# Projections can query read replicas for some operations
# (but must write to primary)
```

### Projection Scaling

Multiple projection instances can process events in parallel using partitioning:

```python
class PartitionedProjection(DeclarativeProjection):
    """Projection that processes a subset of aggregates."""

    def __init__(
        self,
        partition_id: int,
        total_partitions: int,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._partition_id = partition_id
        self._total_partitions = total_partitions
        # Override projection name to include partition
        self._projection_name = f"{self.__class__.__name__}_{partition_id}"

    async def _process_event(self, event: DomainEvent) -> None:
        # Only process events for this partition
        event_partition = hash(str(event.aggregate_id)) % self._total_partitions
        if event_partition != self._partition_id:
            return  # Skip events for other partitions

        await super()._process_event(event)


# Deploy 4 instances, each handling 1/4 of aggregates
# Instance 0: partition_id=0, total_partitions=4
# Instance 1: partition_id=1, total_partitions=4
# etc.
```

### Redis Event Bus Consumer Groups

For Redis-based event distribution, use consumer groups:

```python
from eventsource.bus.redis import RedisEventBus, RedisEventBusConfig

config = RedisEventBusConfig(
    redis_url="redis://localhost:6379/0",
    consumer_group="my-projection-group",
    consumer_name="worker-1",  # Unique per instance
    max_pending_time_ms=60000,  # Claim pending messages after 60s
)

event_bus = RedisEventBus(config)

# Multiple workers can process events in parallel
# Redis ensures each event is delivered to only one consumer
```

---

## Backup and Recovery

### Event Store Backup

Events are immutable, making backup straightforward:

```bash
#!/bin/bash
# backup_events.sh - Daily event store backup

BACKUP_DIR="/backups/eventsource"
DATE=$(date +%Y%m%d_%H%M%S)
DATABASE_URL="postgresql://user:pass@host:5432/db"

# Create backup directory
mkdir -p "$BACKUP_DIR"

# Full backup with parallel jobs
pg_dump "$DATABASE_URL" \
    --format=directory \
    --jobs=4 \
    --compress=9 \
    --file="$BACKUP_DIR/backup_$DATE"

# Cleanup backups older than 30 days
find "$BACKUP_DIR" -type d -name "backup_*" -mtime +30 -exec rm -rf {} +

echo "Backup completed: $BACKUP_DIR/backup_$DATE"
```

### Point-in-Time Recovery

PostgreSQL WAL archiving enables point-in-time recovery:

**postgresql.conf:**

```ini
archive_mode = on
archive_command = 'cp %p /archive/%f'
```

**Recovery procedure:**

```bash
# 1. Stop PostgreSQL
systemctl stop postgresql

# 2. Restore base backup
rm -rf /var/lib/postgresql/data/*
pg_restore --dbname=postgres --create --jobs=4 /backups/base_backup

# 3. Configure recovery
cat > /var/lib/postgresql/data/recovery.conf << EOF
restore_command = 'cp /archive/%f %p'
recovery_target_time = '2025-01-15 14:30:00 UTC'
recovery_target_action = 'promote'
EOF

# 4. Start PostgreSQL (will replay WAL to target time)
systemctl start postgresql
```

### Projection Rebuilding

Projections can be rebuilt from events at any time:

```python
from eventsource import PostgreSQLEventStore
from eventsource.projections import ProjectionCoordinator


async def rebuild_projection(
    event_store: PostgreSQLEventStore,
    projection: DeclarativeProjection,
    batch_size: int = 1000,
) -> int:
    """Rebuild a projection from all events."""
    # 1. Reset projection (clears checkpoint and read model)
    await projection.reset()
    logger.warning("Reset projection %s", projection.projection_name)

    # 2. Replay all events
    event_count = 0
    async for stored_event in event_store.read_all():
        event = stored_event.event

        # Only process events this projection handles
        if type(event) in projection.subscribed_to():
            await projection.handle(event)
            event_count += 1

            if event_count % batch_size == 0:
                logger.info(
                    "Rebuild progress: %d events processed",
                    event_count,
                )

    logger.info(
        "Rebuild complete: %s processed %d events",
        projection.projection_name,
        event_count,
    )

    return event_count


async def rebuild_all_projections(
    event_store: PostgreSQLEventStore,
    coordinator: ProjectionCoordinator,
) -> dict[str, int]:
    """Rebuild all projections."""
    results = {}

    # Reset all projections first
    await coordinator.registry.reset_all()

    # Read all events once
    events = []
    async for stored_event in event_store.read_all():
        events.append(stored_event.event)

    # Replay to all projections
    await coordinator.rebuild_all(events)

    return {"total_events": len(events)}
```

---

## Security Hardening

### Database Credentials

**Never hardcode credentials. Use environment variables or secret managers:**

```python
import os
from dataclasses import dataclass


@dataclass
class SecureConfig:
    """Secure configuration loader."""

    @staticmethod
    def get_database_url() -> str:
        """Load database URL from secure source."""
        # Option 1: Environment variable
        if url := os.environ.get("DATABASE_URL"):
            return url

        # Option 2: AWS Secrets Manager
        if secret_arn := os.environ.get("DATABASE_SECRET_ARN"):
            import boto3
            client = boto3.client("secretsmanager")
            response = client.get_secret_value(SecretId=secret_arn)
            import json
            secret = json.loads(response["SecretString"])
            return f"postgresql+asyncpg://{secret['username']}:{secret['password']}@{secret['host']}:{secret['port']}/{secret['dbname']}"

        raise ValueError("No database credentials configured")
```

### Network Security

**Use TLS for database connections:**

```python
import ssl

def create_secure_engine():
    """Create engine with TLS encryption."""
    ssl_context = ssl.create_default_context(
        cafile=os.environ.get("DATABASE_SSL_CA")
    )
    ssl_context.check_hostname = True
    ssl_context.verify_mode = ssl.CERT_REQUIRED

    return create_async_engine(
        os.environ["DATABASE_URL"],
        connect_args={"ssl": ssl_context},
        pool_size=10,
    )
```

**PostgreSQL pg_hba.conf for restricted access:**

```
# TYPE  DATABASE        USER            ADDRESS                 METHOD
hostssl eventsource     app_user        10.0.0.0/8              scram-sha-256
hostssl eventsource     app_user        192.168.0.0/16          scram-sha-256
host    all             all             0.0.0.0/0               reject
```

### Database User Permissions

Create a limited database user for the application:

```sql
-- Create application role
CREATE ROLE eventsource_app WITH LOGIN PASSWORD 'secure_password';

-- Grant minimal permissions
GRANT CONNECT ON DATABASE eventsource TO eventsource_app;
GRANT USAGE ON SCHEMA public TO eventsource_app;

-- Event store tables (read/write)
GRANT SELECT, INSERT ON events TO eventsource_app;
GRANT SELECT, INSERT, UPDATE ON event_outbox TO eventsource_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON projection_checkpoints TO eventsource_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON dead_letter_queue TO eventsource_app;
GRANT USAGE ON SEQUENCE dead_letter_queue_id_seq TO eventsource_app;

-- Read-only user for analytics
CREATE ROLE eventsource_readonly WITH LOGIN PASSWORD 'readonly_password';
GRANT CONNECT ON DATABASE eventsource TO eventsource_readonly;
GRANT USAGE ON SCHEMA public TO eventsource_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO eventsource_readonly;
```

### Audit Logging

The event store provides a natural audit trail. For additional audit requirements:

```sql
-- Enable PostgreSQL audit logging
ALTER SYSTEM SET log_statement = 'mod';  -- Log INSERT/UPDATE/DELETE
ALTER SYSTEM SET log_connections = 'on';
SELECT pg_reload_conf();
```

---

## Scaling Considerations

### Horizontal Scaling Patterns

**1. Stateless Application Servers:**
```
                        +---> App Server 1 ---+
Load Balancer --------->+---> App Server 2 ---+---> PostgreSQL Primary
                        +---> App Server N ---+          |
                                                         v
                                              PostgreSQL Replicas
```

**2. Projection Workers:**
```
                     +---> Projection Worker 1 (Partition 0)
Event Bus ---------> +---> Projection Worker 2 (Partition 1)
                     +---> Projection Worker N (Partition N-1)
```

### Read Replica Usage

Route read-heavy operations to replicas:

```python
class ScaledEventStore:
    """Event store with read/write splitting."""

    def __init__(self, write_factory, read_factory):
        self._write_store = PostgreSQLEventStore(write_factory)
        self._read_store = PostgreSQLEventStore(read_factory)

    async def append_events(self, *args, **kwargs):
        """Writes always go to primary."""
        return await self._write_store.append_events(*args, **kwargs)

    async def get_events(self, *args, **kwargs):
        """Reads go to replica with fallback to primary."""
        try:
            return await self._read_store.get_events(*args, **kwargs)
        except Exception:
            # Fallback to primary if replica is behind
            return await self._write_store.get_events(*args, **kwargs)
```

### Event Store Partitioning

For very high-volume systems, partition events across multiple databases:

```python
from hashlib import md5


class ShardedEventStore:
    """Event store sharded by aggregate ID."""

    def __init__(self, shard_stores: list[PostgreSQLEventStore]):
        self._shards = shard_stores

    def _get_shard(self, aggregate_id: UUID) -> PostgreSQLEventStore:
        """Consistent hashing to determine shard."""
        hash_value = int(md5(str(aggregate_id).encode()).hexdigest(), 16)
        shard_index = hash_value % len(self._shards)
        return self._shards[shard_index]

    async def append_events(self, aggregate_id: UUID, *args, **kwargs):
        store = self._get_shard(aggregate_id)
        return await store.append_events(aggregate_id, *args, **kwargs)

    async def get_events(self, aggregate_id: UUID, *args, **kwargs):
        store = self._get_shard(aggregate_id)
        return await store.get_events(aggregate_id, *args, **kwargs)
```

---

## Operational Procedures

### Rebuilding a Single Projection

```python
async def rebuild_single_projection(
    projection_name: str,
    event_store: PostgreSQLEventStore,
    projections: dict[str, DeclarativeProjection],
) -> None:
    """Rebuild a specific projection by name."""
    if projection_name not in projections:
        raise ValueError(f"Unknown projection: {projection_name}")

    projection = projections[projection_name]

    logger.warning("Starting rebuild of %s", projection_name)

    # Reset projection
    await projection.reset()

    # Replay events
    count = 0
    async for stored_event in event_store.read_all():
        if type(stored_event.event) in projection.subscribed_to():
            await projection.handle(stored_event.event)
            count += 1

    logger.info("Rebuilt %s: processed %d events", projection_name, count)
```

### Processing the Dead Letter Queue

```python
async def process_dlq(
    dlq_repo: PostgreSQLDLQRepository,
    projections: dict[str, DeclarativeProjection],
    event_registry: EventRegistry,
    operator_id: str,
) -> dict:
    """Process failed events from the DLQ."""
    resolved = 0
    failed = 0

    entries = await dlq_repo.get_failed_events(status="failed", limit=100)

    for entry in entries:
        projection = projections.get(entry["projection_name"])
        if not projection:
            logger.error("Unknown projection: %s", entry["projection_name"])
            continue

        try:
            # Mark as retrying
            await dlq_repo.mark_retrying(entry["id"])

            # Reconstruct event
            event_class = event_registry.get(entry["event_type"])
            event_data = entry["event_data"]
            if isinstance(event_data, str):
                import json
                event_data = json.loads(event_data)
            event = event_class.model_validate(event_data)

            # Process event
            await projection._process_event(event)

            # Mark as resolved
            await dlq_repo.mark_resolved(entry["id"], operator_id)
            resolved += 1
            logger.info("Resolved DLQ entry %s", entry["id"])

        except Exception as e:
            failed += 1
            logger.error(
                "Failed to process DLQ entry %s: %s",
                entry["id"], e,
                exc_info=True,
            )

    return {"resolved": resolved, "failed": failed}
```

### Cleanup Procedures

```python
async def cleanup_old_data(
    dlq_repo: PostgreSQLDLQRepository,
    outbox_repo,
    engine,
) -> dict:
    """Periodic cleanup of old data."""
    results = {}

    # Delete resolved DLQ entries older than 30 days
    dlq_deleted = await dlq_repo.delete_resolved_events(older_than_days=30)
    results["dlq_deleted"] = dlq_deleted

    # Delete published outbox entries older than 7 days
    outbox_deleted = await outbox_repo.delete_published_events(older_than_days=7)
    results["outbox_deleted"] = outbox_deleted

    # Vacuum analyze tables for performance
    async with engine.begin() as conn:
        await conn.execute(text("VACUUM ANALYZE events"))
        await conn.execute(text("VACUUM ANALYZE projection_checkpoints"))
        await conn.execute(text("VACUUM ANALYZE dead_letter_queue"))

    return results
```

---

## Docker Compose for Production

Example Docker Compose configuration for a production-like environment:

```yaml
version: '3.8'

services:
  app:
    build:
      context: .
      dockerfile: Dockerfile
    environment:
      DATABASE_URL: postgresql+asyncpg://app:${DB_PASSWORD}@postgres:5432/eventsource
      DATABASE_POOL_SIZE: 10
      DATABASE_MAX_OVERFLOW: 20
      REDIS_URL: redis://redis:6379/0
      LOG_LEVEL: INFO
      ENABLE_TRACING: "true"
      OTEL_EXPORTER_OTLP_ENDPOINT: http://jaeger:4317
    depends_on:
      postgres:
        condition: service_healthy
      redis:
        condition: service_healthy
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8000/health"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 10s
    deploy:
      replicas: 3
      resources:
        limits:
          cpus: '1.0'
          memory: 512M
    networks:
      - eventsource-network

  postgres:
    image: postgres:15
    environment:
      POSTGRES_DB: eventsource
      POSTGRES_USER: app
      POSTGRES_PASSWORD: ${DB_PASSWORD}
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./src/eventsource/migrations/schemas/all.sql:/docker-entrypoint-initdb.d/01_schema.sql
    command:
      - "postgres"
      - "-c"
      - "shared_buffers=256MB"
      - "-c"
      - "effective_cache_size=768MB"
      - "-c"
      - "max_connections=200"
      - "-c"
      - "log_min_duration_statement=1000"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U app -d eventsource"]
      interval: 5s
      timeout: 5s
      retries: 5
      start_period: 10s
    deploy:
      resources:
        limits:
          cpus: '2.0'
          memory: 2G
    networks:
      - eventsource-network

  redis:
    image: redis:7-alpine
    command: redis-server --maxmemory 128mb --maxmemory-policy allkeys-lru
    volumes:
      - redis_data:/data
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 5s
      retries: 5
    networks:
      - eventsource-network

  jaeger:
    image: jaegertracing/all-in-one:latest
    environment:
      COLLECTOR_OTLP_ENABLED: "true"
    ports:
      - "16686:16686"  # Jaeger UI
    networks:
      - eventsource-network

volumes:
  postgres_data:
  redis_data:

networks:
  eventsource-network:
    driver: bridge
```

---

## Troubleshooting

### Common Issues

| Issue | Symptoms | Resolution |
|-------|----------|------------|
| Connection pool exhausted | Timeout errors, "connection pool exhausted" | Increase pool_size and max_overflow |
| High projection lag | Lag metrics > 60s | Check projection performance, add workers |
| Frequent lock conflicts | Many OptimisticLockError | Reduce aggregate size, implement retry logic |
| Slow aggregate loading | High latency on get_events | Check indexes, consider partition pruning |
| DLQ growth | Failed events accumulating | Investigate errors, fix bugs, process DLQ |

### Debugging Queries

```sql
-- Check connection pool usage
SELECT count(*), state
FROM pg_stat_activity
WHERE datname = 'eventsource'
GROUP BY state;

-- Find slow queries
SELECT query, calls, mean_time, max_time
FROM pg_stat_statements
WHERE query LIKE '%events%'
ORDER BY mean_time DESC
LIMIT 10;

-- Check table sizes
SELECT relname, pg_size_pretty(pg_total_relation_size(relid))
FROM pg_catalog.pg_statio_user_tables
ORDER BY pg_total_relation_size(relid) DESC;

-- Check projection lag
SELECT
    projection_name,
    events_processed,
    last_processed_at,
    EXTRACT(EPOCH FROM (NOW() - last_processed_at)) as lag_seconds
FROM projection_checkpoints
ORDER BY lag_seconds DESC;

-- Check DLQ status
SELECT
    projection_name,
    COUNT(*) as failed_count,
    MIN(first_failed_at) as oldest_failure
FROM dead_letter_queue
WHERE status = 'failed'
GROUP BY projection_name;
```

### Performance Analysis

```sql
-- Enable timing
\timing on

-- Analyze event loading performance
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT * FROM events
WHERE aggregate_id = '550e8400-e29b-41d4-a716-446655440000'
ORDER BY version ASC;

-- Check index usage
SELECT
    schemaname, tablename, indexname,
    idx_scan, idx_tup_read, idx_tup_fetch
FROM pg_stat_user_indexes
WHERE tablename = 'events'
ORDER BY idx_scan DESC;
```

---

## See Also

- [Event Stores API](../api/stores.md) - Database schema documentation
- [Error Handling Guide](error-handling.md) - Exception handling patterns
- [Multi-Tenant Guide](multi-tenant.md) - Multi-tenancy configuration
- [Authentication Guide](authentication.md) - Actor and authentication patterns
