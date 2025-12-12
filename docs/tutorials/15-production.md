# Tutorial 15: Production Deployment Guide

**Difficulty:** Advanced
**Progress:** Tutorial 15 of 21 | Phase 3: Production Readiness

Learn to deploy event-sourced applications to production with proper performance tuning, monitoring, scaling, and security.

**Prerequisites:** Tutorials 11-14, Docker basics, Python 3.11+

```bash
pip install eventsource-py[postgresql]
```

---

## Production Checklist

Verify before deploying:

**Infrastructure:** PostgreSQL configured, connection pooling, schema migrations, event indexes, backups (PITR), monitoring endpoints, structured logging, secrets management

**Application:** Health checks (readiness/liveness), graceful shutdown, DLQ for errors, checkpointing strategy, snapshots, tracing, retry policies, rate limiting

**Operations:** Runbook, alerting rules, capacity planning, disaster recovery tested, performance baselines, on-call rotation

---

## Performance Tuning

### Connection Pool Configuration

```python
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession

def create_production_engine(database_url: str, workload: str = "medium"):
    pool_configs = {
        "light": {"pool_size": 3, "max_overflow": 5},
        "medium": {"pool_size": 10, "max_overflow": 15},
        "heavy": {"pool_size": 20, "max_overflow": 30},
    }
    config = pool_configs.get(workload, pool_configs["medium"])

    return create_async_engine(
        database_url,
        pool_size=config["pool_size"],
        max_overflow=config["max_overflow"],
        pool_timeout=30,
        pool_recycle=1800,
        pool_pre_ping=True,
        echo=False,
        connect_args={"server_settings": {"statement_timeout": "30000"}},
    )

def create_production_session_factory(engine):
    return async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
```

| Workload | Concurrent Requests | pool_size | max_overflow |
|----------|-------------------|-----------|--------------|
| Light | < 50 | 3-5 | 5-10 |
| Medium | 50-500 | 10-15 | 15-20 |
| Heavy | > 500 | 20-30 | 30-50 |

**Note:** Total connections across all instances must not exceed PostgreSQL's `max_connections` (default: 100).

### Event Store Optimization

```python
from eventsource import PostgreSQLEventStore

def create_production_event_store(session_factory):
    return PostgreSQLEventStore(
        session_factory,
        outbox_enabled=True,
        enable_tracing=True,
        auto_detect_uuid=True,
        string_id_fields={"stripe_id", "external_ref"},
    )
```

### Batch Processing Configuration

```python
from eventsource.subscriptions import (
    SubscriptionManager, SubscriptionConfig, CheckpointStrategy, HealthCheckConfig
)

def create_production_subscription_config():
    return SubscriptionConfig(
        start_from="checkpoint",
        batch_size=100,
        max_in_flight=1000,
        checkpoint_strategy=CheckpointStrategy.EVERY_BATCH,
        continue_on_error=True,
        processing_timeout=30.0,
        backpressure_threshold=0.8,
    )

def create_production_manager(event_store, event_bus, checkpoint_repo, dlq_repo=None):
    health_config = HealthCheckConfig(
        max_error_rate_per_minute=10.0,
        max_errors_warning=10,
        max_errors_critical=100,
        max_lag_events_warning=1000,
        max_lag_events_critical=10000,
        max_dlq_events_warning=10,
        max_dlq_events_critical=100,
    )

    return SubscriptionManager(
        event_store=event_store,
        event_bus=event_bus,
        checkpoint_repo=checkpoint_repo,
        shutdown_timeout=30.0,
        drain_timeout=10.0,
        dlq_repo=dlq_repo,
        health_check_config=health_config,
        enable_tracing=True,
    )
```

### Snapshot Configuration

```python
from eventsource import AggregateRepository
from eventsource.snapshots import PostgreSQLSnapshotStore

def create_production_repository(
    event_store, snapshot_session_factory, aggregate_factory,
    aggregate_type: str, event_publisher=None
):
    snapshot_store = PostgreSQLSnapshotStore(snapshot_session_factory)
    return AggregateRepository(
        event_store=event_store,
        aggregate_factory=aggregate_factory,
        aggregate_type=aggregate_type,
        event_publisher=event_publisher,
        snapshot_store=snapshot_store,
        snapshot_threshold=100,
        snapshot_mode="background",
        enable_tracing=True,
    )
```

| Mode | Latency | Use Case |
|------|---------|----------|
| `sync` | Higher | Simple apps, low throughput |
| `background` | Minimal | High-throughput production |
| `manual` | None | Full control over timing |

---

## Monitoring and Observability

### Health Check Endpoints

```python
from fastapi import FastAPI, Response
from contextlib import asynccontextmanager
from eventsource.subscriptions import SubscriptionManager

manager: SubscriptionManager | None = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global manager
    # Initialize infrastructure
    event_store = InMemoryEventStore()
    event_bus = InMemoryEventBus()
    checkpoint_repo = InMemoryCheckpointRepository()
    manager = SubscriptionManager(
        event_store, event_bus, checkpoint_repo,
        shutdown_timeout=30.0, drain_timeout=10.0
    )
    await manager.start()
    yield
    await manager.stop()

app = FastAPI(lifespan=lifespan)

@app.get("/health")
async def health_check():
    if not manager:
        return Response(content='{"status":"unhealthy"}', status_code=503)
    health = await manager.health_check()
    status_code = 200 if health.status in ("healthy", "degraded") else 503
    return Response(content=health.model_dump_json(), status_code=status_code)

@app.get("/health/ready")
async def readiness_check():
    if not manager:
        return Response(content='{"ready":false}', status_code=503)
    readiness = await manager.readiness_check()
    return Response(
        content=readiness.model_dump_json(),
        status_code=200 if readiness.ready else 503
    )

@app.get("/health/live")
async def liveness_check():
    if not manager:
        return Response(content='{"alive":false}', status_code=503)
    liveness = await manager.liveness_check()
    return Response(
        content=liveness.model_dump_json(),
        status_code=200 if liveness.alive else 503
    )
```

### Structured Logging

```python
import logging
import json
import sys
from datetime import datetime, UTC

class JSONFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        # Add extra fields from record.__dict__
        for key, value in record.__dict__.items():
            if key not in ("name", "msg", "args", "levelname", "message"):
                log_data[key] = value
        return json.dumps(log_data)

def configure_production_logging(level: str = "INFO"):
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, level.upper()))
    root_logger.handlers.clear()
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JSONFormatter())
    root_logger.addHandler(handler)
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
    return root_logger

# Usage
logger = configure_production_logging("INFO")
logger.info("Order processed", extra={"order_id": "12345", "total": 299.99})
```

### OpenTelemetry Integration

```python
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource, SERVICE_NAME, SERVICE_VERSION
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

def configure_tracing(service_name: str, service_version: str, otlp_endpoint: str = "localhost:4317"):
    resource = Resource.create({SERVICE_NAME: service_name, SERVICE_VERSION: service_version})
    provider = TracerProvider(resource=resource)
    exporter = OTLPSpanExporter(endpoint=otlp_endpoint, insecure=True)
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    return provider

# Usage
import os
configure_tracing(
    service_name=os.getenv("SERVICE_NAME", "eventsource-app"),
    service_version=os.getenv("SERVICE_VERSION", "1.0.0"),
    otlp_endpoint=os.getenv("OTLP_ENDPOINT", "localhost:4317")
)
```

### Error Monitoring

```python
import logging
from eventsource.subscriptions import SubscriptionManager, ErrorInfo, ErrorSeverity, ErrorCategory

logger = logging.getLogger(__name__)

async def log_all_errors(error_info: ErrorInfo) -> None:
    logger.error("Subscription error", extra={
        "subscription_name": error_info.subscription_name,
        "event_id": str(error_info.event_id),
        "error_message": error_info.error_message,
        "severity": error_info.classification.severity.value,
    })

async def alert_critical_errors(error_info: ErrorInfo) -> None:
    logger.critical(f"CRITICAL: {error_info.error_message} in {error_info.subscription_name}")
    # Integration point for PagerDuty, OpsGenie, Slack

def configure_error_monitoring(manager: SubscriptionManager) -> None:
    manager.on_error(log_all_errors)
    manager.on_error_severity(ErrorSeverity.CRITICAL, alert_critical_errors)
```

---

## Scaling Strategies

### Horizontal Scaling for Projections

```python
import hashlib

def get_partition_for_aggregate(aggregate_id: str, num_partitions: int) -> int:
    """Use consistent hashing to assign aggregates to partitions."""
    hash_value = int(hashlib.sha256(aggregate_id.encode()).hexdigest(), 16)
    return hash_value % num_partitions

class PartitionedProjection:
    """Base class for partitioned projections."""
    def __init__(self, partition_id: int, num_partitions: int):
        self.partition_id = partition_id
        self.num_partitions = num_partitions

    def should_handle(self, aggregate_id: str) -> bool:
        return get_partition_for_aggregate(aggregate_id, self.num_partitions) == self.partition_id

    async def handle_event(self, event) -> None:
        if self.should_handle(str(event.aggregate_id)):
            await self._process_event(event)

    async def _process_event(self, event) -> None:
        raise NotImplementedError
```

### Database Connection Scaling

```python
import os
from dataclasses import dataclass

@dataclass
class ConnectionConfig:
    host: str
    port: int
    database: str
    user: str
    password: str
    pool_size: int
    max_overflow: int

    @classmethod
    def from_environment(cls) -> "ConnectionConfig":
        instance_count = int(os.getenv("INSTANCE_COUNT", "1"))
        max_db_connections = int(os.getenv("MAX_DB_CONNECTIONS", "100"))
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
        return f"postgresql+asyncpg://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
```

### Event Store Partitioning

```sql
-- Partition by time range for high volumes
CREATE TABLE events (
    event_id UUID NOT NULL,
    aggregate_id UUID NOT NULL,
    aggregate_type VARCHAR(255) NOT NULL,
    event_type VARCHAR(255) NOT NULL,
    version INTEGER NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_events PRIMARY KEY (event_id, created_at)
) PARTITION BY RANGE (created_at);

CREATE TABLE events_2024_01 PARTITION OF events
    FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');

CREATE INDEX idx_events_aggregate_id ON events (aggregate_id);
CREATE INDEX idx_events_aggregate_type ON events (aggregate_type);
```

---

## Backup and Recovery

### Event Store Backup Strategy

```python
import json
from pathlib import Path
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

async def export_events_to_jsonl(session: AsyncSession, output_path: Path, batch_size: int = 1000) -> int:
    """Export all events to JSONL for backup."""
    total_exported = 0
    offset = 0

    with open(output_path, "w") as f:
        while True:
            result = await session.execute(
                text("SELECT event_id::text, aggregate_id::text, aggregate_type, event_type, "
                     "version, timestamp, payload, created_at FROM events "
                     "ORDER BY created_at LIMIT :limit OFFSET :offset"),
                {"limit": batch_size, "offset": offset}
            )
            rows = result.fetchall()
            if not rows:
                break

            for row in rows:
                event_data = {
                    "event_id": row[0], "aggregate_id": row[1], "aggregate_type": row[2],
                    "event_type": row[3], "version": row[4], "timestamp": row[5].isoformat(),
                    "payload": row[6], "created_at": row[7].isoformat()
                }
                f.write(json.dumps(event_data) + "\n")
                total_exported += 1

            offset += batch_size

    return total_exported

async def import_events_from_jsonl(session: AsyncSession, input_path: Path, batch_size: int = 1000) -> int:
    """Import events from JSONL backup."""
    total_imported = 0
    batch = []

    with open(input_path, "r") as f:
        for line in f:
            batch.append(json.loads(line.strip()))
            if len(batch) >= batch_size:
                for event_data in batch:
                    await session.execute(
                        text("INSERT INTO events (event_id, aggregate_id, aggregate_type, event_type, "
                             "version, timestamp, payload, created_at) VALUES (:event_id::uuid, "
                             ":aggregate_id::uuid, :aggregate_type, :event_type, :version, "
                             ":timestamp::timestamptz, :payload::jsonb, :created_at::timestamptz) "
                             "ON CONFLICT DO NOTHING"),
                        event_data
                    )
                await session.commit()
                total_imported += len(batch)
                batch = []

    return total_imported
```

### Point-in-Time Recovery

```bash
# postgresql.conf settings for PITR
archive_mode = on
archive_command = 'cp %p /path/to/archive/%f'
wal_level = replica
wal_keep_size = 1GB
checkpoint_timeout = 15min
max_wal_size = 4GB
```

### Projection Rebuild

```python
import asyncio
from eventsource.subscriptions import SubscriptionManager, SubscriptionConfig

async def rebuild_projection(manager: SubscriptionManager, projection, name: str):
    """Rebuild a projection from the beginning."""
    await projection._truncate_read_models()
    await manager.subscribe(projection, name=name, config=SubscriptionConfig(
        start_from="beginning", batch_size=500, checkpoint_strategy="every_batch"
    ))
    await manager.start(subscription_names=[name])

    # Wait for catch-up
    while True:
        health = manager.get_subscription_health(name)
        if health and health.lag_events == 0:
            break
        await asyncio.sleep(1.0)

    return projection
```

---

## Security Considerations

### Connection Security

```python
from sqlalchemy.ext.asyncio import create_async_engine

def create_secure_engine(host: str, port: int, database: str, user: str, password: str,
                         ssl_mode: str = "require", ssl_root_cert: str | None = None):
    """Create a secure database engine with TLS."""
    url = f"postgresql+asyncpg://{user}:{password}@{host}:{port}/{database}"
    connect_args = {"ssl": ssl_mode}
    if ssl_root_cert:
        connect_args["ssl"] = {"ca": ssl_root_cert}
    return create_async_engine(url, connect_args=connect_args, pool_pre_ping=True)
```

### Event Data Protection

```python
import hashlib
from cryptography.fernet import Fernet

class EventDataProtector:
    """Protect sensitive event data with encryption and hashing."""
    def __init__(self, encryption_key: bytes):
        self.fernet = Fernet(encryption_key)

    def encrypt_field(self, value: str) -> str:
        return self.fernet.encrypt(value.encode()).decode()

    def decrypt_field(self, encrypted: str) -> str:
        return self.fernet.decrypt(encrypted.encode()).decode()

    @staticmethod
    def hash_field(value: str, salt: str = "") -> str:
        """One-way hash for PII verification."""
        return hashlib.sha256(f"{salt}{value}".encode()).hexdigest()

    @staticmethod
    def mask_field(value: str, visible_chars: int = 4) -> str:
        """Mask sensitive data for display."""
        if len(value) <= visible_chars:
            return "*" * len(value)
        return "*" * (len(value) - visible_chars) + value[-visible_chars:]
```

### Audit Logging

```python
import logging
from datetime import datetime, UTC
from uuid import UUID

class AuditLogger:
    """Track access and modifications."""
    def __init__(self, logger_name: str = "audit"):
        self.logger = logging.getLogger(logger_name)

    def log_event_access(self, actor_id: str, aggregate_id: UUID, aggregate_type: str,
                         action: str, ip_address: str | None = None):
        self.logger.info("Event data access", extra={
            "audit_type": "access", "timestamp": datetime.now(UTC).isoformat(),
            "actor_id": actor_id, "aggregate_id": str(aggregate_id),
            "aggregate_type": aggregate_type, "action": action, "ip_address": ip_address
        })
```

---

## Complete Production Example

Complete production-ready application setup:

```python
"""Tutorial 15: Production Deployment - Run with: python tutorial_15_production.py"""
import asyncio
import logging
import os
import signal
from typing import Any
from uuid import uuid4
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from eventsource import (DomainEvent, register_event, AggregateRoot, PostgreSQLEventStore,
                         AggregateRepository, InMemoryEventBus, PostgreSQLCheckpointRepository)
from eventsource.subscriptions import (SubscriptionManager, HealthCheckConfig, ErrorInfo, ErrorSeverity)
from eventsource.snapshots import PostgreSQLSnapshotStore

class AppConfig:
    def __init__(self):
        self.database_url = os.getenv("DATABASE_URL",
            "postgresql+asyncpg://postgres:postgres@localhost:5432/eventsource_prod")
        self.pool_size = int(os.getenv("DB_POOL_SIZE", "10"))
        self.max_overflow = int(os.getenv("DB_MAX_OVERFLOW", "15"))
        self.log_level = os.getenv("LOG_LEVEL", "INFO")
        self.shutdown_timeout = float(os.getenv("SHUTDOWN_TIMEOUT", "30"))
        self.drain_timeout = float(os.getenv("DRAIN_TIMEOUT", "10"))

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
            self._state = OrderState(order_id=str(self.aggregate_id),
                customer_id=event.customer_id, total=event.total, status="created")
        elif isinstance(event, OrderShipped):
            if self._state:
                self._state = self._state.model_copy(
                    update={"status": "shipped", "tracking_number": event.tracking_number})

    def create(self, customer_id: str, total: float) -> None:
        if self.version > 0:
            raise ValueError("Order already exists")
        self.apply_event(OrderCreated(aggregate_id=self.aggregate_id,
            customer_id=customer_id, total=total, aggregate_version=self.get_next_version()))

    def ship(self, tracking_number: str) -> None:
        if not self.state or self.state.status != "created":
            raise ValueError("Cannot ship order in current state")
        self.apply_event(OrderShipped(aggregate_id=self.aggregate_id,
            tracking_number=tracking_number, aggregate_version=self.get_next_version()))

def setup_logging(level: str = "INFO") -> logging.Logger:
    logging.basicConfig(level=getattr(logging, level.upper()),
        format='{"timestamp":"%(asctime)s","level":"%(levelname)s","message":"%(message)s"}')
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
    return logging.getLogger("eventsource.app")

async def handle_error(error_info: ErrorInfo) -> None:
    logger = logging.getLogger("eventsource.errors")
    logger.error(f"Subscription error: {error_info.error_message}",
        extra={"subscription": error_info.subscription_name, "event_id": str(error_info.event_id)})

async def handle_critical_error(error_info: ErrorInfo) -> None:
    logger = logging.getLogger("eventsource.alerts")
    logger.critical(f"CRITICAL: {error_info.error_message} in {error_info.subscription_name}")

class ProductionApp:
    def __init__(self, config: AppConfig):
        self.config = config
        self.logger = setup_logging(config.log_level)
        self.engine = None
        self.manager = None
        self.repo = None

    async def initialize(self) -> None:
        self.logger.info("Initializing application...")
        self.engine = create_async_engine(self.config.database_url,
            pool_size=self.config.pool_size, max_overflow=self.config.max_overflow,
            pool_timeout=30, pool_recycle=1800, pool_pre_ping=True)
        session_factory = async_sessionmaker(self.engine, expire_on_commit=False, class_=AsyncSession)

        event_store = PostgreSQLEventStore(session_factory, enable_tracing=True)
        event_bus = InMemoryEventBus()
        checkpoint_repo = PostgreSQLCheckpointRepository(session_factory)
        snapshot_store = PostgreSQLSnapshotStore(session_factory)

        health_config = HealthCheckConfig(max_error_rate_per_minute=10.0,
            max_errors_warning=10, max_errors_critical=100,
            max_lag_events_warning=1000, max_lag_events_critical=10000)

        self.manager = SubscriptionManager(event_store=event_store, event_bus=event_bus,
            checkpoint_repo=checkpoint_repo, shutdown_timeout=self.config.shutdown_timeout,
            drain_timeout=self.config.drain_timeout, health_check_config=health_config,
            enable_tracing=True)

        self.manager.on_error(handle_error)
        self.manager.on_error_severity(ErrorSeverity.CRITICAL, handle_critical_error)

        self.repo = AggregateRepository(event_store=event_store, aggregate_factory=OrderAggregate,
            aggregate_type="Order", event_publisher=event_bus, snapshot_store=snapshot_store,
            snapshot_threshold=100, snapshot_mode="background", enable_tracing=True)

        self.logger.info("Application initialized successfully")

    async def start(self) -> None:
        self.logger.info("Starting application...")
        await self.manager.start()
        self.logger.info("Application started")

    async def stop(self) -> None:
        self.logger.info("Stopping application...")
        if self.manager:
            result = await self.manager.stop()
            self.logger.info(f"Manager stopped: {result.phase.value}, "
                f"duration={result.duration_seconds:.2f}s")
        if self.engine:
            await self.engine.dispose()
        self.logger.info("Application stopped")

    async def health_check(self) -> dict[str, Any]:
        if not self.manager:
            return {"status": "unhealthy"}
        health = await self.manager.health_check()
        return {"status": health.status, "running": health.running,
                "subscription_count": health.subscription_count,
                "total_events_processed": health.total_events_processed}

async def main():
    config = AppConfig()
    app = ProductionApp(config)
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

        health = await app.health_check()
        app.logger.info(f"Health status: {health['status']}")

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

## Next Steps

Continue to [Tutorial 16: Multi-Tenancy](16-multi-tenancy.md).
