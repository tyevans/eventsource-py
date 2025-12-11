# Migration Monitoring Guide

This guide covers monitoring strategies, key metrics, and alerting recommendations for tenant migrations.

## Key Metrics Overview

### Migration Progress Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `migration_state` | Gauge | `migration_id`, `tenant_id` | Current state (0-6 enum) |
| `migration_progress_percent` | Gauge | `migration_id`, `tenant_id` | Bulk copy progress (0-100) |
| `migration_events_copied_total` | Counter | `migration_id`, `tenant_id` | Events copied to target |
| `migration_events_total` | Gauge | `migration_id`, `tenant_id` | Total events to copy |
| `migration_duration_seconds` | Gauge | `migration_id`, `phase` | Time in each phase |

### Dual-Write Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `dual_write_operations_total` | Counter | `tenant_id`, `status` | Dual-write attempts |
| `dual_write_target_failures_total` | Counter | `tenant_id` | Target write failures |
| `dual_write_latency_seconds` | Histogram | `tenant_id` | Dual-write operation latency |
| `dual_write_sync_lag_events` | Gauge | `tenant_id` | Events behind in target |

### Cutover Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `cutover_pause_duration_seconds` | Histogram | `migration_id` | Write pause duration |
| `cutover_blocked_writers` | Gauge | `migration_id` | Writers waiting during pause |
| `cutover_attempts_total` | Counter | `migration_id`, `status` | Cutover attempts |
| `cutover_timeout_total` | Counter | `migration_id` | Cutover timeouts |

### Consistency Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `verification_duration_seconds` | Histogram | `migration_id`, `level` | Verification time |
| `verification_violations_total` | Counter | `migration_id`, `type` | Violations found |
| `verification_events_checked` | Counter | `migration_id` | Events verified |

---

## Prometheus Metrics Collection

### Instrumenting the Migration System

```python
from prometheus_client import Counter, Gauge, Histogram, Info

# Migration state metrics
migration_state = Gauge(
    'migration_state',
    'Current migration state',
    ['migration_id', 'tenant_id']
)

migration_progress = Gauge(
    'migration_progress_percent',
    'Bulk copy progress percentage',
    ['migration_id', 'tenant_id']
)

migration_events_copied = Counter(
    'migration_events_copied_total',
    'Total events copied to target',
    ['migration_id', 'tenant_id']
)

# Dual-write metrics
dual_write_operations = Counter(
    'dual_write_operations_total',
    'Dual-write operations',
    ['tenant_id', 'status']
)

dual_write_latency = Histogram(
    'dual_write_latency_seconds',
    'Dual-write operation latency',
    ['tenant_id'],
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]
)

sync_lag = Gauge(
    'dual_write_sync_lag_events',
    'Events behind in target store',
    ['tenant_id']
)

# Cutover metrics
cutover_pause_duration = Histogram(
    'cutover_pause_duration_seconds',
    'Write pause duration during cutover',
    ['migration_id'],
    buckets=[0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]
)

cutover_blocked_writers = Gauge(
    'cutover_blocked_writers',
    'Writers blocked during cutover',
    ['migration_id']
)
```

### Metrics Collection Loop

```python
async def collect_migration_metrics(
    coordinator: MigrationCoordinator,
    tenant_ids: list[UUID],
    interval_seconds: float = 10.0
):
    """Background task to collect migration metrics."""

    while True:
        for tenant_id in tenant_ids:
            # Get active migration
            migrations = await coordinator.get_migrations_by_tenant(tenant_id)
            active = next(
                (m for m in migrations if m.state.value not in
                 ('migrated', 'failed', 'rolled_back')),
                None
            )

            if active:
                # Update state metric
                state_value = {
                    'pending': 0,
                    'bulk_copy': 1,
                    'dual_write': 2,
                    'cutover_paused': 3,
                    'migrated': 4,
                    'failed': 5,
                    'rolled_back': 6,
                    'paused': 7,
                }
                migration_state.labels(
                    migration_id=str(active.id),
                    tenant_id=str(tenant_id)
                ).set(state_value.get(active.state.value, -1))

                # Update progress metric
                migration_progress.labels(
                    migration_id=str(active.id),
                    tenant_id=str(tenant_id)
                ).set(active.progress_percentage)

                # Update sync lag if in dual-write
                if active.state.value == 'dual_write':
                    lag = await coordinator.get_sync_lag(active.id)
                    sync_lag.labels(tenant_id=str(tenant_id)).set(lag)

        await asyncio.sleep(interval_seconds)
```

---

## Grafana Dashboards

### Migration Overview Dashboard

```json
{
  "title": "Migration Overview",
  "panels": [
    {
      "title": "Active Migrations",
      "type": "stat",
      "targets": [{
        "expr": "count(migration_state{migration_state!=\"4\",migration_state!=\"5\",migration_state!=\"6\"})"
      }]
    },
    {
      "title": "Migration Progress",
      "type": "gauge",
      "targets": [{
        "expr": "migration_progress_percent"
      }]
    },
    {
      "title": "Events Copied Rate",
      "type": "graph",
      "targets": [{
        "expr": "rate(migration_events_copied_total[5m])"
      }]
    },
    {
      "title": "Sync Lag",
      "type": "graph",
      "targets": [{
        "expr": "dual_write_sync_lag_events"
      }]
    }
  ]
}
```

### Dual-Write Health Dashboard

```json
{
  "title": "Dual-Write Health",
  "panels": [
    {
      "title": "Dual-Write Success Rate",
      "type": "graph",
      "targets": [{
        "expr": "rate(dual_write_operations_total{status=\"success\"}[5m]) / rate(dual_write_operations_total[5m])"
      }]
    },
    {
      "title": "Target Write Failures",
      "type": "graph",
      "targets": [{
        "expr": "rate(dual_write_target_failures_total[5m])"
      }]
    },
    {
      "title": "Dual-Write Latency (p99)",
      "type": "graph",
      "targets": [{
        "expr": "histogram_quantile(0.99, rate(dual_write_latency_seconds_bucket[5m]))"
      }]
    }
  ]
}
```

---

## Alerting Rules

### Prometheus Alert Rules

```yaml
groups:
  - name: migration_alerts
    rules:
      # Migration stuck in bulk_copy
      - alert: MigrationBulkCopyStuck
        expr: |
          migration_state == 1
          and rate(migration_events_copied_total[15m]) == 0
        for: 15m
        labels:
          severity: warning
        annotations:
          summary: "Migration {{ $labels.migration_id }} stuck in bulk copy"
          description: "No events copied in the last 15 minutes"

      # High sync lag during dual-write
      - alert: MigrationHighSyncLag
        expr: dual_write_sync_lag_events > 1000
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High sync lag for tenant {{ $labels.tenant_id }}"
          description: "Sync lag is {{ $value }} events"

      # Target write failures
      - alert: MigrationTargetWriteFailures
        expr: rate(dual_write_target_failures_total[5m]) > 0
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Target write failures for tenant {{ $labels.tenant_id }}"
          description: "Target store write failures detected"

      # Cutover taking too long
      - alert: MigrationCutoverSlow
        expr: migration_state == 3 # CUTOVER_PAUSED
        for: 30s
        labels:
          severity: critical
        annotations:
          summary: "Migration {{ $labels.migration_id }} cutover taking too long"
          description: "Cutover has been paused for more than 30 seconds"

      # Migration failed
      - alert: MigrationFailed
        expr: migration_state == 5 # FAILED
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Migration {{ $labels.migration_id }} failed"
          description: "Migration has entered failed state"

      # High dual-write latency
      - alert: MigrationHighDualWriteLatency
        expr: |
          histogram_quantile(0.99, rate(dual_write_latency_seconds_bucket[5m])) > 0.1
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High dual-write latency for tenant {{ $labels.tenant_id }}"
          description: "p99 latency is {{ $value }}s"
```

---

## Health Checks

### Migration System Health Check

```python
from dataclasses import dataclass
from enum import Enum

class HealthStatus(Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"

@dataclass
class MigrationHealthReport:
    status: HealthStatus
    active_migrations: int
    stuck_migrations: int
    high_sync_lag_count: int
    target_write_failure_rate: float
    details: list[str]

async def check_migration_health(
    coordinator: MigrationCoordinator,
    routing_repo: TenantRoutingRepository,
) -> MigrationHealthReport:
    """Check overall migration system health."""

    details = []
    status = HealthStatus.HEALTHY

    # Get all active migrations
    active_migrations = await coordinator.get_active_migrations()
    active_count = len(active_migrations)

    # Check for stuck migrations (no progress in 15 minutes)
    stuck_count = 0
    for m in active_migrations:
        if m.state.value == "bulk_copy":
            # Check if progress is stalled
            # (would need historical data to compare)
            pass

    # Check sync lag for dual-write migrations
    high_lag_count = 0
    for m in active_migrations:
        if m.state.value == "dual_write":
            lag = await coordinator.get_sync_lag(m.id)
            if lag > 1000:
                high_lag_count += 1
                details.append(f"Migration {m.id}: sync lag {lag} events")

    # Calculate target write failure rate
    # (would need metrics from interceptors)
    failure_rate = 0.0

    # Determine overall status
    if stuck_count > 0 or failure_rate > 0.1:
        status = HealthStatus.UNHEALTHY
    elif high_lag_count > 0 or failure_rate > 0.01:
        status = HealthStatus.DEGRADED

    return MigrationHealthReport(
        status=status,
        active_migrations=active_count,
        stuck_migrations=stuck_count,
        high_sync_lag_count=high_lag_count,
        target_write_failure_rate=failure_rate,
        details=details,
    )
```

### HTTP Health Endpoint

```python
from fastapi import FastAPI, Response

app = FastAPI()

@app.get("/health/migration")
async def migration_health():
    """Health check endpoint for migration system."""

    report = await check_migration_health(coordinator, routing_repo)

    status_code = {
        HealthStatus.HEALTHY: 200,
        HealthStatus.DEGRADED: 200,
        HealthStatus.UNHEALTHY: 503,
    }[report.status]

    return Response(
        content=json.dumps({
            "status": report.status.value,
            "active_migrations": report.active_migrations,
            "stuck_migrations": report.stuck_migrations,
            "high_sync_lag_count": report.high_sync_lag_count,
            "target_write_failure_rate": report.target_write_failure_rate,
            "details": report.details,
        }),
        status_code=status_code,
        media_type="application/json",
    )
```

---

## Log Monitoring

### Important Log Events

| Event | Level | Description | Action |
|-------|-------|-------------|--------|
| `Migration started` | INFO | New migration initiated | Monitor progress |
| `Bulk copy progress` | INFO | Copy progress update | Normal |
| `Bulk copy complete` | INFO | Entering dual-write | Prepare for cutover |
| `Target write failed` | WARNING | Dual-write failure | Monitor frequency |
| `Cutover initiated` | INFO | Starting cutover | Watch closely |
| `Cutover timeout` | ERROR | Cutover exceeded timeout | Investigate |
| `Consistency check failed` | ERROR | Verification failed | Investigate |
| `Migration completed` | INFO | Migration successful | Verify |
| `Migration failed` | ERROR | Migration error | Investigate |

### Log Queries

**Kibana/Elasticsearch:**
```
# Find all migration events for a tenant
tenant_id:"12345678-1234-1234-1234-123456789abc" AND logger:"eventsource.migration"

# Find migration errors
level:ERROR AND logger:"eventsource.migration"

# Find cutover events
message:"cutover" AND logger:"eventsource.migration"
```

**Loki/Grafana:**
```
{app="eventsource"} |= "migration" |= "error"
```

### Structured Log Fields

The migration system logs include these fields:
- `migration_id`: Unique migration identifier
- `tenant_id`: Tenant being migrated
- `phase`: Current migration phase
- `operation`: Specific operation being performed
- `duration_ms`: Operation duration
- `events_count`: Number of events processed

---

## Tracing

### OpenTelemetry Spans

The migration system creates spans for:

| Span Name | Parent | Description |
|-----------|--------|-------------|
| `eventsource.migration.start` | None | Migration initialization |
| `eventsource.migration.bulk_copy` | start | Bulk copy phase |
| `eventsource.migration.bulk_copy.batch` | bulk_copy | Single batch copy |
| `eventsource.migration.dual_write` | start | Dual-write phase |
| `eventsource.migration.dual_write.append` | dual_write | Single dual-write |
| `eventsource.migration.cutover` | start | Cutover phase |
| `eventsource.migration.cutover.pause` | cutover | Write pause |
| `eventsource.migration.cutover.verify` | cutover | Consistency check |
| `eventsource.migration.cutover.switch` | cutover | Routing switch |

### Span Attributes

Common attributes on migration spans:
- `migration.id`: Migration UUID
- `migration.tenant_id`: Tenant UUID
- `migration.phase`: Current phase
- `migration.source_store`: Source store ID
- `migration.target_store`: Target store ID

### Configuring Tracing

```python
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.jaeger.thrift import JaegerExporter

# Configure tracer
trace.set_tracer_provider(TracerProvider())
tracer_provider = trace.get_tracer_provider()

# Add Jaeger exporter
jaeger_exporter = JaegerExporter(
    agent_host_name="localhost",
    agent_port=6831,
)
tracer_provider.add_span_processor(BatchSpanProcessor(jaeger_exporter))

# Migration components will now emit traces
coordinator = MigrationCoordinator(
    ...,
    config=MigrationConfig(enable_tracing=True),
)
```

---

## Monitoring Checklist

### Before Starting Migration

- [ ] Prometheus scraping migration metrics
- [ ] Grafana dashboard accessible
- [ ] Alert rules configured
- [ ] Log aggregation working
- [ ] Baseline metrics captured (normal operation)

### During Migration

- [ ] Progress metric increasing (bulk copy)
- [ ] Sync lag acceptable (dual-write)
- [ ] No target write failures
- [ ] Latency within SLA
- [ ] No alerts firing

### During Cutover

- [ ] Watch cutover_pause_duration closely
- [ ] Monitor blocked_writers count
- [ ] Verify cutover completes < timeout
- [ ] Check no errors in logs

### After Migration

- [ ] Migration state is MIGRATED
- [ ] No active alerts
- [ ] Tenant reads/writes working
- [ ] Latency back to baseline
- [ ] Clean up monitoring for completed migration

---

## Capacity Planning

### Estimating Migration Duration

```python
def estimate_migration_duration(
    total_events: int,
    copy_rate_per_second: float = 10000,
    dual_write_lag_events: int = 100,
    cutover_pause_seconds: float = 5.0,
    buffer_factor: float = 1.5,
) -> dict:
    """Estimate migration duration."""

    # Bulk copy time
    bulk_copy_seconds = total_events / copy_rate_per_second

    # Dual-write catch-up time
    # Assume similar rate
    dual_write_seconds = dual_write_lag_events / copy_rate_per_second

    # Total with buffer
    total_seconds = (bulk_copy_seconds + dual_write_seconds + cutover_pause_seconds) * buffer_factor

    return {
        "bulk_copy_estimate": f"{bulk_copy_seconds / 60:.1f} minutes",
        "dual_write_estimate": f"{dual_write_seconds:.1f} seconds",
        "cutover_estimate": f"{cutover_pause_seconds} seconds",
        "total_estimate": f"{total_seconds / 60:.1f} minutes",
    }
```

### Resource Requirements

| Phase | CPU | Memory | Disk I/O | Network |
|-------|-----|--------|----------|---------|
| Bulk Copy | Medium | Low | High (read source, write target) | Medium |
| Dual-Write | Low | Low | Medium (write both) | Medium |
| Cutover | Low | Low | Low | Low |

---

## Next Steps

- [Architecture](./architecture.md): System design overview
- [Runbook](./runbook.md): Operational procedures
- [Troubleshooting](./troubleshooting.md): Problem resolution
