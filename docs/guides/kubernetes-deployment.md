# Kubernetes Deployment Guide

This guide covers deploying eventsource-based applications to Kubernetes, with a focus on graceful shutdown, health checks, and operational best practices for event-sourced applications.

## Table of Contents

1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Pod Lifecycle Integration](#pod-lifecycle-integration)
4. [Health Probes](#health-probes)
5. [Example Manifests](#example-manifests)
6. [Shutdown Configuration](#shutdown-configuration)
7. [Monitoring and Observability](#monitoring-and-observability)
8. [Spot Instance Considerations](#spot-instance-considerations)
9. [Draining Background Snapshots](#draining-background-snapshots)
10. [Troubleshooting](#troubleshooting)
11. [Best Practices Checklist](#best-practices-checklist)

---

## Overview

### Why Graceful Shutdown Matters

Event-sourced applications require special care during shutdown to ensure:

- **Data Integrity**: In-flight events are processed completely before termination
- **Checkpoint Consistency**: Projection checkpoints are saved to prevent duplicate processing on restart
- **No Lost Work**: Events that were mid-processing are not lost or left in an inconsistent state

The eventsource library provides a `ShutdownCoordinator` that handles these concerns automatically when properly integrated with Kubernetes.

### eventsource Shutdown Capabilities

The library provides:

| Capability | Description |
|------------|-------------|
| Signal handling | Automatic SIGTERM/SIGINT registration |
| Phased shutdown | STOPPING -> DRAINING -> CHECKPOINTING -> STOPPED sequence |
| Configurable timeouts | Per-phase timeout control |
| Periodic checkpoints | Checkpoint saves during long drain phases |
| Pre/post shutdown hooks | Integration points for external services |
| Deadline support | Adapt shutdown to available time |
| Health probes | Kubernetes-style readiness and liveness checks |

---

## Prerequisites

### Kubernetes Cluster

- Kubernetes 1.19+ (for proper terminationGracePeriodSeconds support)
- Access to deploy pods with custom health probes

### Application Requirements

Your application should:

1. Use `ShutdownCoordinator` for signal handling
2. Expose HTTP health endpoints
3. Register subscriptions with the manager

### Example Application Setup

```python
from eventsource.subscriptions import (
    ShutdownCoordinator,
    SubscriptionManager,
)

# Create shutdown coordinator with appropriate timeouts
shutdown_coordinator = ShutdownCoordinator(
    timeout=30.0,         # Total shutdown timeout
    drain_timeout=15.0,   # Time for draining in-flight events
    checkpoint_timeout=5.0,  # Time for final checkpoint save
    checkpoint_interval=5.0, # Periodic checkpoint interval during drain
)

# Create subscription manager
manager = SubscriptionManager(
    event_store=event_store,
    event_bus=event_bus,
)

# Register signal handlers
shutdown_coordinator.register_signals()

# Start the manager
await manager.start()

# Wait for shutdown signal
await shutdown_coordinator.wait_for_shutdown()

# Execute graceful shutdown
result = await shutdown_coordinator.shutdown(
    stop_func=manager.stop,
    drain_func=manager.drain_events,
    checkpoint_func=manager.save_checkpoints,
)

if result.forced:
    logger.warning("Shutdown was forced", extra=result.to_dict())
```

---

## Pod Lifecycle Integration

### Understanding Kubernetes Pod Termination

When Kubernetes terminates a pod:

1. **Pod marked for deletion**: Pod removed from Service endpoints
2. **preStop hook executed**: If configured, runs before SIGTERM
3. **SIGTERM sent**: Application has `terminationGracePeriodSeconds` to shut down
4. **Grace period expires**: SIGKILL sent if pod still running

```
Pod Deletion Request
        |
        v
+------------------+
| Remove from      |
| Service/Endpoints|  <-- Clients can still send requests during this transition
+------------------+
        |
        v
+------------------+
| Execute preStop  |  <-- Use for load balancer drain
| Hook             |
+------------------+
        |
        v
+------------------+
| Send SIGTERM     |  <-- ShutdownCoordinator receives this
|                  |
| Graceful         |
| Shutdown         |
| Period           |
+------------------+
        |
        v
+------------------+
| Send SIGKILL     |  <-- Forced termination if still running
+------------------+
```

### terminationGracePeriodSeconds

**Critical Setting**: `terminationGracePeriodSeconds` must exceed your shutdown_timeout.

**Formula**:
```
terminationGracePeriodSeconds = preStop_sleep + shutdown_timeout + buffer
```

**Example Calculation**:
- preStop sleep: 5 seconds (load balancer drain)
- shutdown_timeout: 30 seconds
- buffer: 10 seconds (safety margin)
- **Total**: 45 seconds

```yaml
spec:
  terminationGracePeriodSeconds: 45
```

**Why the buffer matters**:
- Network latency in receiving SIGTERM
- Time for shutdown metrics reporting
- Post-shutdown hook execution
- Prevents race conditions at boundary

### preStop Hook

The preStop hook runs before SIGTERM is sent. Use it to allow load balancer drain time.

**Purpose**:
1. Allow in-flight requests to complete
2. Give load balancers time to route traffic elsewhere
3. Ensure no new work arrives during shutdown

```yaml
lifecycle:
  preStop:
    exec:
      command:
      - /bin/sh
      - -c
      - |
        echo "preStop: deregistering from load balancer"
        # Optional: notify external services
        # curl -X POST http://service-discovery/deregister
        echo "preStop: waiting for LB drain"
        sleep 5
```

**Recommended preStop duration**: 5-10 seconds

For applications behind AWS ALB/NLB or other cloud load balancers, increase to 10-15 seconds to account for health check intervals.

---

## Health Probes

### Readiness Probe

The readiness probe determines if the pod should receive traffic.

**eventsource behavior during shutdown**:
- Immediately returns not-ready when shutdown starts
- Subscriptions in ERROR state cause not-ready
- Subscriptions still STARTING cause not-ready

```yaml
readinessProbe:
  httpGet:
    path: /health/readiness
    port: 8080
  initialDelaySeconds: 5
  periodSeconds: 5
  failureThreshold: 1   # Fail fast for quick removal from service
  successThreshold: 1
  timeoutSeconds: 3
```

**Key Configuration Points**:

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| `failureThreshold: 1` | Single failure removes from service | Fast traffic diversion during shutdown |
| `periodSeconds: 5` | Frequent checks | Quick detection of shutdown state |
| `initialDelaySeconds: 5` | Short delay | Allow application to initialize |

**Example Readiness Endpoint**:

```python
from fastapi import FastAPI, Response

app = FastAPI()

@app.get("/health/readiness")
async def readiness_check(response: Response):
    """Kubernetes readiness probe endpoint."""
    status = await health_provider.readiness_check(
        is_running=manager.is_running,
        is_shutting_down=shutdown_coordinator.is_shutting_down,
    )

    if not status.ready:
        response.status_code = 503

    return {
        "ready": status.ready,
        "reason": status.reason,
        "details": status.details,
    }
```

### Liveness Probe

The liveness probe determines if the pod should be restarted.

**eventsource behavior during shutdown**:
- Remains alive during shutdown to allow graceful completion
- Only fails if internal deadlock detected
- More lenient than readiness

```yaml
livenessProbe:
  httpGet:
    path: /health/liveness
    port: 8080
  initialDelaySeconds: 15
  periodSeconds: 10
  failureThreshold: 3   # Be lenient - avoid unnecessary restarts
  successThreshold: 1
  timeoutSeconds: 5
```

**Key Configuration Points**:

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| `failureThreshold: 3` | Multiple failures before restart | Prevents restart during shutdown drain |
| `periodSeconds: 10` | Less frequent | Liveness should be stable |
| `initialDelaySeconds: 15` | Longer delay | Full initialization time |

**Example Liveness Endpoint**:

```python
@app.get("/health/liveness")
async def liveness_check():
    """Kubernetes liveness probe endpoint."""
    status = await health_provider.liveness_check(
        is_running=manager.is_running,
    )

    if not status.alive:
        return Response(
            content=json.dumps({
                "alive": False,
                "reason": status.reason,
            }),
            status_code=503,
            media_type="application/json",
        )

    return {
        "alive": True,
        "reason": status.reason,
        "uptime_seconds": health_provider.uptime_seconds,
    }
```

### Startup Probe (Optional)

For applications with slow startup, use a startup probe:

```yaml
startupProbe:
  httpGet:
    path: /health/liveness
    port: 8080
  initialDelaySeconds: 10
  periodSeconds: 5
  failureThreshold: 30  # 30 * 5 = 150 seconds max startup time
  timeoutSeconds: 3
```

---

## Example Manifests

### Complete Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: eventsource-worker
  labels:
    app: eventsource-worker
    app.kubernetes.io/name: eventsource-worker
    app.kubernetes.io/component: worker
spec:
  replicas: 3
  selector:
    matchLabels:
      app: eventsource-worker
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxUnavailable: 1
      maxSurge: 1
  template:
    metadata:
      labels:
        app: eventsource-worker
      annotations:
        # Prometheus scraping
        prometheus.io/scrape: "true"
        prometheus.io/port: "8080"
        prometheus.io/path: "/metrics"
    spec:
      # Allow enough time for graceful shutdown
      # preStop (5s) + shutdown_timeout (30s) + buffer (10s) = 45s
      terminationGracePeriodSeconds: 45

      # Ensure pods are spread across nodes
      topologySpreadConstraints:
      - maxSkew: 1
        topologyKey: kubernetes.io/hostname
        whenUnsatisfiable: ScheduleAnyway
        labelSelector:
          matchLabels:
            app: eventsource-worker

      containers:
      - name: worker
        image: myapp/eventsource-worker:latest
        imagePullPolicy: Always

        ports:
        - containerPort: 8080
          name: http
          protocol: TCP

        env:
        # Shutdown configuration
        - name: SHUTDOWN_TIMEOUT
          value: "30"
        - name: DRAIN_TIMEOUT
          value: "15"
        - name: CHECKPOINT_TIMEOUT
          value: "5"
        - name: CHECKPOINT_INTERVAL
          value: "5"

        # Database configuration
        - name: DATABASE_URL
          valueFrom:
            secretKeyRef:
              name: eventsource-secrets
              key: database-url

        # Logging
        - name: LOG_LEVEL
          value: "INFO"

        # Pod information for logging
        - name: POD_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        - name: POD_NAMESPACE
          valueFrom:
            fieldRef:
              fieldPath: metadata.namespace

        # preStop hook for load balancer drain
        lifecycle:
          preStop:
            exec:
              command:
              - /bin/sh
              - -c
              - |
                echo "preStop: starting graceful deregistration"
                sleep 5
                echo "preStop: completed"

        # Readiness probe - fast failure on shutdown
        readinessProbe:
          httpGet:
            path: /health/readiness
            port: 8080
          initialDelaySeconds: 5
          periodSeconds: 5
          failureThreshold: 1
          successThreshold: 1
          timeoutSeconds: 3

        # Liveness probe - stays alive during shutdown
        livenessProbe:
          httpGet:
            path: /health/liveness
            port: 8080
          initialDelaySeconds: 15
          periodSeconds: 10
          failureThreshold: 3
          successThreshold: 1
          timeoutSeconds: 5

        resources:
          requests:
            cpu: "100m"
            memory: "256Mi"
          limits:
            cpu: "1000m"
            memory: "512Mi"

        securityContext:
          allowPrivilegeEscalation: false
          readOnlyRootFilesystem: true
          runAsNonRoot: true
          runAsUser: 1000
          capabilities:
            drop:
            - ALL

      # Graceful termination
      dnsPolicy: ClusterFirst
      restartPolicy: Always

      securityContext:
        fsGroup: 1000
        runAsNonRoot: true
```

### Service

```yaml
apiVersion: v1
kind: Service
metadata:
  name: eventsource-worker
  labels:
    app: eventsource-worker
spec:
  type: ClusterIP
  selector:
    app: eventsource-worker
  ports:
  - name: http
    port: 8080
    targetPort: 8080
    protocol: TCP
```

### PodDisruptionBudget

Ensure high availability during voluntary disruptions:

```yaml
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: eventsource-worker-pdb
spec:
  minAvailable: 2  # Or use maxUnavailable: 1
  selector:
    matchLabels:
      app: eventsource-worker
```

**Guidelines**:
- For 3 replicas: `minAvailable: 2` or `maxUnavailable: 1`
- For stateful workloads: be more conservative
- Allows rolling updates while maintaining availability

### ConfigMap for Settings

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: eventsource-config
data:
  # Shutdown settings
  SHUTDOWN_TIMEOUT: "30"
  DRAIN_TIMEOUT: "15"
  CHECKPOINT_TIMEOUT: "5"
  CHECKPOINT_INTERVAL: "5"

  # Subscription settings
  BATCH_SIZE: "100"
  POLL_INTERVAL: "1.0"

  # Health check settings
  MAX_LAG_WARNING: "1000"
  MAX_LAG_CRITICAL: "10000"
```

---

## Shutdown Configuration

### Timeout Configuration Mapping

| eventsource Setting | Environment Variable | K8s Setting Relationship |
|---------------------|---------------------|--------------------------|
| `timeout` | `SHUTDOWN_TIMEOUT` | Must be < `terminationGracePeriodSeconds - preStop` |
| `drain_timeout` | `DRAIN_TIMEOUT` | Time to drain in-flight events |
| `checkpoint_timeout` | `CHECKPOINT_TIMEOUT` | Time for final checkpoint save |
| `checkpoint_interval` | `CHECKPOINT_INTERVAL` | Periodic saves during drain |

### Using the Shutdown Deadline Feature

For scenarios with hard deadlines (spot instances, maintenance windows), use the deadline feature:

```python
from datetime import UTC, datetime, timedelta
from eventsource.subscriptions import ShutdownCoordinator

coordinator = ShutdownCoordinator(
    timeout=30.0,
    drain_timeout=15.0,
    checkpoint_timeout=5.0,
)

# When receiving termination notice with specific deadline
async def handle_termination_notice(notice_time: datetime, grace_seconds: int):
    """Handle external termination notice."""
    deadline = notice_time + timedelta(seconds=grace_seconds)

    # Set deadline - coordinator will adapt timeouts automatically
    coordinator.set_shutdown_deadline(deadline)

    # Check remaining time
    remaining = coordinator.get_remaining_shutdown_time()
    logger.info(f"Shutdown deadline set, {remaining:.1f}s remaining")

    # Request shutdown
    coordinator.request_shutdown()
```

**How deadline adaptation works**:

1. If plenty of time available: use configured timeouts
2. If time is limited: proportionally reduce phase timeouts
3. If critically short: skip drain phase, prioritize checkpointing

```python
# Example: 10 seconds remaining
# - Stop: 1s (minimum)
# - Drain: 0s (skipped)
# - Checkpoint: 8s (prioritized)
# - Buffer: 1s
```

### Environment Variable Configuration

```python
import os
from eventsource.subscriptions import ShutdownCoordinator

def create_shutdown_coordinator() -> ShutdownCoordinator:
    """Create coordinator from environment variables."""
    return ShutdownCoordinator(
        timeout=float(os.environ.get("SHUTDOWN_TIMEOUT", "30")),
        drain_timeout=float(os.environ.get("DRAIN_TIMEOUT", "15")),
        checkpoint_timeout=float(os.environ.get("CHECKPOINT_TIMEOUT", "5")),
        checkpoint_interval=float(os.environ.get("CHECKPOINT_INTERVAL", "5")),
    )
```

---

## Monitoring and Observability

### Available Shutdown Metrics

When OpenTelemetry is configured, these metrics are emitted:

| Metric | Type | Description |
|--------|------|-------------|
| `eventsource.shutdown.initiated_total` | Counter | Number of shutdown operations initiated |
| `eventsource.shutdown.completed_total` | Counter | Completed shutdowns with outcome label |
| `eventsource.shutdown.duration_seconds` | Histogram | Total shutdown duration |
| `eventsource.shutdown.drain_duration_seconds` | Histogram | Duration of drain phase |
| `eventsource.shutdown.events_drained_total` | Counter | Events drained during shutdown |

### Prometheus Queries

```promql
# Shutdown success rate (last 24 hours)
sum(rate(eventsource_shutdown_completed_total{outcome="clean"}[24h])) /
sum(rate(eventsource_shutdown_completed_total[24h]))

# Average shutdown duration by outcome
histogram_quantile(0.95,
  sum(rate(eventsource_shutdown_duration_seconds_bucket[1h])) by (le, outcome)
)

# Forced shutdowns (potential issues)
sum(increase(eventsource_shutdown_completed_total{outcome=~"forced|timeout"}[1h]))

# Events lost during forced shutdown
sum(increase(eventsource_shutdown_events_not_drained_total[1h]))
```

### Alert Recommendations

```yaml
# alerts.yaml
groups:
- name: eventsource-shutdown
  rules:
  - alert: HighForcedShutdownRate
    expr: |
      sum(rate(eventsource_shutdown_completed_total{outcome=~"forced|timeout"}[1h])) /
      sum(rate(eventsource_shutdown_completed_total[1h])) > 0.1
    for: 15m
    labels:
      severity: warning
    annotations:
      summary: "High rate of forced shutdowns"
      description: "More than 10% of shutdowns are being forced"

  - alert: ShutdownDurationExceedsGracePeriod
    expr: |
      histogram_quantile(0.95,
        sum(rate(eventsource_shutdown_duration_seconds_bucket[1h])) by (le)
      ) > 40
    for: 15m
    labels:
      severity: critical
    annotations:
      summary: "Shutdown duration approaching grace period"
      description: "P95 shutdown duration is {{ $value }}s, may exceed terminationGracePeriodSeconds"
```

### Structured Logging

Configure structured logging for shutdown events:

```python
import logging
import json

class ShutdownEventLogger:
    """Log shutdown events in structured format."""

    def __init__(self):
        self.logger = logging.getLogger("eventsource.shutdown")

    def log_shutdown_started(self, coordinator):
        self.logger.info(
            "Shutdown sequence started",
            extra={
                "event": "shutdown_started",
                "timeout": coordinator.timeout,
                "drain_timeout": coordinator.drain_timeout,
                "deadline": coordinator.deadline.isoformat() if coordinator.deadline else None,
            }
        )

    def log_shutdown_completed(self, result):
        self.logger.info(
            "Shutdown sequence completed",
            extra={
                "event": "shutdown_completed",
                "phase": result.phase.value,
                "duration_seconds": result.duration_seconds,
                "events_drained": result.events_drained,
                "checkpoints_saved": result.checkpoints_saved,
                "forced": result.forced,
                "reason": result.reason.value if result.reason else None,
                "events_not_drained": result.events_not_drained,
            }
        )
```

---

## Spot Instance Considerations

### AWS Spot Instances

AWS provides a 2-minute (120 second) termination notice for Spot instances.

**Recommended Configuration**:

```yaml
terminationGracePeriodSeconds: 110

env:
- name: SHUTDOWN_TIMEOUT
  value: "100"
- name: DRAIN_TIMEOUT
  value: "60"
- name: CHECKPOINT_TIMEOUT
  value: "10"
```

**Integration with AWS Node Termination Handler**:

The [AWS Node Termination Handler](https://github.com/aws/aws-node-termination-handler) can notify pods before spot termination:

```python
import aiohttp
from datetime import UTC, datetime, timedelta

async def check_spot_termination():
    """Poll EC2 metadata for spot termination notice."""
    url = "http://169.254.169.254/latest/meta-data/spot/termination-time"

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, timeout=1) as response:
                if response.status == 200:
                    termination_time = await response.text()
                    # Parse and set deadline
                    deadline = datetime.fromisoformat(termination_time.replace("Z", "+00:00"))
                    coordinator.set_shutdown_deadline(deadline)
                    coordinator.request_shutdown()
                    return True
        except Exception:
            pass
    return False
```

### GCP Preemptible VMs

GCP provides only 30 seconds termination notice for preemptible VMs.

**Recommended Configuration**:

```yaml
terminationGracePeriodSeconds: 25

env:
- name: SHUTDOWN_TIMEOUT
  value: "20"
- name: DRAIN_TIMEOUT
  value: "10"
- name: CHECKPOINT_TIMEOUT
  value: "5"
```

**Important**: With only 30 seconds, prioritize checkpointing over draining:

```python
# For GCP preemptible, prioritize checkpoints
coordinator = ShutdownCoordinator(
    timeout=20.0,
    drain_timeout=10.0,
    checkpoint_timeout=8.0,  # Higher relative to drain
    checkpoint_interval=3.0, # More frequent during drain
)
```

### Azure Spot VMs

Azure Spot VMs can receive termination notices through Scheduled Events API.

**Integration Example**:

```python
import aiohttp

async def check_azure_scheduled_events():
    """Check Azure Scheduled Events for termination."""
    url = "http://169.254.169.254/metadata/scheduledevents"
    headers = {"Metadata": "true"}

    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                data = await response.json()
                for event in data.get("Events", []):
                    if event.get("EventType") == "Preempt":
                        # Azure gives ~30 seconds
                        deadline = datetime.now(UTC) + timedelta(seconds=30)
                        coordinator.set_shutdown_deadline(deadline)
                        coordinator.request_shutdown()
                        return True
    return False
```

---

## Draining Background Snapshots

### When Using BackgroundSnapshotStrategy

If your application uses background snapshots for aggregates, pending snapshots need special handling during shutdown:

```python
from eventsource import ShutdownCoordinator
from eventsource.repositories import EventSourcedRepository

# Create repository with background snapshots
repository = EventSourcedRepository(
    event_store=event_store,
    snapshot_store=snapshot_store,
    snapshot_strategy=BackgroundSnapshotStrategy(interval=100),
)

shutdown_coordinator = ShutdownCoordinator()

# Register repository cleanup as post-shutdown hook
async def drain_pending_snapshots(result):
    """Drain pending background snapshots before final exit."""
    count = await repository.await_pending_snapshots(timeout=5.0)
    logger.info(f"Drained {count} pending snapshots during shutdown")

shutdown_coordinator.on_post_shutdown(drain_pending_snapshots)
```

### Pre-Shutdown Hook for External Services

Use pre-shutdown hooks to notify external services before shutdown begins:

```python
async def deregister_from_service_discovery():
    """Deregister from Consul/etcd/etc before shutdown."""
    await service_discovery.deregister(instance_id)
    logger.info("Deregistered from service discovery")

async def flush_metrics():
    """Flush pending metrics before shutdown."""
    await metrics_exporter.flush()

# Register with appropriate timeouts
coordinator.on_pre_shutdown(deregister_from_service_discovery, timeout=5.0)
coordinator.on_pre_shutdown(flush_metrics, timeout=2.0)
```

---

## Troubleshooting

### Common Issues

#### 1. Pods Killed Before Shutdown Completes

**Symptoms**:
- `SIGKILL` in logs
- Events not fully drained
- Checkpoints not saved

**Diagnosis**:
```bash
# Check termination reason
kubectl describe pod <pod-name> | grep -A5 "Last State"

# Check shutdown duration metrics
kubectl logs <pod-name> | grep "Shutdown complete"
```

**Solutions**:
- Increase `terminationGracePeriodSeconds`
- Reduce `shutdown_timeout` to fit within grace period
- Check if preStop hook is too slow

#### 2. Events Not Draining

**Symptoms**:
- `events_drained: 0` in shutdown logs
- `events_not_drained > 0` in metrics

**Diagnosis**:
```bash
# Check drain timeout
kubectl logs <pod-name> | grep "Drain phase timed out"

# Check in-flight count
kubectl logs <pod-name> | grep "in_flight_at_start"
```

**Solutions**:
- Increase `drain_timeout`
- Check if event handlers are blocking
- Review subscription batch sizes

#### 3. Readiness Probe Race Conditions

**Symptoms**:
- Traffic still arriving during shutdown
- "Connection refused" errors

**Diagnosis**:
```bash
# Check endpoint removal timing
kubectl get endpoints <service-name> -w
```

**Solutions**:
- Add preStop sleep: `sleep 5`
- Reduce readiness probe `periodSeconds`
- Use `failureThreshold: 1` for fast removal

#### 4. Double-Signal Forced Shutdown

**Symptoms**:
- `reason: double_signal` in logs
- `forced: true` in shutdown result

**Diagnosis**:
```bash
kubectl logs <pod-name> | grep "second shutdown signal"
```

**Solutions**:
- Usually indicates deployment/scaling too aggressive
- Check if automated systems are sending multiple signals
- Review deployment strategy settings

### Debug Commands

```bash
# Watch pod termination
kubectl get pods -w -l app=eventsource-worker

# Check events during termination
kubectl get events --field-selector involvedObject.name=<pod-name>

# Examine shutdown logs
kubectl logs <pod-name> --previous | grep -E "(shutdown|SIGTERM|drain|checkpoint)"

# Check terminationGracePeriodSeconds
kubectl get pod <pod-name> -o jsonpath='{.spec.terminationGracePeriodSeconds}'

# Simulate termination (test shutdown)
kubectl delete pod <pod-name> --grace-period=45

# Force immediate termination (emergency only)
kubectl delete pod <pod-name> --grace-period=0 --force
```

### Log Analysis Patterns

```bash
# Find shutdown initiation
grep "Received shutdown signal" <log-file>

# Track shutdown phases
grep -E "Shutdown phase:" <log-file>

# Check for timeout issues
grep -E "(timed out|timeout)" <log-file>

# Verify checkpoint saves
grep "Checkpoints saved successfully" <log-file>

# Find forced shutdowns
grep -E "(forced.*true|FORCED)" <log-file>
```

---

## Best Practices Checklist

Use this checklist before deploying to production:

### Configuration

- [ ] `terminationGracePeriodSeconds` exceeds `shutdown_timeout + preStop_duration + 10s`
- [ ] Environment variables configured for shutdown timeouts
- [ ] preStop hook includes appropriate sleep duration (5-10s)
- [ ] ConfigMap/Secrets properly mounted for database credentials

### Health Probes

- [ ] Readiness probe configured with `failureThreshold: 1`
- [ ] Liveness probe configured with `failureThreshold: 3` (lenient)
- [ ] Health endpoints implemented and tested
- [ ] Startup probe configured if application has slow initialization

### High Availability

- [ ] PodDisruptionBudget configured
- [ ] Multiple replicas deployed
- [ ] Pod anti-affinity or topology spread constraints configured
- [ ] Resource requests and limits set appropriately

### Monitoring

- [ ] Shutdown metrics exported to monitoring system
- [ ] Alerts configured for forced shutdowns
- [ ] Structured logging enabled for shutdown events
- [ ] Dashboard includes shutdown duration trends

### Testing

- [ ] Tested rolling update with graceful shutdown
- [ ] Tested pod termination sequence
- [ ] Verified health probe behavior during shutdown
- [ ] Simulated spot instance termination (if applicable)

### Security

- [ ] Security context configured (non-root, read-only filesystem)
- [ ] Secrets not exposed in environment variable logs
- [ ] Network policies configured if required

---

## See Also

- [Production Deployment Guide](production.md) - General production deployment best practices
- [Subscriptions Guide](subscriptions.md) - Subscription manager configuration
- [Observability Guide](observability.md) - Monitoring and tracing setup
- [Error Handling Guide](error-handling.md) - Error handling patterns

### External Resources

- [Kubernetes Pod Lifecycle](https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/)
- [Kubernetes Best Practices: Terminating with Grace](https://cloud.google.com/blog/products/containers-kubernetes/kubernetes-best-practices-terminating-with-grace)
- [AWS Node Termination Handler](https://github.com/aws/aws-node-termination-handler)
- [GCP Preemptible VM Documentation](https://cloud.google.com/compute/docs/instances/preemptible)
- [Azure Spot VM Documentation](https://docs.microsoft.com/en-us/azure/virtual-machines/spot-vms)
