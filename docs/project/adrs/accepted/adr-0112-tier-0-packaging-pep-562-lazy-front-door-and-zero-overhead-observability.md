---
id: '0112'
title: Tier-0 Packaging, PEP 562 Lazy Front Door, and Zero-Overhead Observability
status: Accepted
target_bc: core
governing_prds:
- PRD-0001
- PRD-0004
governing_stories:
- US-0013
- US-0015
---

# ADR-0112: Tier-0 Packaging, PEP 562 Lazy Front Door, and Zero-Overhead Observability

## Summary
Lightweight base install, named extras, lazy frontdoor loading, and no-op tracing.

## Context
A core framework should not force users to install heavy database drivers (psycopg, asyncpg, aiosqlite), message broker clients (kafka-python, pika, redis), or tracing collectors (opentelemetry) if they only need in-memory or domain modeling.

## Decision
1. Tier-0 core packaging: base `eventsource` install requires only pure dependencies (`pydantic`, `sqlalchemy`).
2. All infrastructure drivers live behind explicit optional extras (`postgres`, `sqlite`, `kafka`, `rabbitmq`, `redis`, `telemetry`).
3. PEP 562 lazy dynamic loading in `eventsource/__init__.py`: bare `import eventsource` is lightning fast and does not trigger backend imports.
4. Top-level `__all__` is byte-identical across lazy execution and static type checking.
5. Distributed tracing via OpenTelemetry is completely no-op by default with zero performance overhead when disabled.

## Consequences
- Minimal package footprint and ultra-fast application startup.
- Clean dependency tree without silent transitive bloat.
- Enterprise-grade OpenTelemetry observability without forced dependencies.
