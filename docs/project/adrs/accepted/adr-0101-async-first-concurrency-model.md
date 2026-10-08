---
id: '0101'
title: Async-First Concurrency Model
status: Accepted
target_bc: core
governing_prds:
- PRD-0001
governing_stories:
- US-0001
- US-0002
- US-0003
---

# ADR-0101: Async-First Concurrency Model

## Summary
Pure asyncio core throughout stores, buses, runners, and repositories.

## Context
Event-driven architectures and event sourcing systems are fundamentally I/O bound: persisting event batches, publishing across message brokers, driving streaming subscriptions, and polling feeds. Synchronous, blocking implementations under Python introduce thread starvation and poor scaling under concurrent workloads.

## Decision
1. All core library interfaces (ports, application services, domain state rehydration, adapters) are natively asynchronous (`async def`).
2. Synchronous facades and test helpers live strictly in outer adapters or testing utilities (`testing/sync_facade.py`) for developer convenience without compromising core asynchronous purity.
3. Concurrency primitives across all subsystems rely on `asyncio.Lock`, `asyncio.Event`, and cooperative task scheduling.

## Consequences
- Maximum throughput and non-blocking I/O across database and broker adapters.
- Clean integration with modern Python async web frameworks (FastAPI, Starlette, Litestar).
- Eliminates thread safety race conditions in shared state repositories.
