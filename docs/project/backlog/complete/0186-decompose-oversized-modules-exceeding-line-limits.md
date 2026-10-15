---
id: "0186"
title: Remove RabbitMQEventBus Private Proxy Shims and Decompose RabbitMQ Topology
status: Complete
target_bc: adapters
governing_adrs:
  - ADR-0002
  - ADR-0003
  - ADR-0107
governing_prds:
  - PRD-0003
governing_stories:
  - US-0003
  - US-0022
priority: P1
---

# TASK-0186: Remove RabbitMQEventBus Private Proxy Shims and Decompose RabbitMQ Topology

## Summary
Remove 14 backward-compatible private proxy properties from `RabbitMQEventBus` (`src/eventsource/adapters/rabbitmq/bus.py`) and decompose `src/eventsource/adapters/rabbitmq/topology.py` (491 lines) into cohesive submodules strictly under 400 lines.

## Problem Statement & Context
- `RabbitMQEventBus` (`src/eventsource/adapters/rabbitmq/bus.py` lines 238-380) defines 14 private `@property` and `@setter` shims (`_tracer`, `_enable_tracing`, `_connection`, `_channel`, `_connected`, `_reconnecting`, `_was_consuming`, `_lock`, `_exchange`, `_dlq_exchange`, `_consumer_queue`, `_dlq_queue`, `_consuming`, `_consumer_task`) solely to proxy state owned by `RabbitMQConnectionManager`, `RabbitMQTopology`, `RabbitMQPublisher`, and `RabbitMQConsumer`.
- `src/eventsource/adapters/rabbitmq/topology.py` is 491 lines, triggering a proactive file-length warning (>=400 lines).

## Required Actions
1. Remove the 14 backward-compatible private proxy properties/setters from `src/eventsource/adapters/rabbitmq/bus.py` and update `tests/unit/test_rabbitmq_event_bus/` to exercise public APIs or explicit collaborators directly.
2. Decompose `src/eventsource/adapters/rabbitmq/topology.py` (491 lines) into focused modules under 400 lines while preserving public exports in `eventsource.adapters.rabbitmq`.

## Definition of Done (Blackbox Frontdoor TDD)
1. Zero backward-compatible private proxy properties remain in `src/eventsource/adapters/rabbitmq/bus.py`.
2. All `src/eventsource/adapters/rabbitmq/` modules are strictly under 400 lines.
3. All unit and integration tests pass cleanly.
