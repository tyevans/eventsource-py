---
id: '0003'
title: Publish and Subscribe to Events via Message Buses
status: Accepted
created: 2026-10-07
persona: Jordan (The Backend Platform Engineer)
target_bc: adapters
feature: FEAT-BUS-ADAPTERS
governing_prd: PRD-0001
scenarios:
  - Publish domain event to bus and dispatch to subscriber
  - Dispatch event to Dead Letter Queue on unrecoverable failure
---

# US-0003 — Publish and Subscribe to Events via Message Buses

## Governing PRD
- [`PRD-0001: Production-Ready Event Sourcing and Live Migration Framework`](../../product/accepted/prd-0001-production-ready-event-sourcing-framework.md)

## User Story

**As a** backend platform engineer (Jordan),
**I want** to publish and subscribe to domain events over Redis, RabbitMQ, Kafka, or InMemory message buses,
**So that** downstream subscribers receive events with at-least-once delivery guarantees and unrecoverable handler errors route to dead-letter queues.

## Acceptance Criteria

```gherkin
Scenario: Publish domain event to bus and dispatch to subscriber
  Given an active "EventBus" adapter and a registered async subscriber for "OrderShipped"
  When an "OrderShipped" event is published to the bus
  Then the subscriber handler is invoked with the deserialized event within timeout bounds.
```

```gherkin
Scenario: Dispatch event to Dead Letter Queue on unrecoverable failure
  Given a subscriber handler configured with max retries 2 that fails consistently
  When a new event is received by the consumer loop
  Then the event is retried twice and subsequently written to the DLQ repository
  And consumer acknowledgement is processed without blocking other messages.
```
