# ADR 7: Uniform Message Bus Contract and At-Least-Once Delivery Semantics

## Summary
Uniform EventBus protocol across brokers with at-least-once delivery and DLQ isolation.

## Context
Different messaging technologies (Kafka, RabbitMQ, Redis, Memory) have divergent delivery models. Applications need consistent delivery guarantees, error isolation, and poison-pill containment.

## Decision
1. Uniform `EventBus` port across all broker adapters supporting typed message envelopes (`EventEnvelope[E]`).
2. At-least-once delivery semantics: messages are acknowledged or committed only after handler dispatch succeeds.
3. Handler errors are isolated and aggregated in `HandlerDispatchError`; broker offset is not committed on failure.
4. Bounded background publishing with automatic fallback to synchronous inline execution when backpressure queues fill.
5. Dedicated `DeadLetterQueue` port for unprocessable poison-pill messages.

## Consequences
- Zero silent message drops on broker network or consumer failures.
- Swappable broker implementations without rewriting subscriber handlers.
- Poison-pill messages are safely quarantined without stalling stream processing.
