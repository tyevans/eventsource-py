# ADR 8: Subscription Engine, Feed-Driven Checkpointing, and Ordered Sequential Delivery

## Summary
Feed-driven checkpointing, sequential subscriber delivery, and page-based batching.

## Context
Driving streaming subscriptions solely from message bus events leads to missed events if the consumer is offline or if bus partitions rebalance. Checkpoints must be durable and decoupled from broker transient state.

## Decision
1. `LiveRunner` and `CatchupRunner` checkpointing is feed-driven from `GlobalEventFeed`, treating the message bus purely as an efficient wake-up notification.
2. Strict per-subscription sequential delivery guarantees: an event is never dispatched to subscriber $N+1$ until event $N$ finishes successfully.
3. Live batch delivery operates in discrete fixed pages rather than sliding time windows.
4. Single source of truth for shutdown timeouts and exponential backoff retry policies.

## Consequences
- Subscriptions can pause, catch up from stream start, or recover from outages without event loss.
- Predictable stream ordering without out-of-order race conditions.
- Reliable catchup performance without unbounded memory consumption.
