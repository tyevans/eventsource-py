# 0011 - Uniform Handler-Error Isolation with `HandlerDispatchError` and No-Ack-on-Failure

## Status

Accepted (2026-07-29). Implemented by `eventsource.exceptions.HandlerDispatchError`
and its use on the consume paths of `src/eventsource/bus/redis.py`,
`rabbitmq.py`, and `kafka.py`. `src/eventsource/bus/memory.py` is unaffected:
`InMemoryEventBus.publish()` continues to swallow-and-log per 0007 D3.

This ADR **amends** [0007 - Event Bus Delivery Semantics and Tracing
Contract](0007-event-bus-delivery-semantics.md), specifically D3 ("Handler
errors are caught, logged, and swallowed") and the "For users writing
handlers" consequence ("Do not expect `await bus.publish(...)` to raise when a
handler fails: it will not"). D3's isolation principle stands; its claim that
failures are swallowed without ever surfacing to the caller, and its
description of retry/DLQ as the only place a failure becomes visible, no
longer hold for the broker consume paths. 0007's Status section carries a
pointer to this ADR.

## Context

`docs/superpowers/specs/2026-07-29-event-bus-contract-and-coverage-design.md`
stated, as a design decision, that "Error handling is unchanged. Handler
errors stay isolated and logged in `_safe_handle`; the DLQ and retry paths
keep their current structure." That held for `InMemoryEventBus` and for
Redis's original implementation.

While porting DLQ repository tests and writing the conformance suite (ledger
Task 8), a concrete divergence surfaced: `RedisEventBus`'s consume path acked
a message (`XACK`) even when a subscribed handler raised, because
`_safe_handle` swallowed the exception before the caller's ack/nack decision
was made. A handler that fails is, from the consumer group's perspective,
indistinguishable from one that succeeded -- the message is gone from the
pending list and will not be retried or redelivered, only logged. `RabbitMQ`
and `Kafka` had the same shape of gap on their own consume paths.

This is a correctness gap, not a style question: at-least-once delivery (0007
D1) is the guarantee that lets handlers recover from a failed attempt via
redelivery. Acking on a failed handler quietly converts at-least-once into
at-most-once for exactly the failure case redelivery exists to cover.

The user, presented with the divergence, ruled explicitly: the isolation
contract is uniform across backends, and the fix belongs in the code, not in
a docstring documenting the difference. Each backend was then fixed in turn
(Redis in Task 8, RabbitMQ in Task 9, Kafka in Task 10) to the same shape.

## Decision

### Every backend still runs all handlers per delivery

D3's isolation principle is unchanged and reaffirmed: one handler's failure
does not prevent other handlers subscribed to the same event from running.
`asyncio.gather(..., return_exceptions=True)` (or the equivalent) is still
used so a slow or failing audit handler cannot block a read-model handler on
the same event.

### Failures are aggregated into `HandlerDispatchError`

Instead of being swallowed after logging, one or more handler failures for a
single dispatch are collected and raised as a single
`eventsource.exceptions.HandlerDispatchError` (a new public export), which
carries the list of underlying `failures`. Each backend still emits the same
ERROR log line and `ATTR_HANDLER_SUCCESS = False` span per failed handler that
0007 D3 describes -- this ADR adds a raised exception on top of those
signals, it does not remove them.

When exactly one handler fails, the broker consume paths (`RabbitMQ`, `Kafka`)
unwrap the single failure before it reaches their existing retry/DLQ metadata
code (`x-dlq-error-type` on RabbitMQ, `dlq_error_type` on Kafka), so a lone
failure's error type and message are preserved in DLQ metadata exactly as
before this change -- callers inspecting DLQ entries see no format change for
the single-failure case.

### The raise is a broker consume-path contract, not a `publish()` contract

`HandlerDispatchError` is raised by the dispatch layer specifically on the
three brokers' **consume** paths -- `redis.py:777`, `rabbitmq.py:3227`, and
`kafka.py:2537` -- where dispatch happens as part of processing a delivered
message inside `start_consuming`, immediately before the ack/commit decision.
It is not raised from any backend's `publish()`. `InMemoryEventBus.publish()`
dispatches directly (there is no separate consume loop) and keeps 0007's
swallow-and-log contract unchanged: `_safe_handle` still catches, logs, and
does not raise, and `publish()` does not raise due to a handler failure. This
is not a divergence from the uniform-isolation ruling -- the ruling's
substance, that every handler runs regardless of an earlier handler's
failure, has always held for InMemory and continues to hold everywhere. What
differs is only whether there is an ack/commit for the aggregate failure to
withhold, and InMemory has none.

`RedisEventBus`, `RabbitMQEventBus`, and `KafkaEventBus` let
`HandlerDispatchError` propagate out of dispatch on their consume loops,
which prevents the ack/commit that would otherwise follow a successful
delivery:

- **Redis**: no `XACK`; the entry stays in the consumer group's pending list
  and is reclaimed by the existing `XCLAIM`-based redelivery path once idle
  past `pending_idle_ms`.
- **RabbitMQ**: no `ack()`; the existing `_handle_failed_message` retry path
  (incrementing `x-retry-count`, backoff, eventual `_send_to_dlq`) runs as it
  already did for other failure types.
- **Kafka**: `commit()` is not called after a failed dispatch; the consumer
  re-reads the uncommitted offset on restart or rebalance, same as any other
  processing failure.

Each backend's existing retry-count/backoff/DLQ machinery is unchanged by
this ADR; the change is only that a handler failure now reaches that
machinery instead of being absorbed before it.

`InMemoryEventBus` has no ack/commit step and no consume loop, so this
decision does not apply to it: `publish()` keeps 0007's "will not raise due
to handler failure" guidance unchanged, on every path.

## Consequences

### For users writing handlers

For applications consuming from Redis, RabbitMQ, or Kafka, a handler
exception now results in redelivery (subject to each backend's existing
`max_retries` and DLQ routing) rather than silent, permanent loss of that
handler's side effect. Handlers must already be idempotent per 0007 D1, so
this closes a correctness gap rather than introducing a new burden.

`await bus.publish(...)` on `InMemoryEventBus` still does not raise due to
handler failure -- 0007's guidance there is unchanged. Code that consumes
from a broker via `start_consuming` and inspects processing internals (or
subclasses a consume loop) may now observe `HandlerDispatchError` where it
previously observed nothing; code that only calls `publish()` sees no new
exception on any backend.

### For contributors adding a fifth adapter

Run all handlers per delivery; collect failures rather than aborting on the
first one; raise `HandlerDispatchError` with the collected failures when any
occurred; and on any consume/commit/ack path, let that exception prevent the
ack so the backend's own redelivery mechanism takes over. If more than one
failure can be unwrapped to a single error for DLQ metadata purposes, follow
the existing single-failure-unwrap pattern in `rabbitmq.py` and `kafka.py`.

### For 0007 D3 and D1

D3's "Handler errors are caught, logged, and swallowed" no longer accurately
describes the broker consume paths: errors are caught (isolated across
handlers), logged, and raised as `HandlerDispatchError`, and on brokers that
raise prevents the ack that D1's at-least-once guarantee depends on to
recover. D1 itself is strengthened, not changed: this ADR is what makes
at-least-once actually hold in the handler-failure case, which the prior
implementation on Redis silently did not.

## Alternatives Considered

### Document the per-backend divergence instead of fixing it

Rejected by explicit user ruling. `EventBus` is a shared abstraction
precisely so applications can move between backends without rewriting
handlers (0007's stated design goal); an isolation contract that differs by
backend defeats that goal more thoroughly than the redelivery/idempotency
requirement it would have avoided. Documenting "Redis acks on handler failure,
RabbitMQ and Kafka do not" would have been accurate but would leave a trap for
every future backend switch.

### Abort dispatch on the first handler failure (fail-fast)

Rejected, for the same reason 0007 rejected it originally: it would couple
independent subscribers and make behavior depend on registration order,
since `asyncio.gather` collects in task order. This ADR keeps D3's isolation
principle; it changes only what happens to the *aggregate* result of
isolated dispatch, not whether handlers run.

### Keep swallowing failures but improve logging/metrics only

Rejected. Better observability does not change what the consumer group,
queue, or partition offset records happened, so it would not close the
at-most-once regression on the broker consume paths. The fix has to change
the ack/commit decision itself.

## References

- `src/eventsource/exceptions.py` -- `HandlerDispatchError`
- `src/eventsource/bus/redis.py`, `rabbitmq.py`, `kafka.py` -- unwrap-and-raise
  at the consume path, DLQ metadata preservation for the single-failure case
- `src/eventsource/bus/memory.py` -- unaffected; `publish()` continues to
  swallow-and-log per 0007 D3, no `HandlerDispatchError` import or raise
- `docs/adrs/0007-event-bus-delivery-semantics.md` -- D1, D3; amended by this
  ADR
- `.superpowers/sdd/2026-07-29-event-bus-contract-and-coverage/progress.md` --
  Task 8 (Redis), Task 9 (RabbitMQ), Task 10 (Kafka) entries recording the
  user ruling
