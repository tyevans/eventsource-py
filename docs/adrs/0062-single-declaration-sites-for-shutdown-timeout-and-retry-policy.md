# 0062. Single declaration sites for shutdown timeout and retry policy

- Status: Accepted
- Date: 2026-08-09

## Context

Two facts in this library were each stored in more than one place, with nothing
that failed when the copies disagreed — the defect shape recorded in
`.claude/rules/recurring-defects.md` §2.

**Shutdown timeout.** `SubscriptionConfig` declared a `shutdown_timeout` field
with a default and a positivity check, and `SubscriptionManager.__init__`
declared a `shutdown_timeout` argument with the same default. Only the
constructor argument reached anything: it is what the manager hands to its
`ShutdownCoordinator`, and what `run_until_shutdown()` and `stop_all()` override
per call. The config field had no reader anywhere in the tree. A user who set it
got a validated value, a truthful attribute read-back, and no behavior change —
the failure mode is silent, and the documentation had already been reduced to
explaining which copy loses.

**Retry policy.** Two unrelated types were both named `RetryPolicy`:
`eventsource.adapters._bus.retry.RetryPolicy`, a frozen dataclass computing
broker redelivery backoff, and `eventsource.application.projections.retry.RetryPolicy`,
a `runtime_checkable` Protocol describing projection retry decisions. The
top-level package exports the bus dataclass, so `from eventsource import RetryPolicy`
resolved to the dataclass — while the annotations users actually encounter, the
keyword-only `retry_policy` parameter on `DatabaseProjection`, `ReadModelProjection`,
`DeclarativeProjection`, and `CheckpointTrackingProjection`, all referred to the
Protocol. The two are structurally incompatible: the dataclass has `delay_for`,
the Protocol wants `max_retries`, `get_backoff`, and `should_retry`. Following the
export produced an argument the type checker rejects and the projection loop
cannot call. Nothing in either type's tests could notice, because each was
imported by its own full path.

## Decision

**A fact has one declaration site.**

`SubscriptionConfig.shutdown_timeout` is deleted, along with its validation rule
and docstring entry. `SubscriptionManager.__init__(shutdown_timeout=...)` is the
sole declaration; per-call overrides on `stop_all()` and `run_until_shutdown()`
remain, because a one-off override at the call site is not a second declaration
of the default. Shutdown is a manager-wide concern and never varied per
subscription, so there is nothing the deleted field could have expressed that
the surviving one cannot.

**A name identifies one type.**

The projections Protocol is renamed to `ProjectionRetryPolicy`. The bus dataclass
keeps the bare `RetryPolicy` name and the top-level export, because that export
is the one users are told about and the one the changelog documents. Renaming the
Protocol rather than removing the export preserves both surfaces and makes the
distinction visible at every annotation, which is where the confusion arose. The
Protocol is not added to the top-level package: `eventsource.application.projections`
deliberately does not re-export the retry module, and the deep import remains the
documented way to reach it.

## Consequences

Both changes are breaking, and ship without deprecation shims per the project's
standing pre-1.0 policy.

- Constructing `SubscriptionConfig(shutdown_timeout=...)` now raises `TypeError`
  instead of silently doing nothing. That is the intended improvement: the
  failure moves from invisible to immediate. Callers move the value to the
  manager constructor, where it always belonged.
- Code importing `RetryPolicy` from `eventsource.application.projections.retry`
  must import `ProjectionRetryPolicy`. The concrete implementations
  (`ExponentialBackoffRetryPolicy`, `NoRetryPolicy`, `FilteredRetryPolicy`,
  `DEFAULT_RETRY_POLICY`) are unchanged, so structural users of the policy are
  unaffected — only explicit annotations against the Protocol move.
- `from eventsource import RetryPolicy` continues to resolve to the bus
  dataclass, and now that is the only thing with the name.
- A regression test pins the deletion: it asserts `SubscriptionConfig` has no
  `shutdown_timeout` attribute and that passing the keyword raises. Re-adding the
  second declaration site fails the suite rather than passing quietly.

## Related

- Amends `0022` and the subscription configuration surface described in
  `docs/api/subscriptions.md`.
- Extends `0048`, which established the shared bus `RetryPolicy` whose name this
  ADR arbitrates.
