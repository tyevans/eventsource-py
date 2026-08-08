# ADR 0058: `EventSourceError` is the universal base for library exceptions

## Status

Accepted

Amends ADR 0029 and ADR 0032, which rebased the lock and subscription
families onto `EventSourceError` one family at a time. This ADR generalises
the rule rather than repeating it a third time.

## Context

`EventSourceError` is documented as "Base exception for eventsource library",
and both the API reference and the error-handling guide taught callers to use
`except EventSourceError` as the library-boundary handler.

The claim was false. Several exception families derived directly from
`Exception` instead — the snapshot family (whose members are re-exported from
the top-level package, so they are the *first* exceptions many callers meet),
the read-model family, the subscription retry and circuit-breaker errors,
the migration write-pause and store-routing errors, and the RabbitMQ shutdown
and batch-publish errors.

A consumer writing the obvious boundary handler therefore silently missed
every snapshot and read-model failure, which escaped to whatever generic
500 handler sat above. Nothing failed when the docs and the class definitions
disagreed, which is the project's most common defect shape
(`.claude/rules/recurring-defects.md`, "one fact stored in two places with
nothing that fails when the copies disagree").

The prior position was recorded in a comment on `SnapshotError` claiming the
`Exception` base was a deliberately preserved pre-move contract. That
preserved a contract nobody had asked for at the cost of the one the docs
actually promised.

## Decision

Every exception class defined under `src/eventsource/` inherits from
`EventSourceError`.

Where a class also carries a stdlib base that callers may reasonably catch
(for example `ImportError`), that base is kept alongside rather than replaced:
`class RabbitMQNotAvailableError(EventSourceError, ImportError)`. Rebasing is
a widening change — every existing `except <ConcreteError>` and
`except Exception` keeps working, and the clause that newly catches is
`except EventSourceError`, which previously caught nothing in these families.

One category is exempt: the optional-dependency import sentinels
(`RedisNotAvailableError`, `KafkaNotAvailableError`,
`SQLiteNotAvailableError`). They are raised only when an extra is not
installed, so their contract is that a missing extra is indistinguishable from
a missing import. They remain `ImportError` subclasses. The exemption is
enumerated explicitly, not inferred.

The rule is enforced by a guard test that walks the package and asserts the
property from two directions — the top-level public surface and every class
defined anywhere in the tree — and that additionally asserts the exemption
list contains no stale entries, so the list cannot decay into a blanket
suppression.

## Consequences

- `except EventSourceError` is a reliable library-boundary handler. Docs that
  say so are now backed by a test rather than by prose.
- Adding a new exception family off to the side is a test failure, not a
  silent regression. New families must either inherit `EventSourceError` or
  argue their way onto the exemption list.
- Handlers that previously relied on a snapshot or read-model error escaping
  an `except EventSourceError` clause would now swallow it. No such handler
  exists in the tree; downstream code that catches `EventSourceError` and
  suppresses rather than re-raises should be checked once on upgrade.
- The exemption for import sentinels means `except EventSourceError` is not
  literally total. That is the right trade: those errors surface at import
  time, before any library boundary handler is on the stack.
