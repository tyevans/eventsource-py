# 0049. Snapshots Fire on Crossing a Boundary, Not Landing on One

`EveryNEvents(n)` asked whether the aggregate's version was an exact multiple
of `n`. ADR 0017 recorded the consequence and accepted it: a save that jumps
the version across a multiple without landing on it takes no snapshot until the
next boundary. ADR 0021 carried the caveat over verbatim.

The caveat understates the failure. "Until the next boundary" assumes some
later save lands on one. For an aggregate whose saves advance the version by a
constant stride, that assumption can be false forever, and the policy is not
late — it never fires at all.

## Status

**Accepted.**

**ADR Impact**

| ADR | Status |
| --- | --- |
| [0017](0017-snapshot-strategy-pattern.md) | Stands as a historical record — already superseded by ADR 0021. Its "straddles the boundary … acceptable because snapshots are an optimization" consequence is the reasoning this ADR corrects, but the record stays as written. |
| [0021](0021-snapshot-policy-scheduler-composition.md) | Amended — `EveryNEvents(n)` changes from `version % n == 0` to a crossing test. The composition ADR 0021 decided (policy decides *when*, scheduler decides *where*, `take_snapshot` does the work) is untouched; only the predicate changes. |

## Context

A save carries every event a command produced. An aggregate that emits several
events per command therefore advances its version in strides, not one at a
time — which is the normal case, not an exotic one.

Whether a strided sequence ever lands on a multiple of `n` is a question of
arithmetic, and the answer is often no. For a constant stride `s` from a
starting version `v₀`, some version is a multiple of `n` only if
`gcd(s, n)` divides `−v₀ mod n`. A stream that starts at version 1 (a creation
event) and advances by 6 (a command emitting six events) against `n = 50` has
`gcd(6, 50) = 2`, which does not divide 49: no version in that sequence is ever
even, and every multiple of 50 is. Such a stream snapshots exactly never, no
matter how long it runs.

Nothing failed. The snapshot store stayed empty, every load replayed the full
history, and the only symptom was a load that grew slowly slower. Tests passed
because every existing test saved a batch whose size divided the threshold, so
the version landed on the multiple and the branch was taken. This is the
"inert code" shape from `.claude/rules/recurring-defects.md` §3: a branch never
taken passes every test that does not assert on it.

## Decision

`EveryNEvents(n)` snapshots when a save moves the aggregate from one interval
of `n` into a later one:

```python
version_before = aggregate.version - events_since_snapshot
return aggregate.version // n > version_before // n
```

A save that crosses several multiples at once still takes one snapshot, at the
version it reached. The intermediate snapshot would be superseded immediately —
the store holds one snapshot per aggregate — so taking it would be work with no
reader.

### What this preserves

The property ADR 0021 wanted from keying off the absolute version is that two
processes saving the same aggregate agree on which save owes the snapshot. That
still holds: the predicate is a pure function of the version reached and the
size of the save, both of which are properties of the save itself, not of the
repository that performed it.

What is no longer true is that snapshots land on a fixed set of versions —
`{n, 2n, 3n, …}`. With crossing, the version at which a snapshot is taken
depends on how the events were batched. This costs nothing: snapshots are keyed
by aggregate, one row, overwritten. Nothing reads a snapshot by version, and
nothing may start to.

### Alternatives rejected

**Count events since the last snapshot.** The obvious reading of the parameter
name, and it fires reliably. Rejected for the reason ADR 0017 gave: the count
is per-repository state, so two processes with different histories disagree
about when a snapshot is due, and a fresh repository never snapshots at all.
The version is the only counter both processes can see.

**Snapshot on every save.** Correct, and wrong at any volume — it turns an
optimization into a write on the hot path.

**Leave it and document the arithmetic.** The straddle was already documented
twice, in ADR 0017 and again in ADR 0021, and stayed invisible both times
precisely because it reads as an edge case rather than as "this aggregate shape
never snapshots".

## Consequences

Aggregates that emit several events per command begin snapshotting, most of
them for the first time. A deployment carrying such a stream will see snapshot
writes start where there were none, and loads shorten accordingly.

Snapshots are still an optimization with no guarantee attached: the policy may
be replaced, the store may fail (`take_snapshot` failures do not fail the
save), and a snapshot whose `schema_version` no longer matches is discarded on
read. None of that changes. What changes is that the default policy now fires
for the aggregate shapes it was always meant to serve.

The predicate is now a function of two arguments rather than one. A
`SnapshotPolicy` implementation that ignores `events_since_snapshot` remains
valid — the protocol is unchanged — but the shipped default no longer does.
