# 0052. The Global Feed Filters By Aggregate Type

`FeedReadOptions` could restrict a feed read to one tenant, but not to one
aggregate type. A consumer interested in a single type had to read the whole
global feed and discard the rest in Python.

## Status

**Accepted.**

**ADR Impact**

| ADR | Status |
| --- | --- |
| [0025](0025-legacy-store-retirement.md) | Stands. The narrowing of the retired `ReadOptions` into three purpose-built option objects is the reason this field goes on `FeedReadOptions` rather than being reintroduced as a general one. |
| [0027](0027-schema-correctness-fixes.md) | Stands. `FeedReadOptions.tenant_id` and its `uuid` binds are untouched. |
| [0046](0046-aggregate-type-single-source.md) | Stands, and constrains this. The filter names an existing fact — the stream category, stored in the `aggregate_type` column — rather than introducing a second declaration of it. |

## Context

The store ports separate two ways of reading more than one stream.
`read_category` answers "the events of this aggregate type", ordered by storage
time. `read_all` answers "the store's events in global order", the ordering a
subscription resumes from. A consumer that wants *both* — one type, in
resumable global order — could express only the second, and filtered the first
in its own loop.

That is not a stylistic preference on the consumer's part. A subscription or
projection scoped to one aggregate type, in a store whose file or database is
shared with other aggregate types, has no other option: the type filter cannot
be pushed anywhere, so every consumer scans the entire log and throws most of
it away. The cost grows with the events it is *not* interested in.

Both SQL adapters already store the fact being filtered on, in an indexed
column, on the same table the feed reads.

## Decision

`FeedReadOptions` gains `aggregate_type: str | None = None`, alongside
`tenant_id`. `None` means all types, matching `tenant_id`'s convention that
`None` is "no scoping" rather than "unknown".

The adapters translate it into a predicate on the same query that already
handles `from_position`, `tenant_id`, and `limit` — a `WHERE` clause in
PostgreSQL and SQLite, a list comprehension in the in-memory store. Ordering,
resumption, and the meaning of `limit` are unchanged: `limit` bounds the events
returned *after* filtering, so a position taken from one page resumes the
filtered sequence rather than skipping the rows that were filtered out.

The in-memory adapter filters on `EventEnvelope.stream_id.category` rather than
`event.aggregate_type`, because that is what the SQL adapters write into the
`aggregate_type` column. One fact, read from the place each adapter stores it.

The semantics are pinned in `GlobalFeedConformance`, not per-adapter tests.

PostgreSQL gains a composite `(aggregate_type, global_position)` index, since
the filter and the ordering are now used together. SQLite needs no counterpart:
`global_position` is its rowid, so the existing `aggregate_type` index is
already ordered by it.

## Consequences

A type-scoped feed read is an indexed read rather than a full-log scan, and the
filtering happens where the data is instead of in the consumer.

`FeedReadOptions` is now the wider of the two read-option objects that name an
aggregate type, and the difference between it and `read_category` is a matter
of ordering, not of what can be selected. That difference is the thing to state
in the docs — a reader who takes them as interchangeable will get the right
events in the wrong order, and a position that does not resume.

The field is additive with a default, so existing callers are unaffected and
existing stored data needs no migration. A custom adapter implementing
`GlobalEventFeed` that ignores the new field will silently return too much;
the conformance suite catches it the moment the adapter runs the suite, which
is what running it is for.
