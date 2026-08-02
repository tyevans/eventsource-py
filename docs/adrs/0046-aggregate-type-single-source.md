# 0046. `aggregate_type` Has One Source: the Aggregate Class

## Status

**Accepted.**

## Context

`aggregate_type` was declarable in three independent places for a single
aggregate, with nothing cross-checking them:

1. the aggregate class's `aggregate_type: ClassVar[str]` attribute
   (mandatory — a concrete subclass that never assigns it raises
   `AggregateTypeNotSetError` at construction, per ADR 0043)
2. the `AggregateRepository(aggregate_type=...)` constructor parameter,
   which took precedence over inference when supplied
3. an event class's own `aggregate_type` field default

Set all three to different values and the failure mode is silent and
severe. Reproduced directly:

```
category CLASS_SAYS -> 0 events
category REPO_SAYS  -> 1 events, stamped aggregate_type=CLASS_SAYS
category EVENT_SAYS -> 0 events
```

The repository's `aggregate_type` string decides which stream category
`save()`/`load()` read and write (`AggregateRepository._stream()` builds
`StreamId(aggregate_id=..., category=self._aggregate_type)`). The
aggregate class's attribute is what actually gets stamped onto every
event `AggregateRoot.create_event()` produces. The event class's own
field default is discarded entirely — nothing in the repository or the
aggregate ever reads it. A save/load round-trip through the same
misconfigured repository is invisible, because both sides use the same
wrong value; the mismatch only surfaces later as an empty category query
or a projection that never fires against events that are already
durable, misfiled, by that point.

An audit of every `AggregateRepository(aggregate_type=...)` call site in
the repository found 86 total: 83 passed a value identical to the
factory class's `aggregate_type` attribute — pure ceremony that
restated what inference already produced. The remaining 3 differed, and
all three lived in
`tests/unit/application/aggregates/test_aggregate_type_inference.py`,
a test module that existed specifically to exercise the override
parameter itself — including a test named `test_explicit_type_still_works`
with the docstring "Existing code with explicit type continues to work,"
a backward-compatibility test dating from when inference was first added
on top of a pre-existing required parameter. Zero divergent uses were
found in `src/`, `docs/`, or `examples/`. The override parameter had no
real consumer left; it existed only to test that it still worked.

## Decision

`AggregateRepository.__init__` drops the `aggregate_type` parameter
entirely. `aggregate_type` is now always inferred from
`aggregate_factory.aggregate_type` — the aggregate class's own required
attribute is the single source of truth for the string that gets
stamped onto events, used to build the stream category, and reported by
`AggregateRepository.aggregate_type`. There is no longer a second place
to declare it and no way for the two to diverge.

This is a breaking change to the public constructor signature. The
project is pre-1.0 with a standing NO-SHIMS policy — clean breaks over
deprecation aliases — so no compatibility parameter or deprecation
warning is provided; callers passing `aggregate_type=` explicitly must
delete the argument.

The event class's own `aggregate_type` field default (item 3 above) is
untouched by this decision. It remains a separate, narrower concern:
whether an event's declared default should be validated against, or
tied to, the aggregate that produces it is not addressed here.

## Consequences

### Positive

- One declaration site for `aggregate_type` per aggregate. The
  repository can no longer stamp a different type onto events than the
  aggregate class declares for itself, closing the silent-miscategorization
  failure mode described above.
- `_infer_aggregate_type`'s `ValueError` — raised when the factory has
  no usable `aggregate_type` — is now the only way construction can fail
  on this axis, and its message no longer needs to describe an
  alternative path (the explicit-parameter escape hatch) that does not
  exist.
- 86 call sites simplify: 83 of them drop a line that was already
  restating the class attribute.

### Negative

- Breaking change for any external caller passing `aggregate_type=`
  explicitly to `AggregateRepository`. There is no deprecation window.
  The fix is mechanical: delete the argument, or if the aggregate class
  cannot be edited directly, subclass it and set the attribute on the
  subclass.
- The narrow legitimate use the override existed for — wrapping a
  third-party or otherwise-unmodifiable aggregate class under a
  different type name — is no longer possible without subclassing.

### Non-breaking

`AggregateRepository.aggregate_type` (the read-only property) is
unchanged in shape; only how the value is produced changes. Nothing
downstream of construction (tracing attributes, snapshot lookups, stream
identity) changes behavior for the 83 call sites that already matched
class and parameter.

## Alternatives Considered

**Keep the parameter but add a runtime check that it matches the
factory's class attribute when both are given.** Rejected: this
preserves two declaration sites and merely converts silent divergence
into a raised exception, rather than removing the divergence
opportunity. With zero real consumers of the override, deleting the
parameter is strictly simpler and removes the failure mode instead of
detecting it.

**Deprecate the parameter with a warning for one release cycle before
removing it.** Rejected: the project's NO-SHIMS policy is deliberately
pre-1.0 and clean-break; a deprecation cycle exists to protect real
callers, and this audit found none among `src/`, `docs/`, or `examples/`.

## References

- `src/eventsource/application/aggregates/repository.py` —
  `AggregateRepository.__init__`, `_infer_aggregate_type`
- `tests/unit/application/aggregates/test_aggregate_type_inference.py` —
  inference tests retained and strengthened; override-only tests deleted
- `docs/guides/repository-pattern.md`, `docs/getting-started.md` —
  updated to describe inference as the only path
- [ADR 0043](0043-domain-model-guards-and-vocabulary.md) — established
  `aggregate_type` as a required `ClassVar[str]` with no default on
  `AggregateRoot`; this ADR builds on that guarantee rather than
  revising it

## Related

- `CHANGELOG.md` — `### Breaking` entry for this change
