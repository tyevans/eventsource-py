# 0056. `initial_state()` Is Nullary; the Command Carries the Aggregate Id

A decider is `(decide, evolve, initialState, isTerminal)`, and `initialState` is a
*value* — the fold's identity element. This library declared it as a function of the
aggregate id, which made every decider state model carry an id field whose only reader
was `decide`, building events.

## Status

**Accepted.**

**ADR Impact**

| ADR | Status |
| --- | --- |
| [0022](0022-command-objects-and-decider-style.md) | **Amended.** Its Decision 2 lists `initial_state`/`decide`/`evolve` as the three abstract static methods, with `initial_state` taking the aggregate id. `initial_state` now takes no arguments. Everything else in that record — eager state initialization, `execute()`'s stamping and precedence, atomic rejection, structural typing, `CommandRejectedError` — is unchanged. |
| [0042](0042-domain-event-strictness.md) | Stands. Provenance stamping is untouched; `aggregate_id` was never a stamped field and still is not. |
| [0046](0046-aggregate-type-single-source.md) | Stands, and this extends its reasoning to a second field. `aggregate_type` has one source, the aggregate class. `aggregate_id` has one source too — the command — rather than being copied into state so `decide` can read it back out. |

## Context

`DomainEvent.aggregate_id` is required, and `decide` is a pure static function with no
`self`, so the events `decide` returns have to get that id from one of its two
arguments. The original design routed it through the state: `initial_state(aggregate_id)`
received the id, stashed it in a field, and `decide` read `state.order_id` when
constructing events. ADR 0022 recorded that as an explicit contract note.

That works, and it is wrong in a way that costs something. The state of a decider is
the fold of one aggregate's events; the value *before* any event is not a fact about
any particular aggregate, it is one value for the whole aggregate type. Threading an id
through it makes the identity element a family of identity elements indexed by
something the fold never uses, and forces a required, defaultless field into every
decider state model — the one field that could not be recovered by replaying the
stream, sitting in a model whose entire point is that it is recoverable by replaying
the stream.

The other argument was already carrying the id in practice. A command is a request to
do something *to a particular aggregate*; the caller has to name it, and the
getting-started guide's own commands already declared `account_id` before this change.
So the id was being supplied twice on every command path — once to the constructor,
once into the state — and read from the copy.

## Decision

`DeciderAggregate.initial_state()` takes no arguments.

`decide(command, state)` obtains the aggregate id from the **command**. A command names
the aggregate it targets; that is what distinguishes it from an event, which is already
attached to a stream. Decider state models therefore carry no id field, and every field
they do carry is derivable from the event stream.

This is a **breaking change to a public domain-ring API** and ships with no shim, per
the pre-1.0 policy: no two-argument overload, no `*args`, no signature sniffing. A
subclass deletes the parameter from `initial_state`, drops the id field from its state
model, adds the target id to each command, and captures it in the `decide` `match` arm
instead of reading it off `state`.

`DeciderScenario` loses its `aggregate_id=` constructor argument for the same reason:
its only job was feeding `initial_state`, and the scenario now has no identity of its
own. Its `initial_state=` argument is a zero-argument callable.

The instance-level machinery is untouched. `_get_initial_state()`, `_apply()`, and
`__init__` still have `self.aggregate_id` — only the static abstract method's signature
changes.

## Consequences

The decider's three functions now match the pattern as it is described everywhere else,
which matters for a style this library recommends by default: a reader who knows the
decider pattern no longer has to account for a local deviation, and a reader learning
it here is not taught one.

State models get smaller and, more usefully, get an invariant: *everything in a decider
state is a fold of the events*. A field that cannot be reconstructed from the stream is
now a defect rather than a convention.

Commands get a required id field they mostly already had. Where they did not, the cost
is one line per command and one capture per `match` arm — and the argument is not
ceremony: a command with no target was already relying on the caller having routed it
to the right aggregate instance, with nothing in the command itself recording that
intent.

The aggregate does not verify that a command's id matches its own. `execute()` stamps
`aggregate_version` and `aggregate_type` but leaves `aggregate_id` to `decide`, so a
command carrying the wrong id produces events attributed to another stream. The
alternative — stamping `aggregate_id` in `_stamp()` the way the other two are stamped —
would make the command's id redundant again and reintroduce exactly the two-sources
problem ADR 0046 removed for `aggregate_type`. Application code that accepts ids from
outside should check the id it routes on against the id it constructs the command with,
which is a check it wants regardless.
