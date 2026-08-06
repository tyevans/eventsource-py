# 0055. `StoreProjection` Forwards the Projection Constructor by Name, Once

0.10.0 widened three projection constructors because their subclasses had been
silently dropping `retry_policy` and `tracer`. That fixed the classes in this
tree and left every subclass outside it re-deriving the same forwarding
boilerplate by hand.

## Status

**Accepted.**

**ADR Impact**

| ADR | Status |
| --- | --- |
| [0013](0013-handler-registry-composition.md) | Stands. `StoreProjection` is a `DeclarativeProjection`, so handler discovery and routing are inherited unchanged; this ADR adds nothing to and takes nothing from the registry. |
| [0024](0024-projection-persistence-ports.md) | Stands, and supplies the ring test. It classified `DatabaseProjection` as an adapter because its constructor names an `async_sessionmaker`. `StoreProjection` takes a type parameter instead, which is what keeps it in the application ring. |
| [0045](0045-pep695-type-parameter-syntax.md) | Stands. `StoreProjection[TStore]` uses the inline parameter syntax it decided, declared at the class rather than as a shared `TypeVar`. |

## Context

A projection that folds the log into exactly one store is the common shape,
and writing one currently means either accepting `DeclarativeProjection`'s
constructor verbatim or restating it. Restating it is what the 0.10.0 defect
was: several signatures spelling out one parameter list, with nothing failing
when one of them fell behind.

The library fixed its own. A consumer that needs a store — or any constructor
parameter of its own — is back in the same position, and worse placed to
notice: the narrowing is invisible until someone passes an argument the class
does not take, and the class is *theirs*, so no upstream test covers it. One
downstream consumer carried a file whose entire content was the restatement,
with a docstring explaining why. That file is the artifact of a gap here, not
of a mistake there.

The restatement also leaks a version fact. A subclass that names
`retry_policy` in its own signature has to know which release made that
parameter addable; get the floor wrong and the code imports fine and fails at
construction against an older library.

`**kwargs: Any` is the obvious way to stop restating, and it is why the
problem persisted: it trades a silent narrowing for a silent widening. The
type checker stops verifying that forwarded options exist, are spelled right,
or have the right types, and a typo becomes a runtime `TypeError` at the
`super().__init__` call. The downstream file rejected it for exactly this, and
that judgment was correct.

## Decision

Add **`StoreProjection[TStore]`** to `application/projections/`: a
`DeclarativeProjection` holding one store, exposed to handler methods as
`self._store`.

Its constructor takes the store and forwards everything else through
`**options: Unpack[ProjectionOptions]`, where `ProjectionOptions` is a public
`TypedDict` naming the parent's option set. This is PEP 692: the keyword
arguments stay individually typed, checked, and completable, and no caller or
subclass writes a parameter name. A subclass adding parameters of its own
declares only those, and forwards the rest as one opaque `**options`.

The store is a **type parameter**, never a concrete adapter or a driver type.
That is the whole of what keeps this class in the application ring, and the
line ADR 0024 drew when it sent `DatabaseProjection` to `adapters/sql/`.

**The store attribute is `_store`, not `store`.** Every collaborator on these
classes is protected — `_tracer`, `_retry_policy`, `_tenant_filter`,
`_handler_registry`, `_model_class`, `_session_factory` — and the only
intended reader is a `@handles` method on the subclass itself. A public
`store` would be the sole exception, and would promise external callers a
read-through the class does not otherwise offer.

**Forwarding is `Unpack[ProjectionOptions]`, not explicit parameters and not
`**kwargs: Any`.** Explicit parameters solve the problem only for a subclass
that adds nothing; the moment a consumer needs one parameter of their own,
they are restating the list again, which is the defect. `**kwargs: Any` solves
it at the cost of type checking. `Unpack` is the option that gives up neither.

### The constructor contract

The base class is half the answer. The other half is a promise, stated here
because it is what a subclass author actually relies on:

**Every projection base's constructor accepts at least what its parent's
accepts. This holds permanently, in every release, and is enforced by test.**

That is what 0.10.0 established by fixing the four constructors and adding the
superset test; it had never been written down as a guarantee, only performed.
Writing it down is what lets a subclass author stop tracking releases. Without
it, `**options` forwarding is a mechanism whose safety depends on nobody
narrowing a constructor later — and a narrowing would break forwarding
subclasses exactly as silently as it broke restating ones.

The concrete consequence for a consumer: a parameter never disappears, so the
question "which version added `retry_policy`?" has no successor question. A
subclass forwarding `**options` names no parameter at all, so the only version
floor it needs is the one for `StoreProjection` itself — one floor for the base
class, not one per parameter, which is the failure mode that shipped an
undetected too-low floor downstream.

The guarantee is about *acceptance*, not defaults or semantics. A parameter's
default may change, and such a change is breaking in the ordinary way and
announced in the ordinary way; what does not happen is a parameter silently
ceasing to be accepted.

`ProjectionOptions` is a second declaration of the parent's parameter list,
which is recurring defect shape #2 — so it is pinned: a test asserts the
TypedDict's keys equal `DeclarativeProjection.__init__`'s named parameters,
and fails if either side gains or loses one. The 0.10.0 superset test is
extended rather than sidestepped: its parameter-name extraction now expands an
`Unpack`-annotated `**kwargs`, so `StoreProjection` is checked against its
parent by the same rule as every other pair in the chain.

## Consequences

A consumer writing a store-backed projection subclasses `StoreProjection`,
writes `@handles` methods, and never learns what `DeclarativeProjection`'s
constructor accepts. Options added to the parent in a future release reach
existing subclasses without them changing anything, and without a version
floor to get wrong.

The superset test becomes load-bearing in a way it was not before. It was
added as a regression guard; it is now the enforcement of a public contract,
and a future change that trips it is a contract breach to be reconsidered
rather than a test to be updated.

`ProjectionOptions` is public API, because a subclass's own signature has to
name it. It is therefore subject to the same compatibility expectations as the
constructor it mirrors — which is appropriate, since it *is* that constructor's
signature under another name.

Forwarded options become keyword-only. `DeclarativeProjection` accepts
`checkpoint_repo`, `dlq_repo`, and `enable_tracing` positionally;
`StoreProjection` does not, `store` being its only positional parameter. No
existing call site is affected, the class being new, and keyword-only is the
better default for a set of optional collaborators.

`Unpack` for `**kwargs` is a new idiom in this tree. It is deliberately scoped
to constructor forwarding along an inheritance chain — the one place where
restating a parameter list has demonstrably drifted. It is not a general
license to replace explicit signatures.
