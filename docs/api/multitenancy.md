# Multi-Tenancy API Reference

Technical reference for the library's multi-tenancy support: the
`contextvars`-based tenant context, the `TenantDomainEvent` base class that
requires a `tenant_id`, the `TenantAwareRepository` wrapper that validates tenant
ownership around an `AggregateRepository`, and the three tenant exceptions.

There is no single package for this feature — it was dissolved into the ring
architecture under [ADR 0038](../adrs/0038-multitenancy-dissolution.md). The
pieces live across four modules:

| Module | Contains |
| --- | --- |
| `eventsource.domain.tenant_context` | `tenant_context`, `get_current_tenant`, `get_required_tenant`, `set_current_tenant`, `clear_tenant_context`, `tenant_scope`, `tenant_scope_sync` |
| `eventsource.domain.tenant_events` | `TenantDomainEvent` |
| `eventsource.application.aggregates.tenant_repository` | `TenantAwareRepository` |
| `eventsource.domain.exceptions` | `TenantContextNotSetError`, `TenantContextResetError`, `TenantMismatchError` (merged in alongside the rest of the exception hierarchy) |

Every name except `TenantAwareRepository` is re-exported from the top-level
`eventsource` package, so `from eventsource import tenant_scope` is the way to
reach it — there is no dotted-path equivalent to fall back to, since the old
`eventsource.multitenancy` barrel no longer exists. `TenantAwareRepository` is
the one name that must be imported from its full module path:
`from eventsource.application.aggregates.tenant_repository import TenantAwareRepository`.

## Overview

The feature supplies three cooperating pieces plus their error types. A
`ContextVar` carries the active tenant UUID through async tasks and threads;
`TenantDomainEvent` turns the optional `DomainEvent.tenant_id` into a required
field and can read that field from the context; `TenantAwareRepository` wraps an
`AggregateRepository` and checks, before each save, that every uncommitted event
belongs to the tenant currently in context.

Nothing in this feature touches the event store's SQL. Isolation is enforced in
Python, on the write path, against values already present on the events. Read
isolation -- filtering loaded events by tenant -- is not implemented here; see
the known limitation under `TenantAwareRepository`.

### Import Surface

| Name | Kind / signature | One-line role |
| --- | --- | --- |
| `tenant_context` | `ContextVar[UUID \| None]`, default `None` | The underlying storage for the active tenant |
| `get_current_tenant` | `() -> UUID \| None` | Read the tenant, or `None`; never raises |
| `get_required_tenant` | `() -> UUID` | Read the tenant, or raise `TenantContextNotSetError` |
| `set_current_tenant` | `(tenant_id: UUID) -> Token[UUID \| None]` | Set the tenant, returning a reset `Token` |
| `clear_tenant_context` | `() -> None` | Set the tenant back to `None` |
| `tenant_scope` | async context manager, yields `UUID` | Scoped set + token-based restore |
| `tenant_scope_sync` | sync context manager, yields `UUID` | Same, for synchronous code |
| `TenantDomainEvent` | class, subclass of `DomainEvent` | Event base with required `tenant_id` |
| `TenantAwareRepository` | `Generic[TAggregate]` class | Validating wrapper over `AggregateRepository` |
| `TenantContextNotSetError` | exception, subclass of `EventSourceError` | No tenant in context where one was required |
| `TenantContextResetError` | exception, subclass of `EventSourceError` | A context token was reset out of LIFO order, or reset twice |
| `TenantMismatchError` | exception, subclass of `EventSourceError` | Events carry a tenant other than the active one |

Each source module declares its own `__all__` containing exactly the names
listed for it in the module table above -- there is no barrel module anymore,
so there is no union `__all__` to speak of; each name is reached either
through `eventsource`'s own `__all__` or, for `TenantAwareRepository`, through
its module directly.

The eleven names other than `TenantAwareRepository` also appear in
`eventsource.__all__`; `TenantAwareRepository` does not (it never did, even
when the feature lived in `eventsource.multitenancy`), so it is only reachable
as `from eventsource.application.aggregates.tenant_repository import
TenantAwareRepository`.

### When to Use This Feature

Reach for this multi-tenancy support when a single deployment serves several
tenants out of one event store and you want tenant ownership recorded on every
event and re-checked before it is appended.

- **Tag events with a tenant.** Subclass `TenantDomainEvent` instead of
  `DomainEvent`; pydantic then rejects construction without a `tenant_id`, so
  an untagged event cannot reach the store. Use
  `TenantDomainEvent.with_tenant_context(...)` inside a scope to fill the field
  from context rather than threading the UUID through call sites.
- **Propagate the tenant across an async call graph.** Set the context once at
  the edge (request middleware, message consumer, job runner) with
  `tenant_scope`, and any code below it -- including awaited coroutines and
  tasks spawned from that context -- reads the same value. This is ordinary
  `contextvars` behavior, with the isolation properties and caveats that
  implies.
- **Catch cross-tenant writes before they persist.** Wrap the repository in
  `TenantAwareRepository`. With the default `validate_on_save=True`, `save()`
  compares each uncommitted event's `tenant_id` against
  `get_required_tenant()` and raises `TenantMismatchError` listing the
  offending `event_id`s rather than appending them.

It is *not* the right tool when:

- **Tenants must be physically separated.** Nothing here shards or routes to
  per-tenant databases or schemas; every tenant's events live in the same
  stream space.
- **You need loads filtered by tenant.** `enforce_on_load=True` only asserts
  that a context exists -- it does not restrict which events come back.
  Pair this module with database-level isolation (for example PostgreSQL row
  level security) when read isolation matters.
- **You need projection or subscription filtering.** Projections and
  subscriptions have no tenant awareness in this feature; a projection reads
  every tenant's events unless you filter inside the handler.

Events that lack a `tenant_id` entirely are skipped by save-time validation, so
tenant-aware and legacy non-tenant events can coexist on the same aggregate
during a migration. `validate_on_save=False` disables the check outright for the
same reason.

## Tenant Context

`eventsource.domain.tenant_context` is a thin, typed wrapper around a single
`ContextVar`. Everything else in the feature -- `with_tenant_context()`,
`TenantAwareRepository` -- reads the tenant through these functions, so the
semantics below are the semantics of tenant propagation library-wide.

There is exactly one variable. Setting a tenant anywhere sets it for the
current execution context and everything that inherits from it; nothing
namespaces contexts per store, per repository, or per aggregate type.

### `tenant_context` (ContextVar[UUID | None])

```python
tenant_context: ContextVar[UUID | None] = ContextVar("tenant_context", default=None)
```

The module-level variable holding the active tenant. Its default is `None`,
which means "no tenant context established" -- reading it before any set never
raises `LookupError`.

Defined in `eventsource.domain.tenant_context` and re-exported from the
top-level `eventsource` package, so both of these bind the same object:

```python
from eventsource import tenant_context
from eventsource.domain.tenant_context import tenant_context
```

There is one instance for the whole process. The `ContextVar` name argument is
`"tenant_context"`, which is used only in `repr()` and debugging output; it does
not namespace anything.

It is exported publicly because `set_current_tenant()` returns a
`Token[UUID | None]` that only `tenant_context.reset(token)` can consume --
`contextvars` provides no other way to undo a set. That is the one intended
direct use. Prefer the accessor functions for reads and the scope managers for
writes:

| Operation | Direct form | Preferred form |
| --- | --- | --- |
| Read, tenant optional | `tenant_context.get()` | `get_current_tenant()` |
| Read, tenant required | `tenant_context.get()` + `None` check | `get_required_tenant()` |
| Set | `tenant_context.set(tenant_id)` | `set_current_tenant()` (adds a debug log) or `tenant_scope()` |
| Restore prior value | `tenant_context.reset(token)` | `tenant_scope()` / `tenant_scope_sync()` |
| Force empty | `tenant_context.set(None)` | `clear_tenant_context()` |

The variable is not type-narrowed for you: `tenant_context.get()` is typed
`UUID | None`, so reading it directly forces a `None` check at every call site.
`get_required_tenant()` exists to do that once.

### `get_current_tenant() -> UUID | None`

```python
def get_current_tenant() -> UUID | None:
    return tenant_context.get()
```

The whole implementation. It returns the value of the `tenant_context`
`ContextVar` for the current execution context, or `None` when nothing has set
it. Takes no arguments, performs no I/O, logs nothing, and **never raises** --
the variable's `default=None` means even a first read in a brand-new context
cannot produce `LookupError`.

`None` is returned in exactly these situations:

- No `set_current_tenant()` or `tenant_scope()` has run in this context.
- `clear_tenant_context()` was called, which sets the variable to `None`.
- A `tenant_scope()` exited and the value it restored was itself `None`.
- The read happens in a context that did not inherit the set -- a plain
  `threading.Thread`, or a sibling `asyncio.gather()` task (see
  [Caveat: Concurrent Tasks and Context Isolation](#caveat-concurrent-tasks-and-context-isolation)).

```python
from uuid import uuid4
from eventsource import get_current_tenant, set_current_tenant, clear_tenant_context

assert get_current_tenant() is None
tenant_id = uuid4()
set_current_tenant(tenant_id)
assert get_current_tenant() == tenant_id
clear_tenant_context()
assert get_current_tenant() is None
```

The return type is `UUID | None`, so under `mypy` every call site must narrow
before using the value as a `UUID`:

```python
tenant_id = get_current_tenant()
if tenant_id is not None:
    span.set_attribute("tenant.id", str(tenant_id))
```

If that `if` would immediately be followed by raising or aborting, use
`get_required_tenant()` instead -- it returns a plain `UUID` and raises
`TenantContextNotSetError` for you.

Use `get_current_tenant()` where a missing tenant is a legitimate state:
logging and metrics enrichment, tracing attributes, and code paths that must
work both inside and outside a tenant scope (admin tooling, migrations,
background maintenance). Because it cannot raise, it is also safe in `finally`
blocks, exception handlers, and `__repr__` implementations.

Nothing inside the library calls `get_current_tenant()` on the write path;
`TenantDomainEvent.with_tenant_context()` and `TenantAwareRepository` both use
`get_required_tenant()`. Reading `None` here therefore says nothing about
whether a later save will pass validation -- it says only that this context has
no tenant right now.

### `get_required_tenant() -> UUID`

```python
def get_required_tenant() -> UUID:
    tenant_id = tenant_context.get()
    if tenant_id is None:
        raise TenantContextNotSetError()
    return tenant_id
```

The assertion form of `get_current_tenant()`. Takes no arguments, performs no
I/O, and logs nothing. It returns a plain `UUID` -- not `UUID | None` -- so call
sites need no narrowing; the cost is that a missing context raises
`TenantContextNotSetError` instead of returning a value.

```python
from eventsource import get_required_tenant, TenantContextNotSetError

try:
    tenant_id = get_required_tenant()
except TenantContextNotSetError:
    ...  # nothing set the context upstream
```

Because the check is `is None`, every situation in which
[`get_current_tenant()`](#get_current_tenant---uuid--none) returns `None` --
never set, cleared, restored-to-`None`, or read from a non-inheriting thread or
sibling task -- raises here instead.

```python
from eventsource import set_current_tenant, clear_tenant_context

set_current_tenant(tenant_id)
assert get_required_tenant() == tenant_id
clear_tenant_context()
get_required_tenant()  # raises TenantContextNotSetError
```

`TenantContextNotSetError` takes no constructor arguments and carries a fixed
remediation message:

```
No tenant context set. Use set_current_tenant() or tenant_scope() before
performing multi-tenant operations.
```

It subclasses `EventSourceError`, so a broad `except EventSourceError` handler
catches it alongside the rest of the library's errors.

#### Where the Library Calls It

This is the internal accessor on every tenant-enforcing path, which is why a
missing scope surfaces as `TenantContextNotSetError` from code you did not write:

| Caller | Behavior when context is unset |
| --- | --- |
| `TenantDomainEvent.with_tenant_context(**kwargs)` | Raises, unless an explicit `tenant_id` was passed in `kwargs` (the context is only consulted when the key is absent) |
| `TenantAwareRepository.save()` with `validate_on_save=True` (default) | Raises before any event is appended |
| `TenantAwareRepository.load()` / `load_or_create()` / `create_new()` with `enforce_on_load=True` | Raises before delegating to the wrapped repository |

In each case the raise happens *before* the underlying store is touched, so a
failed call leaves no partial write.

Use `get_required_tenant()` in your own code at the same boundary: inside a
handler, projection, or service method that is only meaningful for one tenant,
where reading `None` would be a bug rather than a supported mode. For optional
or best-effort reads -- logging, tracing, `finally` blocks, `__repr__` --
use `get_current_tenant()`, which cannot raise.

### `set_current_tenant(tenant_id: UUID) -> Token[UUID | None]`

Sets the tenant for the current execution context and returns the `Token`
produced by `ContextVar.set`. Also emits a `logger.debug` line
(`eventsource.domain.tenant_context` logger) recording the tenant.

The function performs no validation: it does not check that `tenant_id` is
non-null or that it differs from the current value. Calling it repeatedly
simply overwrites, and each call yields a distinct token.

```python
token = set_current_tenant(uuid4())
```

Use it at a boundary where entering and leaving are separate events and a
`with` block cannot span them -- for example an ASGI middleware that sets the
tenant in one callback and restores it in another.

#### Using the Returned Token to Restore Prior Context

The token is the only way to restore what was there *before* the set, which is
not necessarily `None`:

```python
from eventsource import tenant_context, set_current_tenant

token = set_current_tenant(tenant_b)
try:
    ...
finally:
    tenant_context.reset(token)  # back to whatever was active, not to None
```

Tokens are single-use and must be reset in the same context they were created
in; reusing one, or resetting it from a different context, raises
`ValueError` from `contextvars`. If you find yourself pairing a set with a
`finally: reset`, use `tenant_scope()` / `tenant_scope_sync()` -- they do
exactly this.

### `clear_tenant_context() -> None`

Calls `tenant_context.set(None)` and logs a debug line. Note the implementation
detail with visible consequences: this **sets the variable to `None`**, it does
not reset a token. A previously outer tenant is therefore *not* restored -- the
context becomes empty for the remainder of the current context, and any token
taken earlier still resets to the value that preceded that earlier set.

```python
set_current_tenant(tenant_a)
set_current_tenant(tenant_b)
clear_tenant_context()
assert get_current_tenant() is None  # tenant_a is not restored
```

Use it to guarantee a clean slate at the end of a unit of work when no scope
manager owns the lifetime -- for instance in test teardown or in a worker loop
that reuses a thread across jobs.

#### Caveat: Concurrent Tasks and Context Isolation

`ContextVar` values are per-execution-context, and asyncio copies the current
context when a task is created. Three consequences, all covered by the tests in
`tests/unit/domain/test_tenant_context.py`:

- **Sibling tasks are isolated.** Coroutines gathered with `asyncio.gather()`
  each get their own copy of the context, so a tenant set inside one task is
  invisible to the others, no matter how they interleave at `await` points.
- **Spawned tasks inherit, then diverge.** A task created with
  `asyncio.create_task()` starts with a copy of the creating context and sees
  the tenant that was active at creation time. Anything it sets afterward stays
  in its own copy and does not propagate back to the parent.
- **`clear_tenant_context()` only clears the caller's context.** Calling it
  inside a child task does not clear the parent's tenant, and calling it in the
  parent does not reach already-running children. It is not a way to cancel a
  tenant globally.

The same applies to threads: a thread started with `threading.Thread` does not
inherit the context at all, so the tenant reads back as `None` unless you
explicitly re-set it (or run the callable via `contextvars.Context.run`).

### `tenant_scope(tenant_id: UUID)` (async context manager)

```python
async with tenant_scope(tenant_id) as tid:
    ...
```

An `@asynccontextmanager` that sets the tenant on entry, yields the same
`tenant_id` it was given, and resets the token in a `finally` block on exit --
so the previous tenant is restored even when the body raises. This is the
recommended way to establish tenant context in async code.

```python
from eventsource import tenant_scope, get_current_tenant

async def handle_request(tenant_id):
    async with tenant_scope(tenant_id):
        await repository.save(aggregate)   # sees tenant_id
    assert get_current_tenant() is None    # restored (was None here)
```

Nothing in the body is awaited by the manager itself; it does no I/O and adds
no synchronization. The yielded value is a convenience -- the context is what
matters.

#### Nested Scopes and Token-Based Restore

Because exit resets the entry token rather than clearing, scopes nest to
arbitrary depth and unwind to the enclosing tenant, not to `None`:

```python
async with tenant_scope(tenant_a):
    assert get_current_tenant() == tenant_a
    async with tenant_scope(tenant_b):
        assert get_current_tenant() == tenant_b
    assert get_current_tenant() == tenant_a   # restored, not cleared
```

An exception raised inside the inner scope restores `tenant_a` on the way out
before propagating, so an outer `except` block still observes the correct
tenant. Note the contrast with `clear_tenant_context()`, which would leave
`None` at that point.

### `tenant_scope_sync(tenant_id: UUID)` (sync context manager)

The `@contextmanager` twin of `tenant_scope`, with identical set/yield/reset
behavior for synchronous call sites:

```python
from eventsource import tenant_scope_sync

with tenant_scope_sync(tenant_id):
    sync_adapter.append(...)
```

It nests and restores on exception exactly as the async version does. Use it in
synchronous entry points -- WSGI middleware, CLI commands, code calling through
`SyncEventStoreAdapter`. In async code use `tenant_scope`; the sync manager
would still set the variable correctly, but it cannot span an `await` safely as
part of an `async with` chain.

### Context Function Comparison Table

| Callable | Sync/async | Missing context | Restores prior value | Use when |
| --- | --- | --- | --- | --- |
| `get_current_tenant()` | sync, either | returns `None` | n/a | Tenant is optional |
| `get_required_tenant()` | sync, either | raises `TenantContextNotSetError` | n/a | Tenant is mandatory |
| `set_current_tenant()` | sync, either | n/a | only if you reset the token | Set and restore happen in different callbacks |
| `clear_tenant_context()` | sync, either | no-op (already `None`) | no -- forces `None` | Guaranteeing an empty context |
| `tenant_scope()` | async | n/a | yes, via token in `finally` | Async request/job boundary |
| `tenant_scope_sync()` | sync | n/a | yes, via token in `finally` | Sync request/job boundary |

All six operate on the same `tenant_context` variable and therefore share its
propagation rules: inherited by tasks at creation, isolated between siblings,
not inherited by plain threads.
