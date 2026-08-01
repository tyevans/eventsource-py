# Multi-Tenancy

Use this guide to run one event-sourced application for many tenants: bind a
tenant to the current request, stamp that tenant onto every event you emit, and
have the repository reject writes that mix tenants.

Multi-tenancy support gives you four pieces to wire together, split across
`domain/tenant_context.py`, `domain/tenant_events.py`,
`application/aggregates/tenant_repository.py`, and `domain/exceptions.py` —
all reachable from the top-level `eventsource` package except
`TenantAwareRepository`, which was never re-exported from the front door:

- **Tenant context** — `tenant_scope`, `tenant_scope_sync`,
  `set_current_tenant`, `clear_tenant_context`, `get_current_tenant`, and
  `get_required_tenant`, backed by the `tenant_context` `ContextVar` so the
  tenant follows your code across `await` boundaries.
- **`TenantDomainEvent`** — a `DomainEvent` subclass where `tenant_id` is
  required rather than optional, with `with_tenant_context(...)` to fill it in
  from the ambient scope.
- **`TenantAwareRepository`** — a wrapper around `AggregateRepository` that
  validates the tenant of every uncommitted event before it is persisted, and
  can optionally require tenant context on load.
- **`TenantContextNotSetError` and `TenantMismatchError`** — the two failures
  you need to handle at your request boundary.

Follow the steps in order: context first, then events, then the repository
wrapper, then the configuration and error handling that match how strict you
need to be. The final sections cover nesting scopes for background work and the
isolation guarantees this wrapper does *not* provide — read those before you
rely on it as your only tenant boundary.

## Before you start

You need:

- **A working aggregate and repository.** `TenantAwareRepository` wraps an
  existing `AggregateRepository[TAggregate]`; it never constructs one for you.
  If you do not have a repository wired to an event store yet, set that up
  first.
- **A tenant identifier that is a `UUID`.** Every entry point in this package is
  typed on `uuid.UUID`: `tenant_context` is a `ContextVar[UUID | None]`,
  `set_current_tenant` and `tenant_scope` take a `UUID`, and
  `TenantDomainEvent.tenant_id` is a required `UUID` field. String tenant slugs
  must be resolved to a UUID at your request boundary.
- **A place to establish context per request.** Middleware, a framework
  dependency, or a job wrapper — anywhere you can wrap the unit of work in
  `async with tenant_scope(tenant_id)` (or `with tenant_scope_sync(...)` in sync
  code). Context is per execution context, not global.

Imports come from two places:

```python
from eventsource import (
    TenantContextNotSetError,
    TenantDomainEvent,
    TenantMismatchError,
)
from eventsource import (
    TenantAwareRepository,
    clear_tenant_context,
    get_current_tenant,
    get_required_tenant,
    set_current_tenant,
    tenant_context,
    tenant_scope,
    tenant_scope_sync,
)
```

`TenantDomainEvent`, both tenant exceptions, and every context helper shown
above are re-exported from the top-level `eventsource` package.
`TenantAwareRepository` is the one exception — it is not re-exported from
the front door; import it directly from
`eventsource.application.aggregates.tenant_repository`.

No extra dependency is required: multi-tenancy is part of the core install and
uses nothing beyond pydantic and the standard library's `contextvars`.

Two decisions to make before you write code, because they change what the rest
of this guide tells you to configure:

- **Whether every event on your aggregates is tenant-scoped.** Genuinely global
  events can stay plain `DomainEvent` subclasses — `DomainEvent` declares
  `tenant_id: UUID | None`, so an absent tenant is representable. See "Mixing
  tenant and non-tenant events on one aggregate".
- **Whether you are adopting this on an existing store.** Events written before
  you introduced tenants will not carry a `tenant_id`. Plan to start in
  migration mode (`validate_on_save=False`) rather than the default, and read
  Step 4 before flipping any switch.

Finally, know the boundary of this feature up front: `TenantAwareRepository`
validates the tenant of the events you are about to write. It does not filter
events on load and it does not partition storage — physical isolation remains
your event store's responsibility. See "Limitations" at the end of this guide.

## Step 1: Establish tenant context at the request boundary

Everything downstream — `with_tenant_context(...)`, `TenantAwareRepository`,
`get_required_tenant()` — reads one `ContextVar`:

```python
tenant_context: ContextVar[UUID | None] = ContextVar("tenant_context", default=None)
```

Set it once, as far out as you can: HTTP middleware, a framework dependency, a
consumer loop that reads the tenant off a message, a job wrapper. Do not set it
inside handlers or aggregates — by then it is too late for the code that ran
before it, and too easy to miss a path.

Prefer the scoped context managers over the raw setters. They restore the
previous value in a `finally` block, so context is cleaned up even when the
request raises.

### Async entrypoints: `async with tenant_scope(tenant_id)`

`tenant_scope` is an async context manager that sets the tenant on entry, yields
it, and resets it on exit:

```python
from uuid import UUID

from eventsource import tenant_scope


async def handle_request(request) -> Response:
    tenant_id: UUID = resolve_tenant(request)  # your own lookup
    async with tenant_scope(tenant_id):
        return await process(request)
```

The yielded value is the tenant ID, so `async with tenant_scope(tid) as tenant:`
works if you want it bound to a name.

As ASGI middleware, wrap the downstream call:

```python
class TenantMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        tenant_id = tenant_from_headers(scope["headers"])
        async with tenant_scope(tenant_id):
            await self.app(scope, receive, send)
```

Because the reset happens in `finally`, an exception raised inside the scope
propagates normally and the previous tenant is restored on the way out. The
reset is token-based (`tenant_context.reset(token)`), not a clear, so it
restores whatever was set before — `None` at the top level, the outer tenant
inside a nested scope. See "Nesting tenant scopes" below.

Entry and exit are logged at `DEBUG` on `eventsource.domain.tenant_context`
(`"Tenant scope entered: %s"` / `"Tenant scope exited: %s"`), which is the
quickest way to confirm the scope wraps the whole request rather than part of
it.

### Sync entrypoints: `with tenant_scope_sync(tenant_id)`

`tenant_scope_sync` is the same context manager for synchronous code — a WSGI
app, a Celery task, a CLI command, a test fixture. It sets the tenant on entry,
yields it, and resets it in a `finally` on exit:

```python
from uuid import UUID

from eventsource import tenant_scope_sync


def run_report(tenant_id: UUID) -> Report:
    with tenant_scope_sync(tenant_id):
        return build_report()
```

Like the async variant it yields the tenant ID, so `with
tenant_scope_sync(tid) as tenant:` binds it to a name, and its reset is
token-based — leaving the block restores whatever was set before, not `None`.
Nesting therefore behaves the same way:

```python
with tenant_scope_sync(tenant_a):
    assert get_current_tenant() == tenant_a
    with tenant_scope_sync(tenant_b):
        assert get_current_tenant() == tenant_b
    assert get_current_tenant() == tenant_a
```

An exception raised inside the block propagates as normal and the previous
tenant is still restored on the way out.

Both variants write to the same `tenant_context` variable, so mixed-colour call
stacks work: a sync entrypoint that enters `tenant_scope_sync` and then calls
`asyncio.run(...)` passes the tenant into the coroutine, because the task
`asyncio.run` creates copies the current context. The reverse — an async request
that offloads work with `asyncio.to_thread(...)` — also carries the tenant into
the worker thread. What does not carry it is a bare `ThreadPoolExecutor.submit`
or `threading.Thread`; see "How context propagates" below.

Pick the variant that matches the colour of the function you are wrapping. Do
not wrap an `await` in `tenant_scope_sync`: a synchronous `with` block does not
survive a suspension point, so a second task resuming on the same event loop can
observe your tenant. Use `async with tenant_scope(...)` in any coroutine.

Entry and exit log at `DEBUG` on `eventsource.domain.tenant_context` as
`"Tenant scope (sync) entered: %s"` / `"Tenant scope (sync) exited: %s"` —
distinct strings from the async variant, so you can tell from a log which one a
given code path took.

### Manual control: `set_current_tenant` / `clear_tenant_context` and the returned `Token`

Use the raw setters only when the set and the reset cannot live in one lexical
block — a framework with separate `before_request` / `after_request` hooks, a
test fixture with setup and teardown phases. Everywhere else, prefer the scope
managers above; they do exactly this, correctly, in a `finally`.

```python
from eventsource import (
    clear_tenant_context,
    set_current_tenant,
    tenant_context,
)

token = set_current_tenant(tenant_id)
try:
    do_work()
finally:
    tenant_context.reset(token)
```

`set_current_tenant(tenant_id)` is a thin wrapper over
`tenant_context.set(tenant_id)` that logs and returns the
`contextvars.Token[UUID | None]` the `ContextVar` produced. Note the import:
`tenant_context` itself is exported from `eventsource` (defined in
`eventsource.domain.tenant_context`), and you need it — the example above
bypasses `reset_tenant_context(token)`, so
restoring goes through the `ContextVar` directly.

**Keep the token.** Passing it to `tenant_context.reset(token)` restores
whatever the value was immediately before that particular set — `None` at the
top level, the outer tenant inside a nested set. That is what makes nesting
correct. If you discard the token you can only clear, not restore.

Standard `contextvars` rules apply to the token, and they are the sharp edges of
doing this by hand:

- A token may be reset **once**; a second `reset` with the same token raises
  `RuntimeError`.
- A token must be reset in the **same context** that created it. A token created
  in a request handler and reset inside a task spawned from it raises
  `ValueError`. Framework hooks that run the before/after pair in different
  contexts (some worker pools do) will fail this way — if you cannot guarantee
  one context, wrap the unit of work in `tenant_scope` instead.

`clear_tenant_context()` is a different operation, not a token-free reset. It
sets the variable to `None` **and invalidates every outstanding token** in the
current execution context:

```python
set_current_tenant(tenant_a)
token = set_current_tenant(tenant_b)
clear_tenant_context()
assert get_current_tenant() is None   # tenant_a is gone, not restored
reset_tenant_context(token)           # raises TenantContextResetError: token is invalidated
```

So calling it inside a nested scope wipes the outer tenant, and a later
`reset_tenant_context()` call — including the implicit one an enclosing
`tenant_scope()`/`tenant_scope_sync()` performs on exit — raises
`TenantContextResetError` instead of restoring anything, because the token it
holds no longer matches the current state. **Never call `clear_tenant_context()`
inside an active scope** unless you want that scope's exit to fail loudly; it
is a hard reset for the outermost boundary — the end of a request in a pooled
worker, or between tests — not a tool to use mid-scope.

Its docstring carries one more caveat worth repeating: clearing affects only the
current execution context. Concurrent tasks each hold their own copy, so
clearing in one does not clear the others.

Both functions log at `DEBUG` on the `eventsource.domain.tenant_context` logger
(`"Tenant context set: %s"` and `"Tenant context cleared"`), distinct from the
scope managers' messages, so a debug log tells you which mechanism a code path
used.

### Reading context: `get_current_tenant()` vs `get_required_tenant()`

Two readers, both of which do nothing more than read `tenant_context`. They
differ only in what they do when nothing is set:

```python
get_current_tenant()   # -> UUID | None; never raises
get_required_tenant()  # -> UUID; raises TenantContextNotSetError when unset
```

Reach for `get_current_tenant()` where an absent tenant is a legitimate state
you want to branch on — optional filtering, diagnostics, logging, helpers that
run both inside a request and from a maintenance script. Its contract is that
it never raises, which also makes it the safe choice in `except` blocks and
teardown paths:

```python
from eventsource import get_current_tenant

tenant_id = get_current_tenant()
if tenant_id is None:
    logger.info("running outside a tenant scope; skipping tenant filter")
```

Reach for `get_required_tenant()` in code that has no meaningful behaviour
without a tenant. It fails at the point the tenant was needed, with a message
that names the fix:

```
No tenant context set. Use set_current_tenant() or tenant_scope() before
performing multi-tenant operations.
```

That is far more actionable in a log than the `None` dereference or validation
error you would otherwise hit a few frames later. `TenantContextNotSetError`
takes no constructor arguments — the message is fixed — so catch it by type,
never by matching the string.

```python
from uuid import UUID

from eventsource import TenantContextNotSetError
from eventsource import get_required_tenant


def audit_prefix() -> str:
    tenant_id: UUID = get_required_tenant()
    return f"tenant:{tenant_id}"
```

Both readers see exactly what the enclosing scope set, and nothing once it
exits:

```python
from eventsource import (
    get_current_tenant,
    get_required_tenant,
    tenant_scope,
)

assert get_current_tenant() is None          # outside any scope
async with tenant_scope(tenant_id):
    assert get_current_tenant() == tenant_id
    assert get_required_tenant() == tenant_id
assert get_current_tenant() is None          # restored on exit
```

You will rarely call `get_required_tenant()` yourself on the write path,
because the library already calls it for you:
`TenantDomainEvent.with_tenant_context(...)` uses it to fill in `tenant_id`,
and `TenantAwareRepository` uses it in `save()` and — when configured to
enforce on load — in its load paths. A `TenantContextNotSetError` raised from
either means the same thing as one raised from your own code: the unit of work
was not wrapped in a scope. Fix that at the request boundary (Step 1) rather
than catching the error where it surfaced.

A rule that covers the choice: use `get_required_tenant()` whenever the next
line would be wrong with `None`, and `get_current_tenant()` only when you
actually write the `if ... is None` branch. A bare `get_current_tenant()` whose
result is passed straight into a query filter or an event field is the pattern
that silently produces untenanted rows.

### How context propagates across `await`, `asyncio.create_task`, and thread pools

Nothing in `eventsource.domain.tenant_context` implements propagation — `tenant_context`
is a plain `ContextVar[UUID | None]`, and standard `contextvars` semantics
decide where the tenant is visible. Internalise those semantics, because the
failure mode is silent: a task that sees the wrong tenant, or none, produces
mis-stamped or rejected events far from the code that lost the context.

The single rule: **context flows forward into work started while it is set, and
never backward or sideways into work that already exists.**

- **Across `await`**: the tenant follows the coroutine. A task keeps one context
  for its whole lifetime, so anything you `await` inside a scope sees the
  tenant, however deep the call stack — including the repository's internal
  `get_required_tenant()` calls.

- **`asyncio.create_task` / `asyncio.gather` inside a scope**: the child copies
  the current context at creation time, so it inherits the tenant. A task
  created inside `tenant_scope(tenant_id)` reads that tenant even though it
  never entered a scope of its own:

  ```python
  async def inner() -> UUID | None:
      return get_current_tenant()

  async with tenant_scope(tenant_id):
      assert await asyncio.create_task(inner()) == tenant_id
  ```

- **Concurrent tasks are isolated**: because each child gets a *copy*, not a
  share, tasks that interleave on the same event loop cannot see each other's
  tenant, and a scope entered inside a child never leaks back to the parent.
  Enter one scope per task and let them run together:

  ```python
  async def handle(tenant_id: UUID) -> None:
      async with tenant_scope(tenant_id):
          await asyncio.sleep(0)   # any suspension point
          assert get_current_tenant() == tenant_id

  await asyncio.gather(handle(tenant_a), handle(tenant_b))
  ```

  This holds at any width — twenty tasks with twenty tenants and staggered
  awaits each still observe only their own.

- **Tasks created before the scope**: a task created outside (or before) the
  scope copied the context as it was then, and entering a scope in the parent
  afterwards does not reach it. Establish context *before* spawning work.

- **Background tasks that outlive the request**: a task spawned inside
  `tenant_scope` keeps the tenant after the scope exits, because it holds its
  own copy — the parent's `tenant_context.reset(token)` does not touch it. That
  is usually what you want for fire-and-forget work. If it is not, pass the
  tenant explicitly and open a fresh scope inside the task.

- **`asyncio.to_thread(...)`**: copies the current context into the worker
  thread, so the tenant carries over and `tenant_scope_sync` is not needed
  inside the callable.

- **Bare threads and executors**: `ThreadPoolExecutor.submit(...)` and a
  manually constructed `threading.Thread` do **not** copy context. A new thread
  starts empty, `get_current_tenant()` returns `None`, and
  `get_required_tenant()` raises `TenantContextNotSetError`. Either re-establish
  the tenant inside the worker:

  ```python
  tenant_id = get_required_tenant()          # capture on the calling thread

  def work() -> None:
      with tenant_scope_sync(tenant_id):     # re-establish inside the worker
          do_work()

  executor.submit(work)
  ```

  or submit through a captured context: `executor.submit(copy_context().run,
  work)`.

- **`asyncio.run(...)` from sync code**: the loop's main task copies the calling
  thread's context, so `with tenant_scope_sync(tid): asyncio.run(main())`
  carries the tenant into the coroutine.

Two consequences worth calling out. First, `clear_tenant_context()` clears only
the calling context — concurrent tasks hold their own copies and are unaffected,
which is why it is safe as a per-request reset but useless as a global one.
Second, a `contextvars.Token` can only be reset in the context that created it;
handing a token across a task boundary and resetting it there raises
`ValueError`. Both are reasons to prefer the scope managers over manual setters
anywhere work fans out.

## Scope a projection to the request's tenant (callable filter)

When one projection instance serves many tenants — a request-scoped read model,
or a single reader whose tenant is decided by the ambient context — pass a
zero-argument callable returning `UUID | None` instead of a fixed `UUID`:

```python
from eventsource import get_current_tenant, tenant_scope
from eventsource.application.projections import DeclarativeProjection


projection = OrderSummaryProjection(tenant_filter=get_current_tenant)

async with tenant_scope(acme):
    await projection.handle(order_created)   # filtered against `acme`
```

`get_current_tenant` matches the required signature exactly (no arguments,
returns `UUID | None`), so it can be handed over as-is — do not call it at
construction time, or you would freeze whatever tenant happened to be current
then.

`_get_tenant_filter_value()` invokes the callable **once per event**, from
`_process_event`, so the effective tenant is re-resolved on every `handle()`
call and may differ between events:

```python
async with tenant_scope(acme):
    await projection.handle(acme_event)      # processed

async with tenant_scope(globex):
    await projection.handle(globex_event)    # processed
    await projection.handle(acme_event)      # skipped
```

Any callable works, not just the contextvar getter — a closure over a mutable
cell, a lookup on a request object, or a method bound to your web framework's
context:

```python
def current_tenant_for_worker() -> UUID | None:
    return worker_state.tenant_id


projection = OrderSummaryProjection(tenant_filter=current_tenant_for_worker)
```

Two consequences worth planning for:

- **Outside any tenant scope the callable returns `None`**, and `None` means
  *no filtering* rather than *no events* — every event is processed. If a stray
  unscoped `handle()` call would corrupt the read model, either assert the scope
  yourself before dispatching (`get_required_tenant()` raises
  `TenantContextNotSetError` when unset) or use a static `UUID` filter instead.
- **The callable runs on the projection's event-handling path**, so keep it
  cheap and non-blocking. A `ContextVar` read is effectively free; a database
  lookup per event is not — resolve that once and cache it in the cell your
  callable reads.

If the projection is driven by a subscription runner rather than by your request
handler, remember that the runner, not the request, calls `handle()`. Establish
the scope around that call — for example inside the handler you register with
the subscription — or fall back to one instance per tenant (see "Run one
projection instance per tenant").

## How filter values are resolved: `_get_tenant_filter_value`

Both filter styles funnel through one method,
`DeclarativeProjection._get_tenant_filter_value()`, which normalizes whatever
you passed to `tenant_filter` into a plain `UUID | None`:

| `tenant_filter` you passed | Resolved value |
| --- | --- |
| `None` (default) | `None` — no filtering |
| a `UUID` | that same `UUID`, unchanged |
| a callable | whatever the callable returns, re-evaluated per call |

The implementation is exactly that three-branch check: `None` short-circuits,
`isinstance(self._tenant_filter, UUID)` returns the stored value, and anything
else is invoked with no arguments. There is no coercion step — a filter that is
neither `None` nor a `UUID` is *called*, so passing a `str` raises `TypeError`
at event-handling time rather than at construction. Convert strings yourself:

```python
projection = OrderSummaryProjection(tenant_filter=UUID(tenant_id_str))
```

The resolved value feeds `_should_process_event(event)`, which treats `None` as
"process everything". So a callable returning `None` — `get_current_tenant`
outside any tenant scope, for instance — disables filtering for that event
instead of dropping it.

Use the method directly when you want to see what a projection would filter on
right now, without dispatching an event:

```python
from eventsource import get_current_tenant, tenant_scope

projection = OrderSummaryProjection(tenant_filter=get_current_tenant)

assert projection._get_tenant_filter_value() is None      # unscoped

async with tenant_scope(acme):
    assert projection._get_tenant_filter_value() == acme
```

Two behaviours to keep in mind when your callable is not a cheap contextvar
read:

- **It is called more than once per skipped event.** `_process_event` resolves
  the value inside `_should_process_event`, then twice more while building the
  DEBUG skip log's message and `extra` fields. A callable with side effects (a
  counter, a lazily-opened session) will see those extra invocations, and an
  expensive one pays for them.
- **Nothing is cached between events.** The stored `tenant_filter` is kept
  as-is; only the *resolution* is repeated. That is what makes the callable form
  track the ambient tenant, and why a static `UUID` filter is the cheaper choice
  when the tenant never changes for that instance.

## What gets processed and what gets skipped

The decision is made by `DeclarativeProjection._should_process_event(event)`,
called at the top of `_process_event` before any handler lookup. It compares the
resolved filter value (previous section) against
`getattr(event, "tenant_id", None)` — four cases, all listed below.

### Events whose `tenant_id` matches the filter -> processed

The comparison is plain `UUID` equality — `bool(event_tenant == filter_value)` —
so a `UUID` rebuilt from its string form matches a `UUID` constructed any other
way, and neither side is normalized, parsed, or string-compared:

```python
acme = UUID("11111111-1111-1111-1111-111111111111")
projection = OrderSummaryProjection(tenant_filter=acme)

event = OrderCreated(aggregate_id=uuid4(), tenant_id=UUID(str(acme)))
await projection.handle(event)   # processed
```

On a match, `_should_process_event` returns `True` and `_process_event`
continues straight into the normal dispatch path: handler lookup in the
registry, then dispatch (inside the `eventsource.projection.handler` span when
tracing is enabled). Nothing else about the event is inspected, and behaviour is
identical to a projection constructed with no filter at all — including the
unregistered-event path, so a matching event with no registered handler still
follows your `unregistered_event_handling` setting.

The same holds for the callable form, resolved per event: an event is processed
whenever the filter *at that moment* equals the event's tenant.

```python
async with tenant_scope(acme):
    await projection.handle(acme_event)   # resolved filter == acme -> processed
```

Checkpointing is unaffected: a processed event advances the checkpoint exactly
as it always would, after the handler returns.

### Events whose `tenant_id` differs -> skipped silently (logged at DEBUG)

When `_should_process_event` returns `False`, `_process_event` emits one DEBUG
record and returns. There is no other signal — plan around these four
consequences:

- **No exception, no DLQ entry.** `_handle_with_retry` sees a normal return, so
  the retry loop never engages and nothing is written to the DLQ repository.
- **The handler is never looked up.** The filter check runs before
  `self._handler_registry.get_handler(type(event))`, so
  `unregistered_event_handling="error"` cannot fire for a skipped event: a
  filtered-out event of an otherwise unhandled type is simply dropped.
- **The checkpoint still advances.** After `_process_event` returns,
  `_handle_with_retry` calls `record_checkpoint(...)` (when `checkpoint_repo`
  is configured) and logs its usual "processed event" DEBUG line — a skip is indistinguishable
  from a successful handle at that layer. That is the behaviour you want for a
  per-tenant reader: it must not stall or rewind on another tenant's traffic.
  It also means resuming from the checkpoint will not replay skipped events, so
  changing an instance's `tenant_filter` does not backfill the newly included
  tenant.
- **The only trace is the DEBUG record**, whose message begins
  `Skipping event <event_id>: tenant <event tenant> doesn't match filter
  <filter value>`, carrying `extra` fields `projection`, `event_id`,
  `event_type`, `event_tenant_id`, and `filter_tenant_id` (the last two
  stringified, so an absent tenant appears as `"None"`).

Because the message is a DEBUG record, skips are invisible at the default INFO
level. Turn them on for the projections logger when you are diagnosing a read
model that is missing rows:

```python
import logging

logging.getLogger("eventsource.application.projections").setLevel(logging.DEBUG)
```

Note that the filter value is resolved again while building the message and the
`extra` mapping, so a callable filter is invoked three times for each skipped
event — one more reason to keep it a cheap contextvar read.

### Events with no `tenant_id` attribute or `tenant_id=None` -> processed anyway (legacy/system events)

`_should_process_event` reads the tenant defensively —
`getattr(event, "tenant_id", None)` — so two different situations collapse to
the same `None`:

- the event class declares `tenant_id` but this instance left it unset.
  `DomainEvent.tenant_id` is `UUID | None` and defaults to `None`, so this is
  the common case for anything not built through `TenantDomainEvent` or
  `with_tenant_context(...)`;
- the event class has no `tenant_id` attribute at all — a model that predates
  the field, or a non-`DomainEvent` payload routed through the projection.

Either way the next branch returns `True` before the equality check ever runs,
so the event is handled exactly as if no filter were set:

```python
acme = UUID("11111111-1111-1111-1111-111111111111")
projection = OrderSummaryProjection(tenant_filter=acme)

await projection.handle(OrderCreated(aggregate_id=uuid4()))                 # processed
await projection.handle(OrderCreated(aggregate_id=uuid4(), tenant_id=None)) # processed
await projection.handle(LegacyImported(aggregate_id=uuid4()))               # processed
```

This is deliberate. Events written before multi-tenancy existed, and system
events that belong to no tenant (schema markers, imports, maintenance events),
would otherwise be invisible to *every* tenant-scoped projection — each
per-tenant instance would skip them and the checkpoint would move past them, so
nothing would ever see them. Passing them through means each instance sees them
once, which is the right default for events that are global by nature.

The consequence to plan around: **the filter is not a security boundary.** An
event with a missing or null tenant reaches the handlers of a projection scoped
to any tenant. Two ways to close that:

Reject untenanted events inside the handler, where you can decide per event
type:

```python
@handles(OrderCreated)
async def _handle_order_created(self, event: OrderCreated) -> None:
    if event.tenant_id is None:
        return
    ...
```

Or remove the case at the source by deriving your events from
`TenantDomainEvent` (`eventsource.domain.tenant_events`, re-exported from
`eventsource`), which redeclares `tenant_id`
as a required `UUID`; construction without a tenant then fails validation
rather than producing an unfiltered event. Note that the guard only helps for
events you control — replayed history written before the switch still carries
`tenant_id=None`.

### `tenant_filter=None` (default) -> no filtering, all events processed

`_should_process_event` returns `True` immediately when the resolved value is
`None`, without touching the event. Omitting `tenant_filter` therefore preserves
pre-tenant behaviour exactly — and, as noted earlier, a *callable* that returns
`None` lands in this same branch, so an unscoped `get_current_tenant` processes
everything rather than nothing.

| Filter value | Event `tenant_id` | Result |
| --- | --- | --- |
| `None` | anything | processed |
| `UUID` | equal `UUID` | processed |
| `UUID` | different `UUID` | skipped, DEBUG log, checkpoint advances |
| `UUID` | `None` / attribute absent | processed |
