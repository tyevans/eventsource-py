# Multi-Tenancy

Use this guide to run one event-sourced application for many tenants. Bind a
tenant to the current request, stamp that tenant onto every event you emit, and
let the repository check stream ownership under that tenant's scope. A stream that
belongs to another tenant looks the same as a stream that does not exist.

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
  checks the tenant of every uncommitted event before it is persisted. Under an
  active scope it also checks that the stream belongs to that tenant on load,
  existence probe, version probe, and append. The optional
  `require_tenant_context` flag adds a precondition on the same calls.
- **`TenantContextNotSetError` and `TenantMismatchError`** — the two failures
  you need to handle at your request boundary.

Follow the steps in order: context first, then events, then the repository
wrapper, then the configuration and error handling that match how strict you
need to be. The later sections cover context propagation, projection filters,
stream ownership, auditing existing data, upgrade notes, and the limits of this
feature. Read the limits before you rely on it as your only tenant boundary.

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
    clear_tenant_context,
    get_current_tenant,
    get_required_tenant,
    set_current_tenant,
    tenant_context,
    tenant_scope,
    tenant_scope_sync,
)
from eventsource.application.aggregates.tenant_repository import (
    TenantAwareRepository,
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

- **Whether every event on your aggregates is tenant-scoped.** Under a tenant
  scope, `save()` rejects an event whose `tenant_id` is `None`. Save global
  events with a plain `AggregateRepository`, or with `validate_on_save=False`
  outside a scope.
- **Whether you are adopting this on an existing store.** Events written before
  you introduced tenants have no `tenant_id`. Under a scope, `TenantAwareRepository`
  does not load these streams. Read "Auditing existing streams" and
  "Upgrade notes" before you change any switch.

Finally, know the boundary of this feature up front. Under a scope,
`TenantAwareRepository` checks ownership of the streams it reads and writes. It
does not check direct store access or unscoped repositories, and it does not
partition storage. Physical isolation remains your event store's responsibility.
See "Limitations" at the end of this guide.

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
inside a nested scope. The nesting example is under "Sync entrypoints" below.

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
and `TenantAwareRepository` uses it in `save()` and — when configured with
`require_tenant_context=True` — in its read and query paths (`load(aggregate_id)`,
`get_or_raise(aggregate_id)`, `get_or_none(aggregate_id)`, `load_or_create(aggregate_id)`,
`exists(aggregate_id)`, and `get_version(aggregate_id)`). A `TenantContextNotSetError`
raised from any of these means the same thing as one raised from your own code:
the unit of work was not wrapped in a scope. Fix that at the request boundary
(Step 1) rather than catching the error where it surfaced.

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

- **Outside any tenant scope the callable returns `None`, and `handle()` raises
  `TenantContextNotSetError`.** The event is not processed. Retries do not help,
  so fix the missing scope at the call site. A static `UUID` filter never raises
  this error. Set `tenant_filter_allow_unscoped = True` on the class only to
  restore the legacy behavior, where `None` processes events of every tenant. Do
  not set it for an isolated read model.
- **The callable runs on the projection's event-handling path**, so keep it
  cheap and non-blocking. A `ContextVar` read is effectively free; a database
  lookup per event is not — resolve that once and cache it in the cell your
  callable reads.

If the projection is driven by a subscription runner rather than by your request
handler, remember that the runner, not the request, calls `handle()`. Establish
the scope around that call, for example inside the handler you register with
the subscription. Read "Run one projection instance per tenant" before you run
more than one instance.

## Run one projection instance per tenant

The checkpoint key is the projection name. By default the name is the class
name, not the instance. Two instances of one class that use the default name
share one checkpoint row. The instance that advances the row moves it past
events that the other instance has not handled. The other instance then skips
those events.

Give each per-tenant instance its own name. Pass `projection_name` to the
constructor:

```python
tenant_projection = OrderSummary(
    checkpoint_repo=checkpoints,
    tenant_filter=tenant_id,
    projection_name=f"OrderSummary:{tenant_id}",
)
```

Each instance now has its own checkpoint row. The name also labels the
instance's dead letter entries, logs, and spans. Use the same name on every
restart. The name must be 1 to 255 characters and must not contain control
characters.

Use these rules with `projection_name`:

- The name is the checkpoint key. Changing it on a running projection starts
  a new checkpoint at the beginning of the feed.
- A projection with a callable filter resolves its tenant per event. It cannot
  have one name per tenant. Use a fixed `UUID` filter for each per-tenant
  instance, or use the first option below.

A checkpoint advances over every event that the tenant filter skips. The
projection does not handle a skipped event, but the checkpoint still moves past
it. Projections that share a name therefore share one checkpoint row. One
instance can move the row past events that the other instance must handle.
Give every projection instance its own name.

Choose one of these options:

- **Run one instance per class.** Use a callable filter, and set the scope
  around each `handle()` call. See "Scope a projection to the request's tenant".
  Use one shared checkpoint for all tenants. Accept that the checkpoint is
  shared and document that choice.
- **Use one class per tenant when the tenant set is fixed.** Each class has its
  own checkpoint key. Each class reads every event and skips the events of other
  tenants, so the read cost grows with the number of tenants.

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

The resolved value feeds `_should_process_event(event)`. An unset filter (`None`)
processes everything. A callable that returns `None` — `get_current_tenant`
outside any tenant scope, for instance — raises `TenantContextNotSetError`,
unless the class sets `tenant_filter_allow_unscoped = True`.

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

### Events with no `tenant_id` -> skipped under a set filter

`_should_process_event` reads the tenant with `getattr(event, "tenant_id", None)`.
Two situations collapse to the same `None`:

- the event class declares `tenant_id` but this instance left it unset, which is
  the common case for anything not built through `TenantDomainEvent` or
  `with_tenant_context(...)`;
- the event class has no `tenant_id` attribute at all.

Neither value equals a set filter, so the event is skipped like any other
other-tenant event:

```python
acme = UUID("11111111-1111-1111-1111-111111111111")
projection = OrderSummaryProjection(tenant_filter=acme)

await projection.handle(OrderCreated(aggregate_id=uuid4(), tenant_id=None))  # skipped
await projection.handle(LegacyImported(aggregate_id=uuid4()))                # skipped
```

With no filter set, the same events are processed:

```python
projection = OrderSummaryProjection()
await projection.handle(OrderCreated(aggregate_id=uuid4(), tenant_id=None))  # processed
```

Global events, such as schema markers or imports, therefore need a projection
with no filter, or a separate projection class. A tenant filter is not an
authorization check. Check the caller's rights in your handler or in the code
that calls it. The filter only chooses which events a projection handles.

### `tenant_filter=None` (default) -> no filtering, all events processed

`_should_process_event` returns `True` immediately when the filter is unset,
without touching the event. Omitting `tenant_filter` therefore preserves
pre-tenant behaviour exactly. A callable that returns `None` does not reach this
branch. It raises, as described above, unless `tenant_filter_allow_unscoped` is
`True`.

| Filter value | Event `tenant_id` | Result |
| --- | --- | --- |
| `None` | anything | processed |
| `UUID` | equal `UUID` | processed |
| `UUID` | different `UUID` | skipped, DEBUG log, checkpoint advances |
| `UUID` | `None` / attribute absent | skipped, DEBUG log, checkpoint advances |

## Reading, querying, and executing with `TenantAwareRepository`

`TenantAwareRepository` exposes the same read, query, and command surface as
`AggregateRepository`:

- **`await tenant_repo.load(aggregate_id)`** and **`await tenant_repo.get_or_raise(aggregate_id)`**:
  Load an existing aggregate by ID. Raises `AggregateNotFoundError` when the
  stream is missing or, under a scope, belongs to another tenant.
  `get_or_raise(aggregate_id)` is an intent-revealing alias for
  `load(aggregate_id)`.
- **`await tenant_repo.get_or_none(aggregate_id)`**:
  Returns the aggregate, or `None` when the stream is missing or belongs to
  another tenant.
- **`await tenant_repo.exists(aggregate_id)`** and **`await tenant_repo.get_version(aggregate_id)`**:
  Query whether the aggregate stream exists (`bool`) or inspect its current
  version (`int`). Under a scope, a stream that another tenant owns reads as
  absent: `False` and `0`.
- **`await tenant_repo.execute(aggregate_id, command, *more_commands, create_if_missing=True)`**:
  Loads (or creates) the aggregate, applies one or more commands or callables
  sequentially, and saves with tenant consistency validation.

When `require_tenant_context=True`, `load(aggregate_id)`,
`get_or_raise(aggregate_id)`, `get_or_none(aggregate_id)`,
`load_or_create(aggregate_id)`, `exists(aggregate_id)`, and
`get_version(aggregate_id)` all verify `get_required_tenant()` before
delegating to the wrapped `AggregateRepository`. Under an active scope, these
calls also check stream ownership, whether or not `require_tenant_context` is
set. See "Stream ownership under a scope".

## Stream ownership under a scope

The owner of a stream is the tenant of its events. When a tenant scope is
active, `TenantAwareRepository` applies these rules. Unscoped calls are not
checked at all. A repository built with `require_tenant_context=False` does this
on purpose, for system code.

- **Load.** Every event in the stream must carry the active tenant. A stream
  with one event of another tenant, or with one event that has no tenant, is
  refused. The refusal is `AggregateNotFoundError`, the same error as a missing
  stream.
- **Snapshots.** A snapshot is used under a scope only when its `tenant_id`
  equals the active tenant. Any other snapshot is ignored, and the stream is
  replayed in full. A snapshot with no tenant is ignored too. Snapshots record
  the owner tenant when they are written. A background snapshot takes the owner
  from the events it was built from, so it keeps the right owner after the scope
  exits.
- **Exists and version.** The owner is the tenant of the first event of the
  stream. A foreign stream reads as absent: `exists` returns `False`, and
  `get_version` returns `0`.
- **Save with a prior version above zero.** The repository checks the owner
  before it appends. A foreign stream raises `AggregateNotFoundError`, and
  nothing is appended. This check runs whether or not `validate_on_save` is set.
- **Save with a new aggregate.** An aggregate with prior version `0` is not
  checked by the repository. When the stream already exists, the store's
  concurrency check fails the append with `OptimisticLockError`. Nothing is
  appended.
- **Save with validation on.** With `validate_on_save=True` (the default), a
  scope rejects any uncommitted event whose `tenant_id` is `None`, and any event
  of another tenant. The error is `TenantMismatchError`, and `actual` is `None`
  for an untenanted event. Without a scope, `save()` raises
  `TenantContextNotSetError`.
- **Command tenant.** When a command or an explicit `tenant_id` contradicts the
  active tenant, `create_event()` raises `TenantMismatchError` before the event
  is applied.

Code that runs in a scope sees foreign streams as missing. The example below
shows the same aggregate id read from two scopes:

```python
from eventsource import tenant_scope
from eventsource.application.aggregates.tenant_repository import TenantAwareRepository
from eventsource.domain.exceptions import AggregateNotFoundError

repo = TenantAwareRepository(order_repository)

async with tenant_scope(acme):
    order = await repo.load(order_id)        # owned by acme: loads

async with tenant_scope(globex):
    assert await repo.exists(order_id) is False
    assert await repo.get_version(order_id) == 0
    try:
        await repo.load(order_id)
    except AggregateNotFoundError:
        pass                                  # foreign stream reads as missing
```

## Auditing existing streams

Streams written before you adopted tenant ownership can mix tenants, or can hold
events with no tenant. Scoped code refuses to load these streams. Run the query
below before you turn on scoped reads for a store that already holds data.

The query is read-only. It returns one row for each stream that has an event
with no tenant, or with a tenant that differs from the tenant of the stream's
first event. The result has the columns `aggregate_type`, `aggregate_id`, and
`flagged_events`.

<!-- audit-query -->
```sql
SELECT e.aggregate_type, e.aggregate_id, COUNT(*) AS flagged_events
FROM events AS e
JOIN events AS first_event
  ON first_event.aggregate_type = e.aggregate_type
 AND first_event.aggregate_id = e.aggregate_id
 AND first_event.version = (
       SELECT MIN(v.version)
       FROM events AS v
       WHERE v.aggregate_type = e.aggregate_type
         AND v.aggregate_id = e.aggregate_id)
WHERE e.tenant_id IS NULL
   OR e.tenant_id <> first_event.tenant_id
GROUP BY e.aggregate_type, e.aggregate_id
ORDER BY e.aggregate_type, e.aggregate_id
```

The same text runs on PostgreSQL (12 or later) and on SQLite. The `events`
table has the same column names on both. The query scans the whole table, so
run it on a replica or in a quiet window for a large store.

The query does not change data. It does not repair a stream. Decide what to do
with each flagged stream. Read "Limitations" for the streams that no current step
can fix.

## Read models under a scope

`TenantScopedReadModelRepository` wraps any read model repository. Import it
from `eventsource.application.readmodels`. It is not in the top-level package.
The read model must have a `tenant_id` field:

```python
from uuid import UUID

from eventsource import ReadModel, tenant_scope
from eventsource.adapters.memory.readmodels import InMemoryReadModelRepository
from eventsource.application.readmodels import TenantScopedReadModelRepository


class OrderSummary(ReadModel):
    tenant_id: UUID | None = None
    total: int = 0


orders = TenantScopedReadModelRepository(InMemoryReadModelRepository(OrderSummary))

async with tenant_scope(tenant_id):
    summary = await orders.get(order_id)  # None when the row belongs to another tenant
    await orders.save(OrderSummary(id=order_id, tenant_id=tenant_id, total=40))
```

Under an active scope:

- Reads add the active tenant as a filter. A row of another tenant looks like a
  missing row. `get` returns `None`, `exists` returns `False`, and `find` and
  `count` leave the row out.
- A row with no tenant is invisible under every scope.
- A save checks the owner of the id first, including soft-deleted rows. A save
  to an id that another tenant owns raises `TenantMismatchError`. A model whose
  `tenant_id` differs from the active tenant also raises it.
- `save_many` checks every model before it writes. If one model fails, the
  batch writes nothing.
- `delete`, `soft_delete`, and `restore` return `False` for an id that another
  tenant owns. The row does not change.
- `truncate` raises `TenantScopeOperationError`. Run it outside any scope.
- Do not pass `tenant_id=` as a filter keyword. The wrapper raises `ValueError`.
- A model with no `tenant_id` field raises `ValueError` from the wrapped
  repository under a scope.

With no scope, every call passes straight to the wrapped repository. Set
`require_tenant_context=True` on the wrapper to raise `TenantContextNotSetError`
when no scope is active.

**Adopting an existing table.** Rows saved before you add `tenant_id` have no
owner. Under a scope they are invisible. Backfill the column with the owner of
each row before you put the wrapper in front of the table.

## Dead letter queue under a scope

A dead letter queue (DLQ) entry has an owner, `DLQEntry.tenant_id`. The framework
sets it from the failed event. An event with no tenant gives an entry with no
owner.

Under an active scope:

- `get_failed_events`, `get_failed_event_by_id`, `mark_resolved`,
  `mark_retrying`, `get_failure_stats`, and `get_projection_failure_counts` act
  only on entries of the active tenant.
- An entry with no owner is invisible under every scope. Unscoped calls see it.
- `delete_resolved_events` raises `TenantScopeOperationError`. Run retention
  cleanup outside any scope.

The owner is set on the first insert. A later failure of the same event in the
same projection does not change it.

## Upgrade notes

These changes affect code written against earlier releases. Read them before you
upgrade.

1. **A callable tenant filter that returns `None` raises.** `handle()` raises
   `TenantContextNotSetError` and does not process the event. To keep the old
   behavior, set `tenant_filter_allow_unscoped = True` on that projection class.
   Do not set it on an isolated read model.
2. **A set tenant filter skips events with no tenant.** Before this change, these
   events were processed. Use a projection with no filter for global events.
3. **`save()` rejects untenanted events under a scope.** The error is
   `TenantMismatchError`, and `actual` is `None`. Before this change, validation
   skipped these events.
4. **A command or explicit `tenant_id` that contradicts the active tenant raises.**
   `create_event()` raises `TenantMismatchError`. Before this change, such events
   were accepted.
5. **Stream ownership applies under a scope.** Load raises `AggregateNotFoundError`
   for a foreign or untenanted stream. `exists` returns `False`, and `get_version`
   returns `0`. A save to a foreign stream raises `AggregateNotFoundError` and
   appends nothing. These checks run with `validate_on_save=False` too.
6. **Snapshots carry an owner.** `Snapshot` has a new optional field,
   `tenant_id`, which defaults to `None`, so existing callers still work. Under a
   scope, a snapshot is used only when its tenant matches. Existing snapshot rows
   have no tenant. Under a scope they are ignored, so each aggregate replays once
   and is snapshotted again. No data migration is needed.
7. **Audit before you enable scoped reads.** Run the query in "Auditing existing
   streams" on any store that holds data from before tenant ownership.
8. **Add the DLQ owner column (PostgreSQL).** Run
   `src/eventsource/adapters/sql/schemas/updates/007_add_dlq_tenant.sql` once.
   The script adds a nullable `tenant_id` column. Until you run it, the adapter
   writes DLQ entries with no owner. It logs one warning for each repository
   instance. Under a scope its reads return no entries, and unscoped reads still
   work. Restart the process after you run the script. SQLite adds the column
   the first time the adapter uses the table. The file
   `007_add_dlq_tenant_sqlite.sql` is for manual runs. It fails with
   "duplicate column name" if the column exists.
9. **Backfill read model owners.** Read model rows saved before you add
   `tenant_id` are invisible under a scope. See "Read models under a scope".

## Limitations

The ownership checks apply to `TenantAwareRepository` under an active scope. They
do not cover these paths:

- **Direct store access.** Code that calls an event store or a snapshot store
  directly bypasses every check. Limit direct access to trusted code.
- **Unscoped repositories.** Without a scope, `load`, `exists`, and `get_version`
  apply no ownership check. This is intended for system code.
- **Global event id uniqueness.** Event ids are unique across all tenants. An
  append that reuses an event id from another tenant raises
  `DuplicateEventError`. That error tells the caller that the foreign event id
  exists. This is an accepted residual risk (ADR-0113). Event ids are the
  deduplication key for the bus, the outbox, and event lookup, so they stay
  global.
- **Mixed legacy streams.** `exists` and `get_version` read only the first event
  of a stream. A stream that mixes tenants therefore reports as existing, with
  its full version, to the tenant of its first event. `load` still refuses it.
  Use the audit query to find these streams.
- **Pre-tenancy streams.** A stream with no tenant on any event is owned by no
  scope, so scoped code cannot load it. No supported step assigns an owner to
  existing untenanted events yet. This is a follow-up task (ADR-0113).
- **The outbox and the bus form one trust domain.** The outbox stores `tenant_id`
  as a column. The bus and the Kafka publisher carry it as a field or a header.
  Neither one filters by tenant. Every subscriber receives events for every
  tenant. A subscriber that serves one tenant must filter in its own code. Do not
  treat the header as an access control.
- **Projections and subscriptions.** A projection filters only when you set
  `tenant_filter`, and the filter only skips events. See "Run one projection
  instance per tenant" for the checkpoint risk.
- **Save to another tenant's id.** Under a scope, a save to an id that another
  tenant owns raises `TenantMismatchError`. The error shows that the id exists
  for another tenant. This is an accepted residual risk (ADR-0114).
- **Bare read model and DLQ repositories.** The read model wrapper and the DLQ
  scope apply in the application and the DLQ adapter. Code that calls the bare
  repository directly bypasses both.
- **Checkpoints have no owner.** A checkpoint row is keyed by the projection
  name only. See "Run one projection instance per tenant".
- **Physical isolation.** Separate databases, separate schemas, and PostgreSQL
  row-level security remain your responsibility.
