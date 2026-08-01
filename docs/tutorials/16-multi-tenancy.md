# Tutorial 16: Multi-Tenancy

In this tutorial you will make one event-sourced aggregate safe to share between several
tenants, using the library's multi-tenancy support: `TenantDomainEvent`,
`tenant_scope()`, and `TenantAwareRepository`.

A *tenant* is a customer whose data must never mix with another customer's. In an
event-sourced system the risk is concrete: every event you append is permanent, so a
single event written under the wrong tenant is a leak you cannot quietly update away.
The approach here is to carry the tenant as *ambient context* -- a `ContextVar` you set
once per request with `async with tenant_scope(tenant_id)` -- and then let event
construction and the repository read that context instead of trusting each call site to
pass the right UUID.

You will define a tenant-scoped event whose `tenant_id` is required rather than
optional; set the ambient tenant for a block of async code; build events from context
with `TenantDomainEvent.with_tenant_context()` and with `create_event()`, which
auto-populates `tenant_id` from the same context; wrap an `AggregateRepository` in
`TenantAwareRepository` so that saving validates every uncommitted event against the
current scope; and deliberately provoke both failure modes -- `TenantMismatchError` when
an event carries a foreign tenant, and `TenantContextNotSetError` when there is no scope
at all. From there you will look at how context behaves across concurrent tasks and in
synchronous callers, at manual control with `set_current_tenant()` /
`get_current_tenant()` / `clear_tenant_context()`, and at the two configuration knobs
`enforce_on_load` and `validate_on_save`.

Everything runs in memory -- no database and no Docker -- so you can watch the isolation
rules fire immediately. That also makes an important limitation visible along the way:
these checks guard *writes*. Filtering reads by tenant is not something the wrapper does
for you, which is why the last steps cover database-level isolation and how to test that
one tenant genuinely cannot see another's data.

By the end you will have a tenant-aware aggregate and repository, and a clear sense of
which guarantees come from the library and which you still have to enforce in your
storage layer.

## What You'll Build

A single `Subscription` aggregate -- the sort of thing a SaaS product gives every
customer one of -- shared by two tenants, `tenant_a` and `tenant_b`, in one process.
You will build it up in one file, `multi_tenancy.py`, and run it after every step.

By the end that file contains:

1. **`SubscriptionStarted` and `PlanChanged`**, subclasses of `TenantDomainEvent`, whose
   `tenant_id` is a required `UUID` rather than the optional field it is on plain
   `DomainEvent`.
2. **A `Subscription` aggregate** built with `DeclarativeAggregate` and `@handles`, whose
   command methods call `self.create_event(...)` -- which fills in `aggregate_id`,
   `aggregate_type`, `aggregate_version`, and `tenant_id` from the ambient context.
3. **A `TenantAwareRepository`** wrapping an ordinary `AggregateRepository` over an
   `InMemoryEventStore`, validating every uncommitted event on `save()`.
4. **Two deliberate failures**: a `TenantMismatchError` raised when tenant B's scope tries
   to save an event stamped for tenant A, and a `TenantContextNotSetError` raised when
   you save with no scope at all.
5. **A strict variant** constructed with `enforce_on_load=True`, so `load()`, `exists()`,
   and `load_or_create()` also refuse to run outside a scope.
6. **A concurrency check**: two `asyncio` tasks in different `tenant_scope()` blocks
   running at once, each seeing only its own tenant -- because the context lives in a
   `ContextVar`, not a global.
7. **A test** that asserts the isolation rules hold, using `pytest.raises` around the two
   error cases.

Everything is in-memory and single-file. No Docker, no PostgreSQL, no migrations -- the
storage layer is deliberately dumb so that every guarantee you see comes from the
multi-tenancy module itself, and every gap it leaves is equally visible.


## Prerequisites

- **Tutorial 2 (Your First Event)** and **Tutorial 3 (Your First Aggregate)** --
  `TenantDomainEvent` is a subclass of `DomainEvent`, and everything here is built on
  `DeclarativeAggregate`, `@handles`, and `AggregateRepository`. If those are not yet
  familiar, the tenant rules will be hard to see through the scaffolding.
- **Tutorial 8 (Testing)** is helpful but not required -- only the last step uses
  `pytest`.
- **Comfort with `asyncio`**, including `async with`, `asyncio.run()`, and
  `asyncio.gather()`. Step 11 runs two tenant scopes concurrently.
- **A rough idea of what a `ContextVar` is.** You do not need to have used
  `contextvars` directly; the point to hold onto is that the value is per-task, not
  per-process, which is exactly why the concurrency step works.
- **Python 3.13 or newer** -- the floor declared in `pyproject.toml`.

Install the library. Multi-tenancy is part of the core package, so no extras are
needed for this tutorial:

```bash
uv sync --all-extras          # or: pip install eventsource-py
```

Everything runs on `InMemoryEventStore`, so there is no database to start and no
Docker Compose file to bring up.

Two import notes worth knowing before you start typing, because they are easy to get
wrong:

- `TenantDomainEvent`, `tenant_scope`, `tenant_scope_sync`, `get_current_tenant`,
  `set_current_tenant`, `clear_tenant_context`, `get_required_tenant`,
  `TenantContextNotSetError`, and `TenantMismatchError` are all re-exported from the
  top-level `eventsource` package.
- `TenantAwareRepository` is **not**. Import it from
  `eventsource.application.aggregates.tenant_repository`.

```python
from eventsource import TenantDomainEvent, tenant_scope
from eventsource.application.aggregates.tenant_repository import TenantAwareRepository
```

Create a file called `multi_tenancy.py` in an empty directory and add to it as you go.
Each step ends with something you can run.

## Step 1: Why Tenant Isolation Needs More Than a Column

Before writing any code, it is worth being precise about what the library does and does
not do for you, because the shape of the rest of this tutorial follows directly from it.

The obvious approach to multi-tenancy is to add a `tenant_id` column and remember to fill
it in. `DomainEvent` already gives you the column:

```python
tenant_id: UUID | None = Field(
    default=None,
    ...
)
```

It is optional and defaults to `None`. That default is the problem. Nothing stops you
from appending an event with `tenant_id=None`, and nothing stops you from appending one
with the *wrong* tenant's UUID -- a stale variable from an earlier request, a value
threaded through five function calls and reassigned in the middle one. In a CRUD system a
mistake like that is a bad row you can `UPDATE`. In an event-sourced system the event log
is append-only: the wrong event is now part of the aggregate's history, it replays into
every projection built from that stream, and the only remedy is a compensating event that
still leaves the original visible.

So the module attacks the problem from two directions at once.

**First, make the field impossible to omit.** `TenantDomainEvent` overrides the inherited
field with a required one:

```python
tenant_id: UUID = Field(
    ...,
    description="Tenant this event belongs to (required)",
)
```

Because `DomainEvent` is a Pydantic model, that is enforced at construction. An event
class that subclasses `TenantDomainEvent` cannot be instantiated without a tenant at all
-- `None` is no longer in the type, and validation fails before the event exists, let
alone reaches a store.

**Second, stop passing the tenant by hand.** Required-ness only guarantees that *some*
UUID is present, not that it is the right one. This is what `tenant_context` -- a
`ContextVar[UUID | None]` in `eventsource.domain.tenant_context` -- is for. You set it once
at the edge of a request with `async with tenant_scope(tenant_id)`, and everything inside
that block reads the tenant from context rather than from an argument:
`TenantDomainEvent.with_tenant_context()` fills the field from it, `create_event()` on a
tenant-aware aggregate fills it from it, and `get_required_tenant()` raises
`TenantContextNotSetError` rather than silently returning `None` if the scope was never
entered. A `ContextVar` is the right container here because its value is per-task, not
per-process: two `asyncio` tasks serving two customers concurrently see two different
tenants, which a module-level global could never do. Step 11 demonstrates exactly that.

**Then verify at the boundary.** Context can still be circumvented -- an event
constructed with an explicit `tenant_id`, or built earlier under a different scope and
saved later. `TenantAwareRepository.save()` is the last check before persistence. It walks
`aggregate.uncommitted_events`, compares each event's `tenant_id` against
`get_required_tenant()`, and raises `TenantMismatchError` (carrying `expected`, `actual`,
and the offending `event_ids`) if any of them disagree. Nothing is written: the check runs
*before* delegation to the wrapped repository, so a single bad event aborts the whole
save.

That is the whole model -- required field, ambient context, validation at the write
boundary. Three cooperating pieces rather than one column, because each one closes a hole
the others leave open.

Two limits are worth internalizing now rather than discovering in production.

**These are write-side guarantees only.** `TenantAwareRepository.load()` does not filter
events by tenant. With the default `enforce_on_load=False` it is a plain delegation; with
`enforce_on_load=True` it checks only that *a* tenant context exists before loading. The
docstring is explicit that filtering "would require EventStore changes." If two tenants'
events somehow share an aggregate stream, a load returns all of them. Read isolation has
to come from your storage layer -- separate databases, separate schemas, or PostgreSQL
row-level security. Step 15 covers this.

**Events without a `tenant_id` are skipped, not rejected.** `_validate_tenant_consistency`
reads `getattr(event, "tenant_id", None)` and `continue`s when it is `None`. This is a
deliberate accommodation for aggregates that emit both tenant-scoped and system-level
events (Step 14) and for incremental migrations -- but it means a plain `DomainEvent`
sneaking into a tenant-scoped aggregate passes validation silently. Subclassing
`TenantDomainEvent` is what makes the check meaningful, which is why the next step starts
there.

## Step 2: Define a Tenant-Scoped Event with TenantDomainEvent

Start `multi_tenancy.py` with the two events the `Subscription` aggregate will emit. Both
subclass `TenantDomainEvent` rather than `DomainEvent`:

```python
from __future__ import annotations

from uuid import UUID, uuid4

from pydantic import ValidationError

from eventsource import TenantDomainEvent


class SubscriptionStarted(TenantDomainEvent):
    """A tenant signed up for a plan."""

    aggregate_type: str = "Subscription"
    plan: str
    seats: int


class PlanChanged(TenantDomainEvent):
    """A tenant moved to a different plan."""

    aggregate_type: str = "Subscription"
    old_plan: str
    new_plan: str
```

That is the entire difference from an ordinary event definition -- one base class. You do
not declare `tenant_id` yourself; `TenantDomainEvent` already declares it as
`tenant_id: UUID = Field(..., description="Tenant this event belongs to (required)")`,
which shadows the optional `tenant_id: UUID | None = None` inherited from `DomainEvent`.
Your subclass adds only the payload fields it cares about.

Everything else about these classes is a normal `DomainEvent`. `event_type` is still
auto-derived from the class name, `aggregate_type` is still a plain field you default on
the class, and the model is still frozen. Add a `__main__` block and see all of it at
once:

```python
if __name__ == "__main__":
    tenant_a = uuid4()
    subscription_id = uuid4()

    started = SubscriptionStarted(
        aggregate_id=subscription_id,
        tenant_id=tenant_a,
        plan="team",
        seats=5,
    )
    print("event_type:", started.event_type)
    print("tenant matches:", started.tenant_id == tenant_a)
    print("serialized:", started.to_dict()["tenant_id"] == str(tenant_a))
```

```console
$ python multi_tenancy.py
event_type: SubscriptionStarted
tenant matches: True
serialized: True
```

Note the third line: `to_dict()` emits `tenant_id` as a *string*, because
`DomainEvent.to_dict()` runs the model through JSON-mode serialization. When the event
comes back through `from_dict()` Pydantic parses it into a `UUID` again, so the round trip
preserves the tenant -- but if you are reading raw stored payloads, compare against
`str(tenant_id)`, not the `UUID`.

### Watching the requirement fire

The value of the required field is that it fails at construction, long before anything
touches an event store. Add these to the `__main__` block:

```python
    try:
        SubscriptionStarted(aggregate_id=subscription_id, plan="team", seats=5)
    except ValidationError as exc:
        print("missing tenant_id rejected:", "tenant_id" in str(exc))

    try:
        SubscriptionStarted(
            aggregate_id=subscription_id,
            tenant_id=None,  # type: ignore[arg-type]
            plan="team",
            seats=5,
        )
    except ValidationError:
        print("explicit None rejected")

    try:
        started.tenant_id = uuid4()  # type: ignore[misc]
    except ValidationError:
        print("frozen: tenant_id cannot be reassigned")
```

```console
$ python multi_tenancy.py
event_type: SubscriptionStarted
tenant matches: True
serialized: True
missing tenant_id rejected: True
explicit None rejected
frozen: tenant_id cannot be reassigned
```

Three separate protections, and it is worth naming them individually because they close
different holes:

1. **Omitting `tenant_id`** raises `pydantic.ValidationError` -- the `missing` error kind,
   with `tenant_id` in the error's `loc`. On a plain `DomainEvent` this same call would
   have succeeded and quietly produced `tenant_id=None`.
2. **Passing `tenant_id=None` explicitly** also raises. `None` is not merely un-defaulted,
   it is out of the declared type. A helper that threads an `Optional[UUID]` down from a
   request handler cannot slip a `None` through by being explicit about it.
3. **Reassignment after construction** raises too, because `DomainEvent` sets
   `model_config = ConfigDict(frozen=True)`. An event's tenant is fixed the instant the
   event exists; nothing downstream can retag it.

The `# type: ignore` comments are there because mypy already rejects both of those calls
statically -- which is the point. Under a type checker these are compile-time errors; at
runtime they are validation errors. You have to work to get a tenant-less event, and this
tutorial only does it to show you the failure.

### What this does *not* buy you

Be clear about the limit before moving on. `SubscriptionStarted` above was constructed
with an explicit `tenant_a` passed in by hand. Nothing checked that `tenant_a` was the
right tenant -- only that *some* UUID was present. Swap in a stale variable and the event
is still perfectly valid, still permanent, and now belongs to the wrong customer.

Required-ness eliminates the *empty* tenant. It cannot eliminate the *wrong* tenant,
because at construction time the class has no idea which tenant the current request is
for. That is exactly what the ambient context solves, and it is what Step 3 sets up.

Run the file once more to confirm all six lines print, then continue.

## Step 3: Set the Ambient Tenant with `async with tenant_scope(tenant_id)`

Step 2 left you with an event class that refuses to be built without a tenant, and no way
to know *which* tenant is correct. `tenant_scope()` is the answer: an async context
manager that publishes the current tenant to everything running inside its block, so no
call site has to be handed a UUID.

Underneath it is a single module-level `ContextVar`, declared in
`eventsource.domain.tenant_context`:

```python
tenant_context: ContextVar[UUID | None] = ContextVar("tenant_context", default=None)
```

`tenant_scope()` sets it on entry, keeps the `Token` that `ContextVar.set()` returns, and
resets it in a `finally` block on exit. That is the whole implementation, and the two
details that matter follow from it: the block *restores* the previous value rather than
clearing to `None` (which is what makes nesting work), and the reset happens even if the
body raises.

Add the imports to the top of `multi_tenancy.py`:

```python
import asyncio

from eventsource import (
    TenantContextNotSetError,
    TenantDomainEvent,
    get_current_tenant,
    get_required_tenant,
    tenant_scope,
)
```

Now replace your `__main__` block's tail with an async function that exercises the scope.
Keep the event definitions from Step 2 above it:

```python
async def explore_context() -> None:
    tenant_a = uuid4()
    tenant_b = uuid4()

    print("before scope:", get_current_tenant())

    async with tenant_scope(tenant_a) as active:
        print("yielded value is the tenant:", active == tenant_a)
        print("inside scope:", get_current_tenant() == tenant_a)

        async with tenant_scope(tenant_b):
            print("nested scope:", get_current_tenant() == tenant_b)

        print("restored after nested:", get_current_tenant() == tenant_a)

    print("after scope:", get_current_tenant())


if __name__ == "__main__":
    asyncio.run(explore_context())
```

```console
$ python multi_tenancy.py
before scope: None
yielded value is the tenant: True
inside scope: True
nested scope: True
restored after nested: True
after scope: None
```

Six lines, and each one is a property you will rely on later.

**The default is `None`, not an error.** Outside any scope, `get_current_tenant()` returns
`None` and never raises -- it is the "tell me if there is one" accessor. That is why the
first and last lines print `None` rather than blowing up.

**The scope yields the tenant.** `async with tenant_scope(tenant_a) as active` binds
`active` to the same UUID you passed in. Useful when the tenant came from a request header
or a lookup and you would otherwise keep a separate variable around.

**Nesting restores, it does not clear.** The inner `tenant_scope(tenant_b)` block shadows
tenant A; leaving it puts tenant A back. This falls out of `tenant_context.reset(token)`
-- the token remembers the prior value. A naive implementation that called
`clear_tenant_context()` on exit would have printed `False` on the fifth line and silently
dropped you out of tenant A's scope while still inside its `async with`.

### Requiring a tenant

Alongside the permissive accessor there is a strict one. `get_required_tenant()` returns
the same UUID when a scope is active, and raises `TenantContextNotSetError` when there is
none. This is the function `TenantAwareRepository` calls internally, so the error you will
see in Step 9 originates here.

```python
    try:
        get_required_tenant()
    except TenantContextNotSetError as exc:
        print("required outside scope:", exc)
```

```console
required outside scope: No tenant context set. Use set_current_tenant() or tenant_scope() before performing multi-tenant operations.
```

Use `get_current_tenant()` when a missing tenant is a legitimate state -- a background job
that serves all tenants, say -- and `get_required_tenant()` everywhere a missing tenant
means the caller forgot to open a scope. Defaulting to the strict one is the safer habit:
`None` propagating quietly into an event payload is precisely the failure mode Step 1
described.

### Cleanup survives exceptions

Because the reset lives in a `finally`, an exception escaping the block still restores the
prior context. Worth confirming, since a leaked tenant after an error is exactly how one
customer's request ends up writing under another's identity:

```python
    try:
        async with tenant_scope(tenant_a):
            raise RuntimeError("boom")
    except RuntimeError:
        pass
    print("cleaned up after exception:", get_current_tenant())
```

```console
cleaned up after exception: None
```

This is the main reason to prefer `tenant_scope()` over the manual
`set_current_tenant()` / `clear_tenant_context()` pair. The manual functions exist and are
covered in Step 13, but they put the cleanup on you, and any `return`, `raise`, or early
exit between the two calls leaks the tenant into whatever runs next on that task.

### Where the scope belongs

In a real application you enter the scope once, at the outermost edge of a request, and
never again -- ASGI middleware that resolves a tenant from a subdomain, JWT claim, or API
key; a consumer that reads the tenant off a message header; a CLI command that takes
`--tenant`. Sketched for a middleware:

```python
async def tenant_middleware(request, call_next):
    tenant_id = resolve_tenant(request)  # subdomain, JWT claim, API key...
    async with tenant_scope(tenant_id):
        return await call_next(request)
```

Everything downstream -- handlers, aggregates, repository -- reads the tenant from context
instead of accepting it as a parameter. That is the payoff: the tenant stops being an
argument that can be forgotten or mistyped at any of a hundred call sites, and becomes a
property of the execution context itself.

One thing the scope does *not* do: it does not touch your events. Setting the context does
not retroactively stamp anything, and constructing a `SubscriptionStarted` inside a scope
still requires a `tenant_id` argument -- Pydantic does not consult a `ContextVar`. Step 4
closes that gap with `with_tenant_context()`, the constructor that reads the value you
just learned to set.

## Step 4: Construct Events via `TenantDomainEvent.with_tenant_context()`

You now have a required `tenant_id` field (Step 2) and an ambient tenant in a
`ContextVar` (Step 3). This step joins them. `with_tenant_context()` is a classmethod on
`TenantDomainEvent` that reads the current scope and fills `tenant_id` in for you, so the
call site never names a tenant at all.

Its whole body is three lines:

```python
@classmethod
def with_tenant_context(cls, **kwargs: Any) -> Self:
    if "tenant_id" not in kwargs:
        kwargs["tenant_id"] = get_required_tenant()
    return cls(**kwargs)
```

Note which accessor it uses: `get_required_tenant()`, the strict one. Constructing an
event outside a scope is not a "no tenant yet" state, it is a bug, so the classmethod
raises rather than producing an event with `tenant_id=None` -- which the required field
would reject a moment later anyway, with a less informative message.

Replace `explore_context()` with `build_events()` in `multi_tenancy.py`:

```python
async def build_events() -> None:
    tenant_a = uuid4()
    tenant_b = uuid4()
    subscription_id = uuid4()

    async with tenant_scope(tenant_a):
        started = SubscriptionStarted.with_tenant_context(
            aggregate_id=subscription_id,
            plan="team",
            seats=5,
        )
        print("tenant from context:", started.tenant_id == tenant_a)
        print("payload intact:", started.plan, started.seats)


if __name__ == "__main__":
    asyncio.run(build_events())
```

```console
$ python multi_tenancy.py
tenant from context: True
payload intact: team 5
```

No `tenant_id=` anywhere in that call. Every other field passes straight through to the
normal Pydantic constructor -- payload fields, `aggregate_id`, and the inherited
`DomainEvent` fields like `aggregate_version` and `actor_id` all work exactly as they
would if you had called `SubscriptionStarted(...)` directly. `with_tenant_context()` is a
`**kwargs` passthrough with one key injected; it is not a restricted subset of the
constructor.

### Explicit `tenant_id` still wins

The injection only happens when the key is absent. Pass `tenant_id` yourself and the
context is ignored entirely:

```python
        override = SubscriptionStarted.with_tenant_context(
            aggregate_id=subscription_id,
            plan="enterprise",
            seats=50,
            tenant_id=tenant_b,
        )
        print("explicit wins:", override.tenant_id == tenant_b)
```

```console
explicit wins: True
```

That is a documented behavior, not a loophole to be closed -- a system-level job that
legitimately writes on behalf of a specific tenant needs a way to say so. But look at what
you just built: an event stamped `tenant_b`, constructed inside `tenant_a`'s scope, with
no complaint from Pydantic. Both halves of the design so far accept it. This is exactly the
event that Step 8 will hand to `TenantAwareRepository.save()` to produce a
`TenantMismatchError`, and the reason the repository check has to exist at all. Required
fields and ambient context cannot catch a caller who is explicit about being wrong; only
the write boundary can.

### Nesting resolves to the innermost scope

Because the classmethod reads the `ContextVar` at call time, it sees whatever scope is
currently innermost -- the restore-on-exit behavior from Step 3 applies unchanged:

```python
        async with tenant_scope(tenant_b):
            inner = SubscriptionStarted.with_tenant_context(
                aggregate_id=subscription_id, plan="solo", seats=1
            )
            print("innermost scope wins:", inner.tenant_id == tenant_b)
```

```console
innermost scope wins: True
```

There is no capture at class-definition time and no caching. Two events built from the
same class at two points in a request can carry two different tenants, if the scopes
around them differ.

### Forgetting the scope

Move the construction outside every `async with` and the strict accessor fires. Add this
at the end of `build_events()`, dedented out of the tenant A block:

```python
    try:
        SubscriptionStarted.with_tenant_context(
            aggregate_id=subscription_id, plan="team", seats=5
        )
    except TenantContextNotSetError as exc:
        print("no scope:", type(exc).__name__)

    outside = SubscriptionStarted.with_tenant_context(
        aggregate_id=subscription_id, plan="team", seats=5, tenant_id=tenant_a
    )
    print("explicit works with no scope:", outside.tenant_id == tenant_a)
```

```console
no scope: TenantContextNotSetError
explicit works with no scope: True
```

The second call proves the failure is about *inference*, not about the scope being
mandatory. `with_tenant_context()` needs a tenant from somewhere; context is where it
looks first, and the explicit argument is the escape hatch.

### The aggregate does this for you

You will rarely call `with_tenant_context()` directly once the aggregate exists, because
`AggregateRoot.create_event()` performs the same lookup as part of a larger one. It
auto-populates `aggregate_id`, `aggregate_type`, and `aggregate_version` from the
aggregate, and then:

```python
if "tenant_id" not in kwargs:
    tenant_id = self._get_tenant_from_context()
    if tenant_id is not None:
        event_kwargs["tenant_id"] = tenant_id
```

Two differences from `with_tenant_context()` are worth noticing, because they change what
failure looks like.

First, `_get_tenant_from_context()` calls the *permissive* `get_current_tenant()`
directly, returning `None` if no scope is currently set. `create_event()` is on the base
`AggregateRoot`, shared by tenant and non-tenant aggregates alike, so it cannot demand a
tenant the way the tenant-specific classmethod can.

Second, when that lookup yields `None` the key is simply omitted -- and for a
`TenantDomainEvent` subclass Pydantic then rejects the construction:

```console
create_event outside scope -> missing ('tenant_id',)
```

So the protection holds either way; you just get a `pydantic.ValidationError` with
`loc=('tenant_id',)` instead of a `TenantContextNotSetError`. If you want the clearer
error inside an aggregate command, call `get_required_tenant()` yourself at the top of
the method.

With that, event construction is safe as long as the caller is honest and inside a scope.
Step 5 builds the `Subscription` aggregate around `create_event()`, and Step 6 adds the
wrapper that stops handling honesty as an assumption.

## Step 5: Build the Tenant-Aware Aggregate and Its @handles Methods

Now the aggregate. The important thing to notice as you write it is how little of it is
about tenancy: there is no `tenant_id` parameter on any command method, no tenant field
on the state model, and no tenant check in any handler. The aggregate is an ordinary
`DeclarativeAggregate` -- the tenant arrives through `create_event()` reading the same
`ContextVar` you set in Step 3.

Add the state model and the aggregate to `multi_tenancy.py`, below the events:

```python
from pydantic import BaseModel

from eventsource import DeclarativeAggregate, handles


class SubscriptionState(BaseModel):
    plan: str
    seats: int


class Subscription(DeclarativeAggregate[SubscriptionState]):
    aggregate_type = "Subscription"
    requires_creation_event = True

    # --- commands ---

    def start(self, plan: str, seats: int) -> None:
        if self.is_created:
            raise ValueError("Subscription already started")
        self.create_event(SubscriptionStarted, plan=plan, seats=seats)

    def change_plan(self, new_plan: str) -> None:
        if new_plan == self.state.plan:
            return
        self.create_event(PlanChanged, old_plan=self.state.plan, new_plan=new_plan)

    # --- handlers ---

    @handles(SubscriptionStarted)
    def _on_started(self, event: SubscriptionStarted) -> None:
        self._state = SubscriptionState(plan=event.plan, seats=event.seats)

    @handles(PlanChanged)
    def _on_plan_changed(self, event: PlanChanged) -> None:
        self._state = self.state.model_copy(update={"plan": event.new_plan})
```

`requires_creation_event = True` means the aggregate has no `_get_initial_state()`; the
first event creates the state, and touching `.state` before then raises
`AggregateNotCreatedError`. `is_created` and `state_or_none` are the non-raising ways to
ask. That is why `start()` guards on `is_created` rather than on `self.state is None`.

The class-level `aggregate_type = "Subscription"` on the aggregate is a plain string
attribute, distinct from the `aggregate_type: str = "Subscription"` *Pydantic field* you
declared on the events in Step 2. `create_event()` passes the aggregate's value into the
event, so keeping the two spellings identical avoids a confusing mismatch in stored
payloads.

### What `create_event()` fills in

Each command method passes only its own payload. `create_event()` supplies the rest:

```python
event_kwargs: dict[str, Any] = {
    "aggregate_id": self.aggregate_id,
    "aggregate_type": self.aggregate_type,
    "aggregate_version": self.get_next_version(),
}

if "tenant_id" not in kwargs:
    tenant_id = self._get_tenant_from_context()
    if tenant_id is not None:
        event_kwargs["tenant_id"] = tenant_id

event_kwargs.update(kwargs)

event = event_class(**event_kwargs)
self.apply_event(event, is_new=True)
```

Four fields for free, then `apply_event(..., is_new=True)`, which validates the version,
advances `self._version`, dispatches to the matching `@handles` method, and appends the
event to `uncommitted_events`. Your kwargs are applied *last*, so an explicit
`tenant_id=` in a command method would override the context -- the same escape hatch
`with_tenant_context()` has, and the same hazard.

The `@handles` methods do the opposite job: they mutate state and nothing else. No
validation, no event creation, no tenant logic. They run both for new events and for
events replayed from the store during `load()`, so anything with a side effect in there
would re-fire on every rehydration. Note also that `_on_plan_changed` uses
`model_copy(update=...)` rather than assigning to `self._state.plan` -- keeping state
replacement rather than mutation makes snapshot serialization predictable.

Replace `build_events()` with a run that exercises both commands inside a scope:

```python
async def use_aggregate() -> None:
    tenant_a = uuid4()
    subscription_id = uuid4()

    async with tenant_scope(tenant_a):
        sub = Subscription(subscription_id)
        sub.start(plan="team", seats=5)
        sub.change_plan("enterprise")

        print("version:", sub.version)
        print("state:", sub.state.plan, sub.state.seats)
        print("uncommitted:", len(sub.uncommitted_events))
        print(
            "all tenant_a:",
            all(e.tenant_id == tenant_a for e in sub.uncommitted_events),
        )
        print("versions:", [e.aggregate_version for e in sub.uncommitted_events])


if __name__ == "__main__":
    asyncio.run(use_aggregate())
```

```console
$ python multi_tenancy.py
version: 2
state: enterprise 5
uncommitted: 2
all tenant_a: True
versions: [1, 2]
```

Two events, both stamped with tenant A, versions 1 and 2, and not a single mention of
`tenant_a` inside the aggregate. That is the payoff from Steps 3 and 4 arriving: the
tenant became a property of the execution context, so the domain model stayed clean.

### Forgetting the scope, again

Construct the aggregate outside any `tenant_scope()` and the failure lands at the same
place Step 4 described -- inside `create_event()`, when Pydantic finds no `tenant_id`:

```python
    outside = Subscription(uuid4())
    try:
        outside.start(plan="solo", seats=1)
    except ValidationError as exc:
        print("outside scope ->", exc.errors()[0]["type"], exc.errors()[0]["loc"])
    print("no partial state:", outside.is_created, outside.has_uncommitted_events)
```

```console
outside scope -> missing ('tenant_id',)
no partial state: False False
```

Worth dwelling on the second line. `create_event()` builds the event *before* calling
`apply_event()`, so when construction fails the aggregate is untouched -- no state, no
uncommitted event, version still 0. The command aborted cleanly rather than leaving a
half-applied aggregate behind.

The error itself is still a `pydantic.ValidationError` rather than the more descriptive
`TenantContextNotSetError`, because `create_event()` lives on the base `AggregateRoot`
and uses the permissive `get_current_tenant()` (Step 4). If you would rather fail loudly
and specifically at the top of a command, call the strict accessor yourself:

```python
    def start(self, plan: str, seats: int) -> None:
        get_required_tenant()  # raises TenantContextNotSetError with a clear message
        if self.is_created:
            raise ValueError("Subscription already started")
        self.create_event(SubscriptionStarted, plan=plan, seats=seats)
```

That is a style choice, not a safety one -- both paths refuse to produce an event.

What none of this covers is the dishonest caller: an event constructed with an explicit
foreign `tenant_id`, or built under one scope and saved under another. The aggregate has
no opinion about that, and neither does Pydantic. Step 6 adds `TenantAwareRepository`,
which inspects `uncommitted_events` at the moment of saving and is the only component in
the stack positioned to catch it.
