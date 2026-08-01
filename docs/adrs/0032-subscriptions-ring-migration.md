# 0032. Subscriptions Ring Migration

The largest remaining pre-ring package -- `subscriptions/`, 20 modules and
roughly 13.7k lines -- moves onto the `domain`/`ports`/`adapters`/`application`
split ADR 0019, ADR 0024, ADR 0026, ADR 0029, and ADR 0030 already applied to
every other pre-ring package. Seventeen modules move verbatim to
`application/subscriptions/`; the subscriber and leader-election contracts
split out into `ports/subscribers.py` and `ports/coordination.py`; the only
concrete leader elector moves to `adapters/memory/coordination.py`; and the
package's exceptions merge into `domain/exceptions.py`. Clean break, no
shims -- the standing rule ADR 0025, ADR 0026, and ADR 0030 already
established for every prior retirement.

## Status

**Amended by [ADR 0041](0041-infrastructure-exceptions-to-ports.md).**
This ADR's merge of `SubscriptionError` and its six subclasses into
`domain/exceptions.py`, and the rebase onto `EventSourceError`, both
stand — neither is retro-edited. ADR 0041 relocates the same classes from
`domain/exceptions.py` to a new `ports/exceptions.py`, alongside the lock
and checkpoint exceptions, because they describe port-contract failures
rather than domain concepts. The rebase this ADR performed is unaffected;
only the module changes again.

**Accepted.** Implemented in `src/eventsource/application/subscriptions/`
(`manager.py`, `lifecycle.py`, `registry.py`, `pause_resume.py`,
`health_provider.py`, `health.py`, `shutdown.py`, `metrics.py`,
`transition.py`, `subscription.py`, `config.py`, `filtering.py`,
`flow_control.py`, `retry.py`, `error_handling.py`, `coordination.py`,
`subscriber.py`, `runners/{catchup,live}.py`), `src/eventsource/ports/subscribers.py`,
`src/eventsource/ports/coordination.py`, `src/eventsource/adapters/memory/coordination.py`,
and the subscription exception hierarchy appended to
`src/eventsource/domain/exceptions.py`. `src/eventsource/subscriptions/` no
longer exists; `import eventsource.subscriptions` now raises
`ModuleNotFoundError`.

**Amends [ADR 0009](0009-multi-instance-subscription-coordination.md).**
Location-only: ADR 0009's Decision does not change and is not retro-edited.
`LeaderElector`/`LeaderElectorWithLease` are still a transport-agnostic
Protocol pair the deployment implements against its own consensus mechanism;
`InMemoryLeaderElector`/`SharedLeaderState` are still the only bundled,
single-process implementation; the reserved bus topics, message contracts,
and `WorkRedistributionCoordinator` are unchanged in shape and behavior. What
moves is where the surface lives: the Protocol pair is now
`ports/coordination.py`, the in-memory implementation is now
`adapters/memory/coordination.py`, and the message types plus
`WorkRedistributionCoordinator` are now `application/subscriptions/coordination.py`
-- a three-way split of what ADR 0009 described as one module,
`src/eventsource/subscriptions/coordination.py`. ADR 0009's `**Status:**` line
carries an "Amended by ADR 0032" pointer; its body is untouched.

### ADR Impact

| ADR | Status |
| --- | --- |
| [0009](0009-multi-instance-subscription-coordination.md) | Amended (location only -- see above) |
| [0019](0019-clean-architecture-store-ports.md) | Stands -- this ADR applies the same ports/adapters split, not a change to it |
| [0020](0020-broker-backend-collaborator-decomposition.md) | Stands -- unrelated to the bus dependency this ADR removes |
| [0024](0024-projection-persistence-ports.md) | Stands -- sibling pattern (checkpoint/DLQ ports), not touched |
| [0029](0029-locks-readmodels-and-engine-rings.md) | Sibling, not amended -- same split shape applied to a different package |
| [0030](0030-top-level-module-ring-consolidation.md) | Sibling, not amended -- same no-shim policy applied to a different package |

No other prior ADR is amended. ADR 0009 is the only one naming
`subscriptions/`'s file paths as part of its Decision.

## Context

`subscriptions/` was the last large pre-ring package: `docs/core-surface.md`
and `.claude/rules/architecture.md`'s ring map both carried it on the
"during transition" list for `application/`, alongside `migration/` and
`handlers/`. Structurally it did not need the Protocol/implementation split
ADR 0019, ADR 0024, ADR 0026, and ADR 0029 applied elsewhere -- most of the
package (manager, runners, retry, health, flow control) is use-case
orchestration with no driver, wire, or storage contact of its own. Two things
did need splitting: `subscriber.py`, which mixed three Protocols with three
user-subclassable base implementations in one file, and `coordination.py`,
which mixed a Protocol pair, one concrete in-memory implementation, and
transport-agnostic message/coordinator types in one file -- exactly the shape
ADR 0009 itself described as "self-contained."

The package's only outward dependency was the `EventBus` interface, and only
through `TYPE_CHECKING`-only imports in four modules (`manager.py`,
`transition.py`, `lifecycle.py`, `runners/live.py`); only `runners/live.py`
called it, and only two methods (`subscribe`, `unsubscribe`). The
"application ring must not import adapters" contract in `pyproject.toml`
forbids `eventsource.application -> eventsource.bus`, and no contract clause
excepted `TYPE_CHECKING`-only imports, so moving the package as-is into
`application/` would have broken the boundary the moment import-linter's
contracts covered the new location.

`subscriptions/exceptions.py` held eight classes rooted at a bare
`SubscriptionError(Exception)`, the same shape `LockAcquisitionError` and
`LockNotHeldError` had before ADR 0029 rebased them onto `EventSourceError`.

## Decision

### 1. Seventeen modules move verbatim to `application/subscriptions/`

`manager.py`, `lifecycle.py`, `registry.py`, `pause_resume.py`,
`health_provider.py`, `health.py`, `shutdown.py`, `metrics.py`,
`transition.py`, `subscription.py`, `config.py`, `filtering.py`,
`flow_control.py`, `retry.py`, `error_handling.py`, and `runners/{catchup,live}.py`
carry over unchanged except for import-path updates. `retry.py`'s
`RetryError`/`CircuitBreakerOpenError` stay colocated in the module rather
than moving to a central exceptions file -- the same "utility-scoped
exception" pattern ADR 0029 recorded for read-model exceptions, not the
central-hierarchy pattern used for `SubscriptionError`. `application/projections/retry.py`
and `application/projections/base.py`, which already imported
`subscriptions.retry`, become intra-ring imports after the move.

### 2. `subscriber.py` splits along the Protocol/implementation line

`Subscriber`, `SyncSubscriber`, `BatchSubscriber` (Protocols) and
`supports_batch_handling()`, `get_subscribed_event_types()` (helper
functions) move to `ports/subscribers.py` -- the same "helper function
colocated with the Protocol it serves" precedent as `ports/outbox.outbox_event_data`
and `ports/locks.migration_lock_key`. `BaseSubscriber`, `BatchAwareSubscriber`,
`FilteringSubscriber` -- concrete, user-subclassable base classes with real
implementation -- stay in `application/subscriptions/subscriber.py`, since
ports carry contracts, never behavior.

### 3. `coordination.py` splits three ways

`LeaderElector`, `LeaderElectorWithLease`, and the `LeaderChangeCallback`
alias move to `ports/coordination.py`. `InMemoryLeaderElector` and
`SharedLeaderState` -- the only concrete implementation, and the only piece
of the original module that touches nothing but in-process state -- move to
`adapters/memory/coordination.py`, alongside the sibling in-memory test
doubles (`InMemoryLockManager`, `InMemoryCheckpointRepository`). The topic
constants, `ShutdownIntent`, the three message types
(`ShutdownNotification`, `HeartbeatMessage`, `WorkAssignment`), `PeerInfo`,
the remaining callback aliases, and `WorkRedistributionCoordinator` stay in
`application/subscriptions/coordination.py`: they are transport-agnostic
orchestration, not the `LeaderElector` port's payloads -- the port's own
signatures use only `bool`/`str`/callback types, never these message types.

### 4. `SubscribableEventBus` and `EventHandlerFunc` relocate to dissolve the bus dependency

Rather than excepting the four `TYPE_CHECKING`-only `EventBus` imports from
import-linter's application/adapters contract, a two-method Protocol,
`SubscribableEventBus`, is added to the existing `ports/bus.py`:

```python
class SubscribableEventBus(Protocol):
    def subscribe(self, event_type: type[DomainEvent],
                  handler: FlexibleEventHandler | EventHandlerFunc) -> None: ...
    def unsubscribe(self, event_type: type[DomainEvent],
                    handler: FlexibleEventHandler | EventHandlerFunc) -> bool: ...
```

`EventBus` satisfies it structurally with the signatures it already had.
This is an Interface Segregation Principle application, not a special case:
the runners' entire contract with the bus is two methods, so the port names
exactly that, and the dependency that would have required a contract
exception disappears instead of being excused. This decision was taken while
`EventBus` still lived in the pre-ring `bus/interface.py`; ADR 0031's bus
ring split landed first and moved `EventBus` (and the `EventHandlerFunc`
type alias `SubscribableEventBus` depends on) into the same `ports/bus.py`,
so both ports now share a module. `SubscribableEventBus` is retained
regardless: consumers type-hint the narrowest port they use, and the
runners' contract remains two methods, not the full `EventBus` ABC. The
top-level barrel's public surface is unaffected: `__all__` is
byte-identical.

### 5. Exceptions merge into `domain/exceptions.py`, `SubscriptionError` rebased

All eight classes move into `domain/exceptions.py` with no name collisions
against the existing hierarchy. `SubscriptionError` rebases from a bare
`Exception` onto `EventSourceError` -- a widening-only change, the same
pattern ADR 0029 applied to `LockAcquisitionError`/`LockNotHeldError`: every
existing `except SubscriptionError` still catches, and `except EventSourceError`
newly catches subscription failures it did not before. The repository's sole
`except SubscriptionError` (`tests/unit/test_subscription_config.py`) is
unaffected by the widening, and `ErrorClassifier` does not key on
`EventSourceError`, so no classification behavior changes.

**Recorded exception (a):** `retry.py`'s `RetryError` and
`CircuitBreakerOpenError` are not moved to `domain/exceptions.py` alongside
`SubscriptionError` -- they stay colocated in the retry module, matching the
utility-scoped pattern already used elsewhere in the library (a second,
unrelated `CircuitBreakerOpenError` already exists in
`eventsource.migration.exceptions`; colocation, not centralization, is what
keeps the two distinguishable by import path).

**Recorded exception (b):** the `SubscriptionError` rebase is the migration's
one semantic change. Every other move in this ADR is import-path-only.

### 6. No shims, no deprecation window

`src/eventsource/subscriptions/` is deleted outright; `import eventsource.subscriptions`
raises `ModuleNotFoundError` with no transition period. The library has no
external consumers yet -- every importer of the old path lives inside this
repository's own `src/` and `tests/` -- so a shim would buy nothing beyond
`__getattr__`/`__dir__` plumbing to write and maintain, the same reasoning
ADR 0025, ADR 0026, and ADR 0030 already applied to every prior retirement.

### 7. The top-level barrel is unchanged

`src/eventsource/__init__.py`'s `__all__` stays byte-identical; no
subscription name is added to it. **Rejected alternative:** export the
subscription surface from the top-level barrel, matching how most other
subsystems are reachable from `eventsource` directly. Rejected because the
barrel already exports `SubscriptionRegistry` from `eventsource.bus.registry`
-- a pre-existing, unrelated class that collides by name with
`subscriptions/registry.py`'s `SubscriptionRegistry`. This collision predates
this ADR and is not resolved by it; it is noted here as a pre-existing
condition worth a `BACKLOG.md` entry if barrel exports are ever revisited.

## Alternatives Considered

**Except the `TYPE_CHECKING`-only bus imports from the application/adapters
contract, or set `exclude_type_checking_imports`.** Rejected: the Dependency
Rule as written in `.claude/rules/architecture.md` is unconditional --
"no names, no types, no imports" -- and a contract exception would be a
special case for this one package rather than a structural fix. The
two-method port (Decision §4) removes the dependency instead of excusing it.

**Move the whole of `subscriber.py` into `ports/`.** Rejected: ports carry
Protocols and pure helper functions, never behavior. `BaseSubscriber`,
`BatchAwareSubscriber`, and `FilteringSubscriber` have real method bodies,
matching the precedent that `DeclarativeProjection` lives in `application/`
even though it implements a `ports/`-defined contract.

**Leave `coordination.py` unsplit, as a single application-ring module.**
Rejected: it would have meant either duplicating the in-memory elector as a
second implementation the application ring owns (against the ports/adapters
pattern ADR 0029 already established for locks), or leaving the concrete
`InMemoryLeaderElector` in a module application code imports for its
Protocol -- the same anti-pattern that motivated splitting `locks/` and
`readmodels/` in the first place. The coordination module's own docstring
already named Kubernetes, Redis, and Consul as future adapter implementations,
which only makes sense if the port and the implementation are different
modules.

**Give `retry.py`'s exceptions a shared `application/retry.py` home instead
of colocating them with the retry logic.** Rejected: no consumer needs a
central retry-exceptions module: `application/projections/retry.py` already
imports `subscriptions.retry` today (an intra-package import once the
package itself is renamed), and introducing a new shared module would add an
indirection with no second caller to justify it.

## Consequences

### Positive

- The ring-2 "during transition" list in `.claude/rules/architecture.md`
  shrinks to `migration/` and `handlers/` -- the last two pre-ring packages.
- Tier 0 (stdlib + pydantic only) grows to include `ports/subscribers.py`
  and `ports/coordination.py`.
- The application-ring-to-bus dependency is gone, not excepted: no contract
  clause anywhere carries an exemption for `subscriptions/`.
- `subscriber.py` and `coordination.py` each stop mixing a Protocol with its
  implementation in one file, matching every other port/adapter pair in the
  library.

### Negative

- Every import of the package's ~90-name public surface changes its source
  path. No external code exists to be broken by this, but every internal
  caller (`application/projections/`, `migration/`, tests, docs, examples)
  needed the sweep this ADR's implementation carried out.
- `except EventSourceError` newly catches subscription failures that a
  boundary handler written against the old hierarchy would not have
  expected. Widening-only, per Decision §5, but still a behavior change for
  any `except EventSourceError` clause that assumed subscription errors
  could never arrive there.
- `InMemoryLeaderElector`/`SharedLeaderState` import-path churn: code that
  imported them from `eventsource.subscriptions` (or, briefly, from
  `application.subscriptions`) must now import from `eventsource.adapters.memory`
  -- the one name-availability break beyond the package path itself, since
  `application/` cannot re-export a name it is not permitted to import.

## References

- `src/eventsource/application/subscriptions/` -- the relocated package
- `src/eventsource/ports/subscribers.py`, `src/eventsource/ports/coordination.py`,
  `src/eventsource/ports/bus.py` (`SubscribableEventBus`), `src/eventsource/ports/handlers.py`
  (`EventHandlerFunc`)
- `src/eventsource/adapters/memory/coordination.py`
- `src/eventsource/domain/exceptions.py` -- the merged `SubscriptionError` hierarchy
- [ADR 0009](0009-multi-instance-subscription-coordination.md) -- the
  coordination Decision this ADR relocates without changing
- [ADR 0029](0029-locks-readmodels-and-engine-rings.md) -- the
  Protocol/implementation split precedent and the exception-rebasing pattern
  this ADR follows
- [ADR 0030](0030-top-level-module-ring-consolidation.md) -- the no-shim
  standing rule this ADR applies

## Related

- `docs/guides/subscriptions.md`, `docs/guides/subscription-coordination.md`
  -- user-facing guides updated for the new import paths
- `docs/api/subscriptions.md`, `docs/api/exceptions.md` -- API reference
  updated for the new module layout and exception hierarchy
- `.claude/rules/architecture.md` -- ring map updated to mark
  `application/subscriptions/`, `ports/subscribers.py`, `ports/coordination.py`,
  `SubscribableEventBus`, and `adapters/memory/coordination.py` as settled
