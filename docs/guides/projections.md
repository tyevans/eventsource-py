# Rebuilding Projections with `replay()`

`eventsource.application.projections.replay` (also re-exported from the top-level
`eventsource` package) rebuilds one or more projections by reading the event
store's global feed (`GlobalEventFeed`) and folding every matching event into
each projection in order:

- **Single-projection or multi-projection rebuilds**: pass a single
  `EventSubscriber` directly (`await replay(store, my_projection)`) or a sequence
  of projections (`await replay(store, [orders, invoices])`).
- **`ReplayReport.succeeded`**: boolean property returning `True` when the
  rebuild completed with zero failures (`len(report.failures) == 0` and
  `report.failures_truncated == 0`).
- **Storage-level filtering**: scope rebuilds with `tenant_id=...`,
  `aggregate_type=...`, and `from_position=...` pushed down to the store query via
  `FeedReadOptions`.
- **Resilient or strict error handling**: record poison events in
  `report.failures` while continuing the rebuild by default, or pass
  `strict=True` to raise `ReplayFailedError` on the first failure.

For an introductory walkthrough of projections, see
[Tutorial 6: Projections](../tutorials/06-projections.md),
[How to Work with Read Models](read-models.md), and the
[Projections API Reference](../api/projections.md).

## Rebuilding a single projection with `replay(store, projection)`

When rebuilding a single projection, pass the projection instance directly as
the second argument to `replay(store, my_projection)` without wrapping it in a
list, and check `report.succeeded`:

```python
from uuid import UUID, uuid4
from eventsource import (
    DeclarativeProjection,
    DomainEvent,
    ExpectedVersion,
    InMemoryEventStore,
    StreamId,
    handles,
    replay,
)


class AccountOpened(DomainEvent):
    aggregate_type: str = "Account"
    owner: str


class MoneyDeposited(DomainEvent):
    aggregate_type: str = "Account"
    amount: float


class AccountBalanceProjection(DeclarativeProjection):
    def __init__(self) -> None:
        super().__init__()
        self.balances: dict[UUID, float] = {}

    @handles(AccountOpened)
    async def _on_opened(self, event: AccountOpened) -> None:
        self.balances[event.aggregate_id] = 0.0

    @handles(MoneyDeposited)
    async def _on_deposited(self, event: MoneyDeposited) -> None:
        self.balances[event.aggregate_id] = (
            self.balances.get(event.aggregate_id, 0.0) + event.amount
        )


async def rebuild_single_projection() -> None:
    async with InMemoryEventStore() as store:
        account_id = uuid4()
        await store.append(
            StreamId(account_id, "Account"),
            [
                AccountOpened(
                    aggregate_id=account_id,
                    aggregate_version=1,
                    owner="alice",
                ),
                MoneyDeposited(
                    aggregate_id=account_id,
                    aggregate_version=2,
                    amount=125.0,
                ),
            ],
            ExpectedVersion.NO_STREAM,
        )

        projection = AccountBalanceProjection()

        # Pass a single projection directly without wrapping in [...]
        report = await replay(store, projection)

        assert report.succeeded is True
        assert report.applied == 2
        assert report.failed == 0
        assert projection.balances[account_id] == 125.0
```

## Rebuilding multiple projections in a single pass

When rebuilding several projections from the same global event feed, pass a
sequence of `EventSubscriber` instances (`[balance_projection, audit_projection]`)
so the feed is read once and each event is folded into every projection in order:

```python
async def rebuild_multiple_projections(
    store: InMemoryEventStore,
    balance_projection: AccountBalanceProjection,
    audit_projection: AccountBalanceProjection,
) -> None:
    report = await replay(store, [balance_projection, audit_projection])

    assert report.succeeded is True
    print(f"Applied {report.applied} events up to {report.last_position}")
```

## Inspecting rebuild outcomes with `ReplayReport` and `report.succeeded`

`replay()` returns a frozen `ReplayReport` summarizing the rebuild:

| Attribute / Property | Type | Description |
| --- | --- | --- |
| `succeeded` | `bool` | `True` when `len(failures) == 0` and `failures_truncated == 0`; `False` if any projection rejected an event. |
| `applied` | `int` | Count of events delivered that no projection rejected. |
| `failed` | `int` | Count of distinct events (`event_id`) rejected by at least one projection in `failures`. |
| `last_position` | `Position \| None` | Final global position reached during the read (`None` when the feed is empty). |
| `failures` | `tuple[ReplayFailure, ...]` | Retained `ReplayFailure` entries (capped at `max_failures`, default `1000`). |
| `failures_truncated` | `int` | Number of additional failures that occurred beyond `max_failures`. |
| `raise_for_failures()` | `() -> None` | Raises `ReplayFailedError` with the first failure if `not succeeded`, or `RuntimeError` when all failures were truncated (`max_failures=0`). |

By default, a projection raising an exception on a poison event does not halt
the rebuild: `replay()` records a `ReplayFailure(position, event_id, event_type, projection, error)`,
sets `report.succeeded` to `False`, and continues folding subsequent events:

```python
from eventsource.application.projections import ReplayFailedError, ReplayFailure


async def inspect_replay_failures(
    store: InMemoryEventStore,
    projection: AccountBalanceProjection,
) -> None:
    logged_failures: list[ReplayFailure] = []

    report = await replay(
        store,
        projection,
        on_failure=logged_failures.append,
    )

    if not report.succeeded:
        for failure in report.failures:
            print(
                f"{failure.projection} rejected {failure.event_type} "
                f"({failure.event_id}) at {failure.position}: {failure.error}"
            )
        # Or raise ReplayFailedError after inspecting all collected failures:
        report.raise_for_failures()
```

To fail fast on the first rejected event (for example in tests or initial
production deployments), pass `strict=True` to raise `ReplayFailedError`:

```python
async def strict_rebuild(
    store: InMemoryEventStore,
    projection: AccountBalanceProjection,
) -> None:
    try:
        report = await replay(store, projection, strict=True)
        report.raise_for_failures()
    except ReplayFailedError as err:
        print(f"Rebuild stopped at {err.failure.event_id}: {err.failure.error}")
        raise
```

## Scoping rebuilds by tenant, aggregate type, or starting position

Pass `tenant_id=...`, `aggregate_type=...`, or `from_position=...` to push filtering
down to the storage adapter via `FeedReadOptions` instead of scanning the entire
log in Python:

```python
from uuid import UUID
from eventsource.ports.positions import Position


async def scoped_rebuild(
    store: InMemoryEventStore,
    projection: AccountBalanceProjection,
    tenant_id: UUID,
    checkpoint: Position | None = None,
) -> None:
    report = await replay(
        store,
        projection,
        from_position=checkpoint,
        tenant_id=tenant_id,
        aggregate_type="Account",
    )
    assert report.succeeded is True
```

## Coordinating batches of events with `ProjectionCoordinator` and `ProjectionRegistry`

When you already have a batch of `DomainEvent` instances in memory and want to
dispatch or rebuild multiple projections and event handlers concurrently, pass
your projections or handlers directly to `ProjectionCoordinator([proj1, proj2])`
or register them fluently with `ProjectionRegistry(projections=[...]).register(...)`:

```python
from eventsource import ProjectionCoordinator, ProjectionRegistry


async def coordinate_projections(
    balance_projection: AccountBalanceProjection,
    events: list[DomainEvent],
) -> None:
    # Pass a sequence of projections/handlers directly:
    coordinator = ProjectionCoordinator([balance_projection])
    await coordinator.dispatch_events(events)

    # Or configure a ProjectionRegistry with max_concurrency and fluent .register():
    registry = ProjectionRegistry(max_concurrency=4).register(balance_projection)
    bounded_coordinator = ProjectionCoordinator(registry)
    await bounded_coordinator.rebuild_all(events)
```
