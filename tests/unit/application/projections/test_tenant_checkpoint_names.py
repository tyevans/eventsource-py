"""Per-tenant projection instances need distinct checkpoint names (ADR-0114).

Instances that share a name share one checkpoint row. Public APIs only.
"""

from uuid import UUID, uuid4

import pytest

from eventsource import DomainEvent, InMemoryCheckpointRepository, InMemoryDLQRepository, handles
from eventsource.application.projections import DeclarativeProjection
from eventsource.application.projections.retry import ExponentialBackoffRetryPolicy
from eventsource.application.subscriptions.retry import RetryConfig


class OrderPlaced(DomainEvent):
    """Order placed for one tenant."""

    aggregate_type: str = "Order"
    tenant_id: UUID


class OrderSummary(DeclarativeProjection):
    """Tenant-filtered projection that records the events it handled."""

    def __init__(
        self,
        tenant: UUID,
        checkpoint_repo: InMemoryCheckpointRepository,
        projection_name: str | None = None,
        dlq_repo: InMemoryDLQRepository | None = None,
        retry_policy: ExponentialBackoffRetryPolicy | None = None,
    ) -> None:
        super().__init__(
            checkpoint_repo=checkpoint_repo,
            dlq_repo=dlq_repo,
            retry_policy=retry_policy,
            tenant_filter=tenant,
            projection_name=projection_name,
        )
        self.handled: list[UUID] = []

    @handles(OrderPlaced)
    async def _on_placed(self, event: OrderPlaced) -> None:
        self.handled.append(event.event_id)


class FailingOrderSummary(OrderSummary):
    """Fails every event, so the event is dead-lettered under the instance name."""

    @handles(OrderPlaced)
    async def _on_placed(self, event: OrderPlaced) -> None:
        raise RuntimeError("cannot apply order")


def _placed(tenant: UUID) -> OrderPlaced:
    return OrderPlaced(aggregate_id=uuid4(), tenant_id=tenant)


async def _deliver(projections: list[OrderSummary], events: list[DomainEvent]) -> None:
    """Fan every event out to every instance, as a subscription runner would."""
    for event in events:
        for projection in projections:
            await projection.handle(event)


@pytest.fixture
def tenant_a() -> UUID:
    return UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")


@pytest.fixture
def tenant_b() -> UUID:
    return UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")


class TestPerTenantNames:
    """Each per-tenant instance with its own name keeps its own checkpoint.

    A checkpoint advances over every event an instance is handed, including
    events of other tenants that the filter skips. The tests therefore hand
    each instance a different prefix of the feed, as a lagging instance would.
    """

    async def test_separate_names_keep_separate_checkpoints(
        self, tenant_a: UUID, tenant_b: UUID
    ) -> None:
        repo = InMemoryCheckpointRepository()
        a = OrderSummary(tenant_a, repo, projection_name=f"OrderSummary:{tenant_a}")
        b = OrderSummary(tenant_b, repo, projection_name=f"OrderSummary:{tenant_b}")
        a1, b1, a2 = _placed(tenant_a), _placed(tenant_b), _placed(tenant_a)

        await _deliver([a], [a1, b1, a2])
        await _deliver([b], [a1, b1])

        assert a.handled == [a1.event_id, a2.event_id]
        assert b.handled == [b1.event_id]
        assert await a.get_checkpoint() == str(a2.event_id)
        assert await b.get_checkpoint() == str(b1.event_id)

    async def test_restart_with_same_names_resumes_each_tenant(
        self, tenant_a: UUID, tenant_b: UUID
    ) -> None:
        repo = InMemoryCheckpointRepository()
        name_a, name_b = f"OrderSummary:{tenant_a}", f"OrderSummary:{tenant_b}"
        a1, b1, a2, b2, a3 = (
            _placed(tenant_a),
            _placed(tenant_b),
            _placed(tenant_a),
            _placed(tenant_b),
            _placed(tenant_a),
        )
        await _deliver([OrderSummary(tenant_a, repo, projection_name=name_a)], [a1, b1, a2])
        await _deliver([OrderSummary(tenant_b, repo, projection_name=name_b)], [a1, b1])

        # Restart: new instances, same names, same checkpoint repository.
        a_new = OrderSummary(tenant_a, repo, projection_name=name_a)
        b_new = OrderSummary(tenant_b, repo, projection_name=name_b)
        assert await a_new.get_checkpoint() == str(a2.event_id)
        assert await b_new.get_checkpoint() == str(b1.event_id)

        # B catches up on what it missed, then both receive new events.
        await _deliver([b_new], [a2, b2])
        await _deliver([a_new, b_new], [a3])

        assert a_new.handled == [a3.event_id]
        assert b_new.handled == [b2.event_id]
        assert await a_new.get_checkpoint() == str(a3.event_id)
        assert await b_new.get_checkpoint() == str(a3.event_id)

    async def test_dead_letter_entries_carry_the_instance_name(self, tenant_a: UUID) -> None:
        repo = InMemoryCheckpointRepository()
        dlq = InMemoryDLQRepository(enable_tracing=False)
        name = f"OrderSummary:{tenant_a}"
        no_retry = ExponentialBackoffRetryPolicy(RetryConfig(max_retries=0))
        failing = FailingOrderSummary(
            tenant_a, repo, projection_name=name, dlq_repo=dlq, retry_policy=no_retry
        )

        with pytest.raises(RuntimeError):
            await failing.handle(_placed(tenant_a))

        entries = await dlq.get_failed_events(projection_name=name)
        assert len(entries) == 1
        assert await dlq.get_failed_events(projection_name="OrderSummary") == []


class TestDefaultNameHazard:
    """Two instances with the default name share one checkpoint row."""

    async def test_shared_default_name_shares_one_checkpoint(
        self, tenant_a: UUID, tenant_b: UUID
    ) -> None:
        repo = InMemoryCheckpointRepository()
        a = OrderSummary(tenant_a, repo)
        b = OrderSummary(tenant_b, repo)
        a1, b1 = _placed(tenant_a), _placed(tenant_b)

        await _deliver([a, b], [a1, b1])

        assert a.handled == [a1.event_id]
        assert b.handled == [b1.event_id]
        # Instance B advanced the single row past A's event, so A's
        # resume point is ahead of the events A has handled.
        assert await a.get_checkpoint() == str(b1.event_id)
        assert await b.get_checkpoint() == str(b1.event_id)

    async def test_skipped_event_still_advances_the_shared_row(
        self, tenant_a: UUID, tenant_b: UUID
    ) -> None:
        repo = InMemoryCheckpointRepository()
        a = OrderSummary(tenant_a, repo)
        b = OrderSummary(tenant_b, repo)
        a1 = _placed(tenant_a)

        await b.handle(a1)

        assert b.handled == []
        assert await b.get_checkpoint() == str(a1.event_id)
        assert await a.get_checkpoint() == str(a1.event_id)


class TestProjectionNameValidation:
    """The optional name is validated; the default is unchanged."""

    def test_default_is_the_class_name(self, tenant_a: UUID) -> None:
        projection = OrderSummary(tenant_a, InMemoryCheckpointRepository())
        assert projection.projection_name == "OrderSummary"

    @pytest.mark.parametrize("name", ["", "   ", "a\nb", "a\x00b", "x" * 256])
    def test_invalid_name_is_rejected(self, tenant_a: UUID, name: str) -> None:
        with pytest.raises(ValueError):
            OrderSummary(tenant_a, InMemoryCheckpointRepository(), projection_name=name)

    def test_non_string_name_is_rejected(self, tenant_a: UUID) -> None:
        with pytest.raises(TypeError):
            OrderSummary(tenant_a, InMemoryCheckpointRepository(), projection_name=42)  # type: ignore[arg-type]

    def test_name_at_column_limit_is_accepted(self, tenant_a: UUID) -> None:
        name = "x" * 255
        projection = OrderSummary(tenant_a, InMemoryCheckpointRepository(), projection_name=name)
        assert projection.projection_name == name
