"""Unit tests for leadership lease renewal verification before cluster operations."""

from typing import Any

import pytest

from eventsource.adapters.memory import (
    InMemoryCheckpointRepository,
    InMemoryEventBus,
    InMemoryEventStore,
    InMemoryLeaderElector,
)
from eventsource.application.subscriptions import (
    Subscription,
    SubscriptionConfig,
    SubscriptionState,
    verify_leadership_lease,
)
from eventsource.application.subscriptions.coordination import WorkRedistributionCoordinator
from eventsource.application.subscriptions.lifecycle import SubscriptionLifecycleManager
from eventsource.domain.event import DomainEvent
from eventsource.domain.event_registry import EventRegistry, register_event
from eventsource.ports.exceptions import TransitionError

_REGISTRY = EventRegistry()


@register_event(registry=_REGISTRY)
class LeaseTestEvent(DomainEvent):
    aggregate_type: str = "LeaseAggregate"
    data: str = "payload"


class DummySubscriber:
    def __init__(self) -> None:
        self.handled_events: list[DomainEvent] = []

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [LeaseTestEvent]

    async def handle(self, event: DomainEvent) -> None:
        self.handled_events.append(event)


class ExpiringLeaseLeaderElector(InMemoryLeaderElector):
    def __init__(
        self,
        identity: str,
        *,
        initial_leader: bool = True,
        expired: bool = False,
        error: Exception | None = None,
    ) -> None:
        super().__init__(_identity=identity, _is_leader=False if expired else initial_leader)
        self._renew_succeeds = not expired
        self._raise_on_renew = error

    async def renew(self) -> bool:
        if self._raise_on_renew is not None:
            raise self._raise_on_renew
        if not self._renew_succeeds:
            self._is_leader = False
            return False
        return self._is_leader


def _make_lifecycle(leader_elector: Any = None) -> SubscriptionLifecycleManager:
    return SubscriptionLifecycleManager(
        InMemoryEventStore(event_registry=_REGISTRY),
        InMemoryEventBus(),
        InMemoryCheckpointRepository(),
        leader_elector=leader_elector,
    )


@pytest.fixture
def sub() -> Subscription:
    cfg = SubscriptionConfig(start_from="beginning")
    return Subscription(name="test-projection", subscriber=DummySubscriber(), config=cfg)


class TestSubscriptionLifecycleManagerLeadership:
    async def test_uncoordinated_lifecycle_succeeds(self, sub: Subscription) -> None:
        lifecycle = _make_lifecycle(leader_elector=None)
        assert await lifecycle.verify_leadership_lease() is True
        await lifecycle.start_subscription(sub)
        assert sub.state == SubscriptionState.LIVE
        await lifecycle.stop_all([sub])

    async def test_coordinated_start_with_active_leader_lease_succeeds(
        self, sub: Subscription
    ) -> None:
        elector = InMemoryLeaderElector("instance-alpha")
        await elector.try_acquire()
        lifecycle = _make_lifecycle(leader_elector=elector)
        await lifecycle.start_subscription(sub)
        assert sub.state == SubscriptionState.LIVE
        await lifecycle.stop_all([sub])

    async def test_coordinated_start_blocks_when_not_leader(self, sub: Subscription) -> None:
        elector = ExpiringLeaseLeaderElector("follower-node", initial_leader=False)
        with pytest.raises(TransitionError) as exc_info:
            await _make_lifecycle(leader_elector=elector).start_subscription(sub)
        assert "unauthorized cluster transition blocked" in str(exc_info.value)
        assert "follower-node" in str(exc_info.value)
        assert sub.state == SubscriptionState.ERROR

    async def test_coordinated_start_blocks_when_lease_renewal_fails(
        self, sub: Subscription
    ) -> None:
        elector = ExpiringLeaseLeaderElector("leader-expired", expired=True)
        with pytest.raises(TransitionError, match="unauthorized cluster transition blocked"):
            await _make_lifecycle(leader_elector=elector).start_subscription(sub)
        assert sub.state == SubscriptionState.ERROR

    async def test_coordinated_start_blocks_after_force_lose_leadership(
        self, sub: Subscription
    ) -> None:
        elector = InMemoryLeaderElector("worker-1")
        await elector.try_acquire()
        await elector.force_lose_leadership()
        with pytest.raises(TransitionError):
            await _make_lifecycle(leader_elector=elector).start_subscription(sub)
        assert sub.state == SubscriptionState.ERROR

    async def test_start_all_captures_lease_expiration_error(self, sub: Subscription) -> None:
        elector = ExpiringLeaseLeaderElector("cluster-leader", expired=True)
        results = await _make_lifecycle(leader_elector=elector).start_all([sub])
        assert isinstance(results.get(sub.name), TransitionError)

    async def test_verify_leadership_lease_handles_backend_exception(
        self, sub: Subscription
    ) -> None:
        err = ConnectionError("Advisory lock lost connection")
        lifecycle = _make_lifecycle(ExpiringLeaseLeaderElector("failing-backend", error=err))
        assert await lifecycle.verify_leadership_lease() is False
        with pytest.raises(TransitionError):
            await lifecycle.start_subscription(sub)


class TestWorkRedistributionCoordinatorLeadership:
    async def test_create_work_assignment_succeeds_when_leader(self) -> None:
        elector = InMemoryLeaderElector("leader-node")
        await elector.try_acquire()
        coordinator = WorkRedistributionCoordinator("leader-node", leader_elector=elector)
        assignment = await coordinator.create_work_assignment(
            "worker-node-1", ["projection-orders"], priority=5
        )
        assert assignment.target_instance_id == "worker-node-1"
        assert assignment.subscriptions == ("projection-orders",)
        assert assignment.source_instance_id == "leader-node" and assignment.priority == 5

    async def test_create_work_assignment_uncoordinated_succeeds(self) -> None:
        coordinator = WorkRedistributionCoordinator("standalone-node", leader_elector=None)
        assignment = await coordinator.create_work_assignment("peer-1", ["projection-1"])
        assert assignment.target_instance_id == "peer-1"

    async def test_create_work_assignment_raises_when_lease_expired(self) -> None:
        elector = ExpiringLeaseLeaderElector("split-brain-leader", expired=True)
        coordinator = WorkRedistributionCoordinator("split-brain-leader", leader_elector=elector)
        with pytest.raises(TransitionError, match="cannot issue work assignment"):
            await coordinator.create_work_assignment("worker-2", ["orders"])

    async def test_initiate_leadership_handoff_verifies_renewal(self) -> None:
        elector = InMemoryLeaderElector("leader-1")
        await elector.try_acquire()
        coordinator = WorkRedistributionCoordinator("leader-1", leader_elector=elector)
        assert await coordinator.initiate_leadership_handoff() is True
        assert elector.is_leader is False

    async def test_initiate_leadership_handoff_skips_when_lease_expired(self) -> None:
        elector = ExpiringLeaseLeaderElector("leader-lost", expired=True)
        coordinator = WorkRedistributionCoordinator("leader-lost", leader_elector=elector)
        assert await coordinator.initiate_leadership_handoff() is False


class TestVerifyLeadershipLeaseHelper:
    async def test_verify_none_elector_returns_true(self) -> None:
        assert await verify_leadership_lease(None) is True

    async def test_verify_active_leader_returns_true(self) -> None:
        elector = InMemoryLeaderElector("leader-1")
        await elector.try_acquire()
        assert await verify_leadership_lease(elector) is True

    async def test_verify_non_leader_returns_false(self) -> None:
        assert await verify_leadership_lease(InMemoryLeaderElector("follower-1")) is False

    async def test_verify_expired_lease_returns_false(self) -> None:
        assert await verify_leadership_lease(ExpiringLeaseLeaderElector("n", expired=True)) is False

    async def test_verify_renewal_exception_returns_false(self) -> None:
        err = RuntimeError("Cluster partitioned")
        assert await verify_leadership_lease(ExpiringLeaseLeaderElector("n", error=err)) is False
