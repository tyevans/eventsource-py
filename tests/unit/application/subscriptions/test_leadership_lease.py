"""
Unit tests for leadership lease renewal verification before cluster operations.

Verifies:
- SubscriptionLifecycleManager re-verifies leadership lease prior to executing transitions.
- Expired or lost leadership leases block unauthorized cluster transitions with TransitionError.
- WorkRedistributionCoordinator re-verifies leadership lease prior to issuing work assignments.
- initiate_leadership_handoff re-verifies leadership lease renewal before release.
- Standalone verify_leadership_lease helper behavior.
- All verification guards execute strictly through public interfaces without private backdoor mocks.
"""

import pytest

from eventsource.adapters.memory.bus import InMemoryEventBus
from eventsource.adapters.memory.checkpoints import InMemoryCheckpointRepository
from eventsource.adapters.memory.coordination import InMemoryLeaderElector
from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.subscriptions import (
    Subscription,
    SubscriptionConfig,
    SubscriptionState,
    verify_leadership_lease,
)
from eventsource.application.subscriptions.coordination import (
    WorkRedistributionCoordinator,
)
from eventsource.application.subscriptions.lifecycle import SubscriptionLifecycleManager
from eventsource.domain.event import DomainEvent
from eventsource.domain.event_registry import EventRegistry, register_event
from eventsource.ports.coordination import LeaderChangeCallback
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


class ExpiringLeaseLeaderElector:
    """Test double implementing LeaderElector protocol to simulate lease expiration."""

    def __init__(self, identity: str, *, initial_leader: bool = True) -> None:
        self._identity = identity
        self._is_leader = initial_leader
        self._renew_succeeds = True
        self._raise_on_renew: Exception | None = None
        self._callbacks: list[LeaderChangeCallback] = []

    @property
    def identity(self) -> str:
        return self._identity

    @property
    def is_leader(self) -> bool:
        return self._is_leader

    @property
    def current_leader(self) -> str | None:
        return self._identity if self._is_leader else None

    async def try_acquire(self, timeout: float = 10.0) -> bool:
        self._is_leader = True
        return True

    async def release(self) -> None:
        self._is_leader = False

    async def renew(self) -> bool:
        if self._raise_on_renew is not None:
            raise self._raise_on_renew
        if not self._renew_succeeds:
            self._is_leader = False
            return False
        return self._is_leader

    def expire_lease(self) -> None:
        self._renew_succeeds = False
        self._is_leader = False

    def simulate_backend_error(self, exc: Exception) -> None:
        self._raise_on_renew = exc

    def on_leader_change(self, callback: LeaderChangeCallback) -> None:
        self._callbacks.append(callback)

    def remove_leader_change_callback(self, callback: LeaderChangeCallback) -> bool:
        try:
            self._callbacks.remove(callback)
            return True
        except ValueError:
            return False


@pytest.fixture
def event_store() -> InMemoryEventStore:
    return InMemoryEventStore(event_registry=_REGISTRY)


@pytest.fixture
def event_bus() -> InMemoryEventBus:
    return InMemoryEventBus()


@pytest.fixture
def checkpoint_repo() -> InMemoryCheckpointRepository:
    return InMemoryCheckpointRepository()


@pytest.fixture
def test_subscription() -> Subscription:
    return Subscription(
        name="test-projection",
        subscriber=DummySubscriber(),
        config=SubscriptionConfig(start_from="beginning"),
    )


class TestSubscriptionLifecycleManagerLeadership:
    """Verifies that leadership lease renewal guards cluster transitions in lifecycle management."""

    async def test_uncoordinated_lifecycle_succeeds(
        self,
        event_store: InMemoryEventStore,
        event_bus: InMemoryEventBus,
        checkpoint_repo: InMemoryCheckpointRepository,
        test_subscription: Subscription,
    ) -> None:
        lifecycle = SubscriptionLifecycleManager(
            event_store=event_store,
            event_bus=event_bus,
            checkpoint_repo=checkpoint_repo,
            leader_elector=None,
        )
        assert await lifecycle.verify_leadership_lease() is True
        await lifecycle.start_subscription(test_subscription)
        assert test_subscription.state == SubscriptionState.LIVE
        await lifecycle.stop_all([test_subscription])

    async def test_coordinated_start_with_active_leader_lease_succeeds(
        self,
        event_store: InMemoryEventStore,
        event_bus: InMemoryEventBus,
        checkpoint_repo: InMemoryCheckpointRepository,
        test_subscription: Subscription,
    ) -> None:
        elector = InMemoryLeaderElector("instance-alpha")
        await elector.try_acquire()

        lifecycle = SubscriptionLifecycleManager(
            event_store=event_store,
            event_bus=event_bus,
            checkpoint_repo=checkpoint_repo,
            leader_elector=elector,
        )
        await lifecycle.start_subscription(test_subscription)
        assert test_subscription.state == SubscriptionState.LIVE
        await lifecycle.stop_all([test_subscription])

    async def test_coordinated_start_blocks_when_not_leader(
        self,
        event_store: InMemoryEventStore,
        event_bus: InMemoryEventBus,
        checkpoint_repo: InMemoryCheckpointRepository,
        test_subscription: Subscription,
    ) -> None:
        elector = ExpiringLeaseLeaderElector("follower-node", initial_leader=False)
        lifecycle = SubscriptionLifecycleManager(
            event_store=event_store,
            event_bus=event_bus,
            checkpoint_repo=checkpoint_repo,
            leader_elector=elector,
        )

        with pytest.raises(TransitionError) as exc_info:
            await lifecycle.start_subscription(test_subscription)

        assert "unauthorized cluster transition blocked" in str(exc_info.value)
        assert "follower-node" in str(exc_info.value)
        assert test_subscription.state == SubscriptionState.ERROR

    async def test_coordinated_start_blocks_when_lease_renewal_fails(
        self,
        event_store: InMemoryEventStore,
        event_bus: InMemoryEventBus,
        checkpoint_repo: InMemoryCheckpointRepository,
        test_subscription: Subscription,
    ) -> None:
        elector = ExpiringLeaseLeaderElector("leader-expired", initial_leader=True)
        elector.expire_lease()

        lifecycle = SubscriptionLifecycleManager(
            event_store=event_store,
            event_bus=event_bus,
            checkpoint_repo=checkpoint_repo,
            leader_elector=elector,
        )

        with pytest.raises(TransitionError) as exc_info:
            await lifecycle.start_subscription(test_subscription)

        assert "unauthorized cluster transition blocked" in str(exc_info.value)
        assert test_subscription.state == SubscriptionState.ERROR

    async def test_coordinated_start_blocks_after_force_lose_leadership(
        self,
        event_store: InMemoryEventStore,
        event_bus: InMemoryEventBus,
        checkpoint_repo: InMemoryCheckpointRepository,
        test_subscription: Subscription,
    ) -> None:
        elector = InMemoryLeaderElector("worker-1")
        await elector.try_acquire()
        await elector.force_lose_leadership()

        lifecycle = SubscriptionLifecycleManager(
            event_store=event_store,
            event_bus=event_bus,
            checkpoint_repo=checkpoint_repo,
            leader_elector=elector,
        )

        with pytest.raises(TransitionError):
            await lifecycle.start_subscription(test_subscription)

        assert test_subscription.state == SubscriptionState.ERROR

    async def test_start_all_captures_lease_expiration_error(
        self,
        event_store: InMemoryEventStore,
        event_bus: InMemoryEventBus,
        checkpoint_repo: InMemoryCheckpointRepository,
        test_subscription: Subscription,
    ) -> None:
        elector = ExpiringLeaseLeaderElector("cluster-leader", initial_leader=True)
        elector.expire_lease()

        lifecycle = SubscriptionLifecycleManager(
            event_store=event_store,
            event_bus=event_bus,
            checkpoint_repo=checkpoint_repo,
            leader_elector=elector,
        )

        results = await lifecycle.start_all([test_subscription])
        assert test_subscription.name in results
        assert isinstance(results[test_subscription.name], TransitionError)

    async def test_verify_leadership_lease_handles_backend_exception(
        self,
        event_store: InMemoryEventStore,
        event_bus: InMemoryEventBus,
        checkpoint_repo: InMemoryCheckpointRepository,
        test_subscription: Subscription,
    ) -> None:
        elector = ExpiringLeaseLeaderElector("failing-backend", initial_leader=True)
        elector.simulate_backend_error(ConnectionError("Advisory lock lost connection to Postgres"))

        lifecycle = SubscriptionLifecycleManager(
            event_store=event_store,
            event_bus=event_bus,
            checkpoint_repo=checkpoint_repo,
            leader_elector=elector,
        )

        assert await lifecycle.verify_leadership_lease() is False
        with pytest.raises(TransitionError):
            await lifecycle.start_subscription(test_subscription)


class TestWorkRedistributionCoordinatorLeadership:
    """Verifies that leadership lease renewal guards work assignments and handoffs."""

    async def test_create_work_assignment_succeeds_when_leader(self) -> None:
        elector = InMemoryLeaderElector("leader-node")
        await elector.try_acquire()

        coordinator = WorkRedistributionCoordinator(
            instance_id="leader-node",
            leader_elector=elector,
        )

        assignment = await coordinator.create_work_assignment(
            target_instance_id="worker-node-1",
            subscriptions=["projection-orders"],
            priority=5,
        )
        assert assignment.target_instance_id == "worker-node-1"
        assert assignment.subscriptions == ("projection-orders",)
        assert assignment.source_instance_id == "leader-node"
        assert assignment.priority == 5

    async def test_create_work_assignment_uncoordinated_succeeds(self) -> None:
        coordinator = WorkRedistributionCoordinator(
            instance_id="standalone-node",
            leader_elector=None,
        )

        assignment = await coordinator.create_work_assignment(
            target_instance_id="peer-1",
            subscriptions=["projection-1"],
        )
        assert assignment.target_instance_id == "peer-1"

    async def test_create_work_assignment_raises_when_lease_expired(self) -> None:
        elector = ExpiringLeaseLeaderElector("split-brain-leader", initial_leader=True)
        elector.expire_lease()

        coordinator = WorkRedistributionCoordinator(
            instance_id="split-brain-leader",
            leader_elector=elector,
        )

        with pytest.raises(TransitionError) as exc_info:
            await coordinator.create_work_assignment(
                target_instance_id="worker-2",
                subscriptions=["orders"],
            )
        assert "cannot issue work assignment" in str(exc_info.value)

    async def test_initiate_leadership_handoff_verifies_renewal(self) -> None:
        elector = InMemoryLeaderElector("leader-1")
        await elector.try_acquire()

        coordinator = WorkRedistributionCoordinator(
            instance_id="leader-1",
            leader_elector=elector,
        )
        assert await coordinator.initiate_leadership_handoff() is True
        assert elector.is_leader is False

    async def test_initiate_leadership_handoff_skips_when_lease_expired(self) -> None:
        elector = ExpiringLeaseLeaderElector("leader-lost", initial_leader=True)
        elector.expire_lease()

        coordinator = WorkRedistributionCoordinator(
            instance_id="leader-lost",
            leader_elector=elector,
        )
        assert await coordinator.initiate_leadership_handoff() is False


class TestVerifyLeadershipLeaseHelper:
    """Verifies the standalone verify_leadership_lease function."""

    async def test_verify_none_elector_returns_true(self) -> None:
        assert await verify_leadership_lease(None) is True

    async def test_verify_active_leader_returns_true(self) -> None:
        elector = InMemoryLeaderElector("leader-1")
        await elector.try_acquire()
        assert await verify_leadership_lease(elector) is True

    async def test_verify_non_leader_returns_false(self) -> None:
        elector = InMemoryLeaderElector("follower-1")
        assert await verify_leadership_lease(elector) is False

    async def test_verify_expired_lease_returns_false(self) -> None:
        elector = ExpiringLeaseLeaderElector("node-1", initial_leader=True)
        elector.expire_lease()
        assert await verify_leadership_lease(elector) is False

    async def test_verify_renewal_exception_returns_false(self) -> None:
        elector = ExpiringLeaseLeaderElector("node-1", initial_leader=True)
        elector.simulate_backend_error(RuntimeError("Cluster partitioned"))
        assert await verify_leadership_lease(elector) is False
