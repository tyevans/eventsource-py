"""
Identity and conformance tests for the subscriptions ring migration, slice 1
(ADR 0031).

Slice 1 extracts the pure boundary interfaces out of
``eventsource.application.subscriptions`` into ``eventsource.ports`` /
``eventsource.adapters.memory`` while the ``subscriptions`` package itself
stays in place (moved to ``eventsource.application.subscriptions`` in slice
2). These tests pin down that the extracted names are genuinely the same
objects as their old re-exports, not copies, and that the new
``SubscribableEventBus`` port and the exception rebase behave as designed.
"""

from eventsource.adapters.memory.coordination import (
    InMemoryLeaderElector,
    SharedLeaderState,
)
from eventsource.bus.memory import InMemoryEventBus
from eventsource.domain.exceptions import EventSourceError, SubscriptionError
from eventsource.ports.bus import SubscribableEventBus
from eventsource.ports.coordination import (
    LeaderChangeCallback,
    LeaderElector,
    LeaderElectorWithLease,
)
from eventsource.ports.handlers import EventHandlerFunc
from eventsource.ports.subscribers import (
    BatchSubscriber,
    Subscriber,
    SyncSubscriber,
    get_subscribed_event_types,
    supports_batch_handling,
)


class TestSubscriberProtocolIdentity:
    """eventsource.application.subscriptions.subscriber re-exports the ports Protocols verbatim."""

    def test_subscriber_is_the_same_object(self) -> None:
        from eventsource.application.subscriptions.subscriber import Subscriber as ReExported

        assert ReExported is Subscriber

    def test_sync_subscriber_is_the_same_object(self) -> None:
        from eventsource.application.subscriptions.subscriber import SyncSubscriber as ReExported

        assert ReExported is SyncSubscriber

    def test_batch_subscriber_is_the_same_object(self) -> None:
        from eventsource.application.subscriptions.subscriber import BatchSubscriber as ReExported

        assert ReExported is BatchSubscriber

    def test_supports_batch_handling_is_the_same_function(self) -> None:
        from eventsource.application.subscriptions.subscriber import (
            supports_batch_handling as reexported,
        )

        assert reexported is supports_batch_handling

    def test_get_subscribed_event_types_is_the_same_function(self) -> None:
        from eventsource.application.subscriptions.subscriber import (
            get_subscribed_event_types as reexported,
        )

        assert reexported is get_subscribed_event_types

    def test_subscriber_reexported_from_ports_package(self) -> None:
        from eventsource import ports

        assert ports.Subscriber is Subscriber
        assert ports.SyncSubscriber is SyncSubscriber
        assert ports.BatchSubscriber is BatchSubscriber
        assert ports.supports_batch_handling is supports_batch_handling
        assert ports.get_subscribed_event_types is get_subscribed_event_types


class TestLeaderElectorIdentity:
    """LeaderElector / LeaderElectorWithLease / LeaderChangeCallback are a single object each."""

    def test_leader_elector_is_the_same_object(self) -> None:
        from eventsource.application.subscriptions.coordination import LeaderElector as ReExported

        assert ReExported is LeaderElector

    def test_leader_change_callback_is_the_same_object(self) -> None:
        from eventsource.application.subscriptions.coordination import (
            LeaderChangeCallback as ReExported,
        )

        assert ReExported is LeaderChangeCallback

    def test_leader_elector_with_lease_not_reexported_from_subscriptions_coordination(
        self,
    ) -> None:
        """LeaderElectorWithLease's canonical home is ports.coordination only."""
        import eventsource.application.subscriptions.coordination as coordination_module

        assert not hasattr(coordination_module, "LeaderElectorWithLease")

    def test_leader_elector_reexported_from_ports_package(self) -> None:
        from eventsource import ports

        assert ports.LeaderElector is LeaderElector
        assert ports.LeaderElectorWithLease is LeaderElectorWithLease
        assert ports.LeaderChangeCallback is LeaderChangeCallback

    def test_leader_elector_with_lease_reexported_from_subscriptions_package(self) -> None:
        from eventsource.application.subscriptions import LeaderElectorWithLease as ReExported

        assert ReExported is LeaderElectorWithLease


class TestInMemoryLeaderElectorRelocated:
    """InMemoryLeaderElector/SharedLeaderState's new public home is adapters.memory."""

    def test_reexported_from_adapters_memory_package(self) -> None:
        from eventsource.adapters import memory

        assert memory.InMemoryLeaderElector is InMemoryLeaderElector
        assert memory.SharedLeaderState is SharedLeaderState

    def test_not_reexported_from_subscriptions_package(self) -> None:
        import eventsource.application.subscriptions as subscriptions_module

        assert not hasattr(subscriptions_module, "InMemoryLeaderElector")
        assert not hasattr(subscriptions_module, "SharedLeaderState")

    async def test_implements_leader_elector_port(self) -> None:
        elector = InMemoryLeaderElector("worker-1")
        assert isinstance(elector, LeaderElector)
        assert await elector.try_acquire() is True
        assert elector.is_leader is True


class TestEventHandlerFuncIdentity:
    """EventHandlerFunc's canonical home is ports.handlers; all re-exports match."""

    def test_bus_interface_reexports_same_object(self) -> None:
        from eventsource.bus.interface import EventHandlerFunc as ReExported

        assert ReExported is EventHandlerFunc

    def test_bus_package_reexports_same_object(self) -> None:
        from eventsource.bus import EventHandlerFunc as ReExported

        assert ReExported is EventHandlerFunc

    def test_top_level_barrel_reexports_same_object(self) -> None:
        from eventsource import EventHandlerFunc as ReExported

        assert ReExported is EventHandlerFunc

    def test_testing_recording_reexports_same_object(self) -> None:
        from eventsource.testing.recording import EventHandlerFunc as ReExported

        assert ReExported is EventHandlerFunc


class TestSubscribableEventBus:
    """The two-method port EventBus implementations satisfy structurally."""

    def test_in_memory_event_bus_is_assignable(self) -> None:
        bus: SubscribableEventBus = InMemoryEventBus()
        assert hasattr(bus, "subscribe")
        assert hasattr(bus, "unsubscribe")

    def test_port_has_only_subscribe_and_unsubscribe(self) -> None:
        # ISP check: the port is intentionally narrow -- just the two
        # methods the live runner actually calls.
        annotations = {name for name in vars(SubscribableEventBus) if not name.startswith("_")}
        assert annotations == {"subscribe", "unsubscribe"}


class TestSubscriptionErrorRebase:
    """SubscriptionError is rebased onto EventSourceError (widening change)."""

    def test_subscription_error_is_event_source_error(self) -> None:
        assert issubclass(SubscriptionError, EventSourceError)

    def test_subscription_error_catchable_as_event_source_error(self) -> None:
        try:
            raise SubscriptionError("boom")
        except EventSourceError as caught:
            assert isinstance(caught, SubscriptionError)
        else:
            raise AssertionError("SubscriptionError was not caught as EventSourceError")
