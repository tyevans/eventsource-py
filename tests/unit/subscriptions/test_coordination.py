"""
Tests for LeaderElector protocol definition and InMemoryLeaderElector implementation.

These tests verify that the protocol is correctly defined and
can be used for runtime type checking, as well as test the
InMemoryLeaderElector implementation.
"""

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from eventsource.subscriptions.coordination import (
    COORDINATION_TOPIC_PREFIX,
    HEARTBEAT_TOPIC,
    SHUTDOWN_NOTIFICATIONS_TOPIC,
    WORK_ASSIGNMENT_TOPIC,
    HeartbeatMessage,
    InMemoryLeaderElector,
    LeaderChangeCallback,
    LeaderElector,
    LeaderElectorWithLease,
    PeerInfo,
    SharedLeaderState,
    ShutdownIntent,
    ShutdownNotification,
    WorkAssignment,
    WorkRedistributionCoordinator,
)


class TestLeaderElectorProtocol:
    """Tests for LeaderElector protocol definition."""

    def test_protocol_is_runtime_checkable(self) -> None:
        """Test that protocol can be used with isinstance."""
        # Protocol should be marked as runtime checkable
        # In Python 3.12+, this is __protocol_attrs__; in 3.11, it's _is_protocol
        assert getattr(LeaderElector, "_is_protocol", False) or hasattr(
            LeaderElector, "__protocol_attrs__"
        )

    def test_protocol_has_required_methods(self) -> None:
        """Test that protocol defines all required abstract methods."""
        # Verify all expected methods/properties are present on the protocol
        expected_members = {
            "identity",
            "is_leader",
            "current_leader",
            "try_acquire",
            "release",
            "renew",
            "on_leader_change",
            "remove_leader_change_callback",
        }
        # Simply check that all expected members exist on the protocol class
        for member in expected_members:
            assert hasattr(LeaderElector, member), f"Missing member: {member}"

    def test_mock_implementation_satisfies_protocol(self) -> None:
        """Test that a mock class satisfying the protocol passes isinstance check."""

        class MockLeaderElector:
            """Mock implementation for testing protocol compliance."""

            @property
            def identity(self) -> str:
                return "test-instance"

            @property
            def is_leader(self) -> bool:
                return False

            @property
            def current_leader(self) -> str | None:
                return None

            async def try_acquire(self, timeout: float = 10.0) -> bool:
                return True

            async def release(self) -> None:
                pass

            async def renew(self) -> bool:
                return True

            def on_leader_change(self, callback: LeaderChangeCallback) -> None:
                pass

            def remove_leader_change_callback(self, callback: LeaderChangeCallback) -> bool:
                return True

        mock = MockLeaderElector()
        assert isinstance(mock, LeaderElector)

    def test_incomplete_implementation_fails_protocol_check(self) -> None:
        """Test that incomplete implementations fail isinstance check."""

        class IncompleteElector:
            """Missing required methods."""

            @property
            def identity(self) -> str:
                return "test"

            # Missing: is_leader, current_leader, try_acquire, release,
            #          renew, on_leader_change, remove_leader_change_callback

        incomplete = IncompleteElector()
        assert not isinstance(incomplete, LeaderElector)


class TestLeaderElectorWithLeaseProtocol:
    """Tests for LeaderElectorWithLease extended protocol."""

    def test_protocol_is_runtime_checkable(self) -> None:
        """Test that extended protocol can be used with isinstance."""
        # Protocol should be marked as runtime checkable
        # In Python 3.12+, this is __protocol_attrs__; in 3.11, it's _is_protocol
        assert getattr(LeaderElectorWithLease, "_is_protocol", False) or hasattr(
            LeaderElectorWithLease, "__protocol_attrs__"
        )

    def test_protocol_extends_leader_elector(self) -> None:
        """Test that LeaderElectorWithLease extends LeaderElector."""
        # Check that extended protocol has all base protocol members
        base_members = {
            "identity",
            "is_leader",
            "current_leader",
            "try_acquire",
            "release",
            "renew",
            "on_leader_change",
            "remove_leader_change_callback",
        }
        for member in base_members:
            assert hasattr(LeaderElectorWithLease, member), f"Missing base member: {member}"

        # Plus the new lease-specific members
        lease_members = {"lease_duration_seconds", "lease_remaining_seconds", "wait_for_leadership"}
        for member in lease_members:
            assert hasattr(LeaderElectorWithLease, member), f"Missing lease member: {member}"

    def test_mock_lease_implementation_satisfies_protocol(self) -> None:
        """Test that mock with lease methods passes extended protocol check."""

        class MockLeaderElectorWithLease:
            """Mock implementation for lease-based protocol."""

            @property
            def identity(self) -> str:
                return "test-instance"

            @property
            def is_leader(self) -> bool:
                return True

            @property
            def current_leader(self) -> str | None:
                return "test-instance"

            async def try_acquire(self, timeout: float = 10.0) -> bool:
                return True

            async def release(self) -> None:
                pass

            async def renew(self) -> bool:
                return True

            def on_leader_change(self, callback: LeaderChangeCallback) -> None:
                pass

            def remove_leader_change_callback(self, callback: LeaderChangeCallback) -> bool:
                return True

            @property
            def lease_duration_seconds(self) -> float:
                return 15.0

            @property
            def lease_remaining_seconds(self) -> float | None:
                return 10.0

            async def wait_for_leadership(self, timeout: float | None = None) -> bool:
                return True

        mock = MockLeaderElectorWithLease()
        assert isinstance(mock, LeaderElectorWithLease)
        assert isinstance(mock, LeaderElector)  # Also satisfies base protocol

    def test_base_implementation_does_not_satisfy_extended(self) -> None:
        """Test that base implementation doesn't satisfy extended protocol."""

        class MockLeaderElector:
            """Only implements base protocol."""

            @property
            def identity(self) -> str:
                return "test"

            @property
            def is_leader(self) -> bool:
                return False

            @property
            def current_leader(self) -> str | None:
                return None

            async def try_acquire(self, timeout: float = 10.0) -> bool:
                return True

            async def release(self) -> None:
                pass

            async def renew(self) -> bool:
                return True

            def on_leader_change(self, callback: LeaderChangeCallback) -> None:
                pass

            def remove_leader_change_callback(self, callback: LeaderChangeCallback) -> bool:
                return True

        mock = MockLeaderElector()
        assert isinstance(mock, LeaderElector)
        # Should NOT satisfy extended protocol (missing lease methods)
        assert not isinstance(mock, LeaderElectorWithLease)


class TestLeaderChangeCallback:
    """Tests for LeaderChangeCallback type alias."""

    def test_callback_type_accepts_valid_callback(self) -> None:
        """Test LeaderChangeCallback type works with valid async functions."""

        async def valid_callback(is_leader: bool) -> None:
            pass

        # Should be assignable to the type (type checker will verify)
        callback: LeaderChangeCallback = valid_callback
        assert callable(callback)

    def test_callback_type_accepts_async_method(self) -> None:
        """Test LeaderChangeCallback works with async methods."""

        class Handler:
            async def on_leader_change(self, is_leader: bool) -> None:
                self.is_leader = is_leader

        handler = Handler()
        callback: LeaderChangeCallback = handler.on_leader_change
        assert callable(callback)


class TestModuleExports:
    """Tests for module exports."""

    def test_all_exports_are_defined(self) -> None:
        """Test that __all__ contains all public symbols."""
        from eventsource.subscriptions import coordination

        # Core leader election exports must be present
        core_expected = {
            "LeaderElector",
            "LeaderElectorWithLease",
            "LeaderChangeCallback",
            "InMemoryLeaderElector",
            "SharedLeaderState",
        }
        for symbol in core_expected:
            assert symbol in coordination.__all__, f"Missing core export: {symbol}"

    def test_exports_from_subscriptions_package(self) -> None:
        """Test that coordination types are exported from subscriptions package."""
        from eventsource.subscriptions import (
            LeaderChangeCallback,
            LeaderElector,
            LeaderElectorWithLease,
        )

        # Just verify imports work
        assert LeaderElector is not None
        assert LeaderElectorWithLease is not None
        assert LeaderChangeCallback is not None


@pytest.mark.asyncio
class TestMockElectorBehavior:
    """Integration-style tests with mock elector to verify expected behavior."""

    async def test_acquire_and_release_workflow(self) -> None:
        """Test basic acquire/release workflow with mock."""
        callbacks_called: list[bool] = []

        async def track_callback(is_leader: bool) -> None:
            callbacks_called.append(is_leader)

        class MockElector:
            def __init__(self, identity: str) -> None:
                self._identity = identity
                self._is_leader = False
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
                if not self._is_leader:
                    self._is_leader = True
                    for cb in self._callbacks:
                        await cb(True)
                    return True
                return True  # Already leader

            async def release(self) -> None:
                if self._is_leader:
                    self._is_leader = False
                    for cb in self._callbacks:
                        await cb(False)

            async def renew(self) -> bool:
                return self._is_leader

            def on_leader_change(self, callback: LeaderChangeCallback) -> None:
                self._callbacks.append(callback)

            def remove_leader_change_callback(self, callback: LeaderChangeCallback) -> bool:
                if callback in self._callbacks:
                    self._callbacks.remove(callback)
                    return True
                return False

        # Test workflow
        elector = MockElector(identity="instance-1")
        assert isinstance(elector, LeaderElector)

        elector.on_leader_change(track_callback)

        # Initially not leader
        assert not elector.is_leader
        assert elector.current_leader is None

        # Acquire leadership
        result = await elector.try_acquire()
        assert result is True
        assert elector.is_leader
        assert elector.current_leader == "instance-1"
        assert callbacks_called == [True]

        # Renew should succeed
        assert await elector.renew() is True

        # Release leadership
        await elector.release()
        assert not elector.is_leader
        assert elector.current_leader is None
        assert callbacks_called == [True, False]

        # Remove callback
        assert elector.remove_leader_change_callback(track_callback) is True
        assert elector.remove_leader_change_callback(track_callback) is False


class TestInMemoryLeaderElector:
    """Tests for InMemoryLeaderElector implementation."""

    @pytest.mark.asyncio
    async def test_single_instance_becomes_leader(self) -> None:
        """Test single instance always becomes leader."""
        elector = InMemoryLeaderElector(_identity="worker-1")

        result = await elector.try_acquire()

        assert result is True
        assert elector.is_leader is True
        assert elector.current_leader == "worker-1"

    @pytest.mark.asyncio
    async def test_release_leadership(self) -> None:
        """Test releasing leadership."""
        elector = InMemoryLeaderElector(_identity="worker-1")
        await elector.try_acquire()

        await elector.release()

        assert elector.is_leader is False

    @pytest.mark.asyncio
    async def test_shared_state_first_wins(self) -> None:
        """Test first instance wins with shared state."""
        state = SharedLeaderState()
        elector1 = InMemoryLeaderElector(_identity="worker-1", shared_state=state)
        elector2 = InMemoryLeaderElector(_identity="worker-2", shared_state=state)

        result1 = await elector1.try_acquire()
        result2 = await elector2.try_acquire()

        assert result1 is True
        assert result2 is False
        assert elector1.is_leader is True
        assert elector2.is_leader is False
        assert state.current_leader == "worker-1"

    @pytest.mark.asyncio
    async def test_shared_state_second_wins_after_release(self) -> None:
        """Test second instance can become leader after first releases."""
        state = SharedLeaderState()
        elector1 = InMemoryLeaderElector(_identity="worker-1", shared_state=state)
        elector2 = InMemoryLeaderElector(_identity="worker-2", shared_state=state)

        await elector1.try_acquire()
        await elector1.release()
        result2 = await elector2.try_acquire()

        assert result2 is True
        assert elector1.is_leader is False
        assert elector2.is_leader is True
        assert state.current_leader == "worker-2"

    @pytest.mark.asyncio
    async def test_callback_invoked_on_become_leader(self) -> None:
        """Test callback invoked when becoming leader."""
        elector = InMemoryLeaderElector(_identity="worker-1")
        callback_values: list[bool] = []

        async def callback(is_leader: bool) -> None:
            callback_values.append(is_leader)

        elector.on_leader_change(callback)
        await elector.try_acquire()

        assert callback_values == [True]

    @pytest.mark.asyncio
    async def test_callback_invoked_on_release(self) -> None:
        """Test callback invoked when releasing leadership."""
        elector = InMemoryLeaderElector(_identity="worker-1")
        callback_values: list[bool] = []

        async def callback(is_leader: bool) -> None:
            callback_values.append(is_leader)

        elector.on_leader_change(callback)
        await elector.try_acquire()
        await elector.release()

        assert callback_values == [True, False]

    @pytest.mark.asyncio
    async def test_callback_error_doesnt_break_leadership(self) -> None:
        """Test that callback error doesn't prevent leadership change."""
        elector = InMemoryLeaderElector(_identity="worker-1")
        callback_invoked = False

        async def failing_callback(is_leader: bool) -> None:
            raise RuntimeError("Callback failed")

        async def success_callback(is_leader: bool) -> None:
            nonlocal callback_invoked
            callback_invoked = True

        elector.on_leader_change(failing_callback)
        elector.on_leader_change(success_callback)
        await elector.try_acquire()

        assert elector.is_leader is True
        assert callback_invoked is True

    @pytest.mark.asyncio
    async def test_force_lose_leadership(self) -> None:
        """Test forcing leadership loss for testing."""
        elector = InMemoryLeaderElector(_identity="worker-1")
        await elector.try_acquire()

        await elector.force_lose_leadership()

        assert elector.is_leader is False

    @pytest.mark.asyncio
    async def test_remove_callback(self) -> None:
        """Test removing a callback."""
        elector = InMemoryLeaderElector(_identity="worker-1")
        callback_values: list[bool] = []

        async def callback(is_leader: bool) -> None:
            callback_values.append(is_leader)

        elector.on_leader_change(callback)
        removed = elector.remove_leader_change_callback(callback)
        await elector.try_acquire()

        assert removed is True
        assert callback_values == []

    @pytest.mark.asyncio
    async def test_concurrent_acquire(self) -> None:
        """Test concurrent acquisition attempts."""
        state = SharedLeaderState()
        electors = [
            InMemoryLeaderElector(_identity=f"worker-{i}", shared_state=state) for i in range(10)
        ]

        results = await asyncio.gather(*(e.try_acquire() for e in electors))

        # Exactly one should succeed
        assert sum(results) == 1
        assert sum(e.is_leader for e in electors) == 1

    def test_satisfies_protocol(self) -> None:
        """Test that implementation satisfies LeaderElector protocol."""
        elector = InMemoryLeaderElector(_identity="worker-1")
        assert isinstance(elector, LeaderElector)

    @pytest.mark.asyncio
    async def test_renew_returns_true_when_leader(self) -> None:
        """Test that renew returns True when currently the leader."""
        elector = InMemoryLeaderElector(_identity="worker-1")
        await elector.try_acquire()

        result = await elector.renew()

        assert result is True

    @pytest.mark.asyncio
    async def test_renew_returns_false_when_not_leader(self) -> None:
        """Test that renew returns False when not the leader."""
        elector = InMemoryLeaderElector(_identity="worker-1")

        result = await elector.renew()

        assert result is False

    @pytest.mark.asyncio
    async def test_identity_property(self) -> None:
        """Test that identity property returns the configured identity."""
        elector = InMemoryLeaderElector(_identity="my-instance")
        assert elector.identity == "my-instance"

    @pytest.mark.asyncio
    async def test_current_leader_returns_none_when_no_leader_single_instance(
        self,
    ) -> None:
        """Test current_leader returns None when not leader in single-instance mode."""
        elector = InMemoryLeaderElector(_identity="worker-1")
        assert elector.current_leader is None

    @pytest.mark.asyncio
    async def test_current_leader_returns_identity_from_shared_state(self) -> None:
        """Test current_leader returns the identity from shared state."""
        state = SharedLeaderState()
        elector1 = InMemoryLeaderElector(_identity="worker-1", shared_state=state)
        elector2 = InMemoryLeaderElector(_identity="worker-2", shared_state=state)

        await elector1.try_acquire()

        # Both should report the same current leader
        assert elector1.current_leader == "worker-1"
        assert elector2.current_leader == "worker-1"

    @pytest.mark.asyncio
    async def test_try_acquire_returns_true_when_already_leader(self) -> None:
        """Test try_acquire returns True when already the leader."""
        elector = InMemoryLeaderElector(_identity="worker-1")

        result1 = await elector.try_acquire()
        result2 = await elector.try_acquire()

        assert result1 is True
        assert result2 is True
        assert elector.is_leader is True

    @pytest.mark.asyncio
    async def test_release_when_not_leader_is_safe(self) -> None:
        """Test that release is safe to call when not the leader."""
        elector = InMemoryLeaderElector(_identity="worker-1")

        # Should not raise
        await elector.release()

        assert elector.is_leader is False

    @pytest.mark.asyncio
    async def test_force_lose_leadership_with_shared_state(self) -> None:
        """Test force_lose_leadership clears shared state."""
        state = SharedLeaderState()
        elector1 = InMemoryLeaderElector(_identity="worker-1", shared_state=state)
        elector2 = InMemoryLeaderElector(_identity="worker-2", shared_state=state)

        await elector1.try_acquire()
        assert state.current_leader == "worker-1"

        await elector1.force_lose_leadership()

        assert elector1.is_leader is False
        assert state.current_leader is None

        # Now elector2 can acquire
        result = await elector2.try_acquire()
        assert result is True
        assert elector2.is_leader is True
        assert state.current_leader == "worker-2"

    @pytest.mark.asyncio
    async def test_callback_invoked_on_force_lose_leadership(self) -> None:
        """Test callback is invoked when forcing leadership loss."""
        elector = InMemoryLeaderElector(_identity="worker-1")
        callback_values: list[bool] = []

        async def callback(is_leader: bool) -> None:
            callback_values.append(is_leader)

        elector.on_leader_change(callback)
        await elector.try_acquire()
        await elector.force_lose_leadership()

        assert callback_values == [True, False]

    @pytest.mark.asyncio
    async def test_remove_nonexistent_callback(self) -> None:
        """Test removing a callback that was never registered."""
        elector = InMemoryLeaderElector(_identity="worker-1")

        async def callback(is_leader: bool) -> None:
            pass

        result = elector.remove_leader_change_callback(callback)
        assert result is False

    @pytest.mark.asyncio
    async def test_multiple_callbacks(self) -> None:
        """Test multiple callbacks are all invoked."""
        elector = InMemoryLeaderElector(_identity="worker-1")
        callback1_values: list[bool] = []
        callback2_values: list[bool] = []

        async def callback1(is_leader: bool) -> None:
            callback1_values.append(is_leader)

        async def callback2(is_leader: bool) -> None:
            callback2_values.append(is_leader)

        elector.on_leader_change(callback1)
        elector.on_leader_change(callback2)
        await elector.try_acquire()

        assert callback1_values == [True]
        assert callback2_values == [True]


class TestSharedLeaderState:
    """Tests for SharedLeaderState."""

    def test_initial_state_has_no_leader(self) -> None:
        """Test that initial state has no leader."""
        state = SharedLeaderState()
        assert state.current_leader is None

    def test_can_set_leader(self) -> None:
        """Test that leader can be set."""
        state = SharedLeaderState()
        state.current_leader = "worker-1"
        assert state.current_leader == "worker-1"

    def test_can_clear_leader(self) -> None:
        """Test that leader can be cleared."""
        state = SharedLeaderState()
        state.current_leader = "worker-1"
        state.current_leader = None
        assert state.current_leader is None


class TestModuleExportsWithInMemory:
    """Tests for updated module exports including InMemoryLeaderElector."""

    def test_all_exports_include_inmemory(self) -> None:
        """Test that __all__ contains InMemoryLeaderElector and SharedLeaderState."""
        from eventsource.subscriptions import coordination

        # Core leader election exports should be present
        core_expected = {
            "LeaderElector",
            "LeaderElectorWithLease",
            "LeaderChangeCallback",
            "InMemoryLeaderElector",
            "SharedLeaderState",
        }
        for symbol in core_expected:
            assert symbol in coordination.__all__, f"Missing core export: {symbol}"

    def test_exports_from_subscriptions_package_include_inmemory(self) -> None:
        """Test that InMemoryLeaderElector is exported from subscriptions package."""
        from eventsource.subscriptions import (
            InMemoryLeaderElector,
            SharedLeaderState,
        )

        # Just verify imports work
        assert InMemoryLeaderElector is not None
        assert SharedLeaderState is not None


# =============================================================================
# Tests for Work Redistribution Signals (P3-003)
# =============================================================================


class TestTopicConstants:
    """Tests for coordination topic constants."""

    def test_topic_prefix(self) -> None:
        """Test that topic prefix is defined."""
        assert COORDINATION_TOPIC_PREFIX == "__eventsource_coordination"

    def test_shutdown_topic(self) -> None:
        """Test shutdown notifications topic."""
        assert SHUTDOWN_NOTIFICATIONS_TOPIC == "__eventsource_coordination.shutdown"

    def test_heartbeat_topic(self) -> None:
        """Test heartbeat topic."""
        assert HEARTBEAT_TOPIC == "__eventsource_coordination.heartbeat"

    def test_work_assignment_topic(self) -> None:
        """Test work assignment topic."""
        assert WORK_ASSIGNMENT_TOPIC == "__eventsource_coordination.work_assignment"


class TestShutdownIntent:
    """Tests for ShutdownIntent enumeration."""

    def test_all_intents_defined(self) -> None:
        """Test that all shutdown intents are defined."""
        assert ShutdownIntent.GRACEFUL.value == "graceful"
        assert ShutdownIntent.PREEMPTION.value == "preemption"
        assert ShutdownIntent.HEALTH_FAILURE.value == "health_failure"
        assert ShutdownIntent.MAINTENANCE.value == "maintenance"

    def test_intent_from_value(self) -> None:
        """Test creating intent from string value."""
        assert ShutdownIntent("graceful") == ShutdownIntent.GRACEFUL
        assert ShutdownIntent("preemption") == ShutdownIntent.PREEMPTION


class TestShutdownNotification:
    """Tests for ShutdownNotification."""

    def test_create_notification(self) -> None:
        """Test creating a shutdown notification."""
        now = datetime.now(UTC)
        later = now + timedelta(seconds=30)

        notification = ShutdownNotification(
            instance_id="worker-1",
            intent=ShutdownIntent.GRACEFUL,
            initiated_at=now,
            expected_completion_at=later,
            subscriptions=("sub-1", "sub-2"),
            in_flight_count=10,
            metadata={"key": "value"},
        )

        assert notification.instance_id == "worker-1"
        assert notification.intent == ShutdownIntent.GRACEFUL
        assert notification.initiated_at == now
        assert notification.expected_completion_at == later
        assert notification.subscriptions == ("sub-1", "sub-2")
        assert notification.in_flight_count == 10
        assert notification.metadata == {"key": "value"}

    def test_to_dict(self) -> None:
        """Test serialization to dictionary."""
        now = datetime(2025, 1, 1, 0, 0, 0, tzinfo=UTC)
        later = datetime(2025, 1, 1, 0, 0, 30, tzinfo=UTC)

        notification = ShutdownNotification(
            instance_id="worker-1",
            intent=ShutdownIntent.GRACEFUL,
            initiated_at=now,
            expected_completion_at=later,
            subscriptions=("sub-1", "sub-2"),
            in_flight_count=10,
        )

        result = notification.to_dict()

        assert result["instance_id"] == "worker-1"
        assert result["intent"] == "graceful"
        assert result["initiated_at"] == "2025-01-01T00:00:00+00:00"
        assert result["expected_completion_at"] == "2025-01-01T00:00:30+00:00"
        assert result["subscriptions"] == ["sub-1", "sub-2"]
        assert result["in_flight_count"] == 10

    def test_from_dict(self) -> None:
        """Test deserialization from dictionary."""
        data = {
            "instance_id": "worker-1",
            "intent": "preemption",
            "initiated_at": "2025-01-01T00:00:00+00:00",
            "expected_completion_at": "2025-01-01T00:00:30+00:00",
            "subscriptions": ["sub-1"],
            "in_flight_count": 5,
            "metadata": {"spot": True},
        }

        notification = ShutdownNotification.from_dict(data)

        assert notification.instance_id == "worker-1"
        assert notification.intent == ShutdownIntent.PREEMPTION
        assert notification.subscriptions == ("sub-1",)
        assert notification.in_flight_count == 5
        assert notification.metadata == {"spot": True}

    def test_time_remaining_seconds(self) -> None:
        """Test time remaining calculation."""
        now = datetime.now(UTC)
        notification = ShutdownNotification(
            instance_id="worker-1",
            intent=ShutdownIntent.GRACEFUL,
            initiated_at=now,
            expected_completion_at=now + timedelta(seconds=30),
            subscriptions=(),
        )

        remaining = notification.time_remaining_seconds

        assert 29 < remaining <= 30

    def test_time_remaining_expired(self) -> None:
        """Test time remaining when already expired."""
        now = datetime.now(UTC)
        notification = ShutdownNotification(
            instance_id="worker-1",
            intent=ShutdownIntent.GRACEFUL,
            initiated_at=now - timedelta(seconds=60),
            expected_completion_at=now - timedelta(seconds=30),
            subscriptions=(),
        )

        assert notification.time_remaining_seconds == 0.0

    def test_is_expired(self) -> None:
        """Test is_expired property."""
        now = datetime.now(UTC)

        # Not expired
        notification = ShutdownNotification(
            instance_id="worker-1",
            intent=ShutdownIntent.GRACEFUL,
            initiated_at=now,
            expected_completion_at=now + timedelta(seconds=30),
            subscriptions=(),
        )
        assert notification.is_expired is False

        # Expired
        expired_notification = ShutdownNotification(
            instance_id="worker-1",
            intent=ShutdownIntent.GRACEFUL,
            initiated_at=now - timedelta(seconds=60),
            expected_completion_at=now - timedelta(seconds=30),
            subscriptions=(),
        )
        assert expired_notification.is_expired is True

    def test_round_trip(self) -> None:
        """Test serialize/deserialize round trip."""
        now = datetime.now(UTC)
        original = ShutdownNotification(
            instance_id="worker-1",
            intent=ShutdownIntent.MAINTENANCE,
            initiated_at=now,
            expected_completion_at=now + timedelta(seconds=60),
            subscriptions=("sub-1", "sub-2"),
            in_flight_count=15,
            metadata={"reason": "upgrade"},
        )

        data = original.to_dict()
        restored = ShutdownNotification.from_dict(data)

        assert restored.instance_id == original.instance_id
        assert restored.intent == original.intent
        assert restored.subscriptions == original.subscriptions
        assert restored.in_flight_count == original.in_flight_count
        assert restored.metadata == original.metadata


class TestHeartbeatMessage:
    """Tests for HeartbeatMessage."""

    def test_create_heartbeat(self) -> None:
        """Test creating a heartbeat message."""
        now = datetime.now(UTC)

        heartbeat = HeartbeatMessage(
            instance_id="worker-1",
            timestamp=now,
            subscriptions=("sub-1",),
            in_flight_count=5,
            is_leader=True,
            load_factor=0.75,
        )

        assert heartbeat.instance_id == "worker-1"
        assert heartbeat.timestamp == now
        assert heartbeat.subscriptions == ("sub-1",)
        assert heartbeat.in_flight_count == 5
        assert heartbeat.is_leader is True
        assert heartbeat.load_factor == 0.75

    def test_to_dict(self) -> None:
        """Test serialization to dictionary."""
        now = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)

        heartbeat = HeartbeatMessage(
            instance_id="worker-1",
            timestamp=now,
            subscriptions=("sub-1", "sub-2"),
            in_flight_count=10,
            is_leader=True,
            load_factor=0.5,
        )

        result = heartbeat.to_dict()

        assert result["instance_id"] == "worker-1"
        assert result["timestamp"] == "2025-01-01T12:00:00+00:00"
        assert result["subscriptions"] == ["sub-1", "sub-2"]
        assert result["in_flight_count"] == 10
        assert result["is_leader"] is True
        assert result["load_factor"] == 0.5

    def test_from_dict(self) -> None:
        """Test deserialization from dictionary."""
        data = {
            "instance_id": "worker-1",
            "timestamp": "2025-01-01T12:00:00+00:00",
            "subscriptions": ["sub-1"],
            "in_flight_count": 5,
            "is_leader": False,
            "load_factor": 0.25,
        }

        heartbeat = HeartbeatMessage.from_dict(data)

        assert heartbeat.instance_id == "worker-1"
        assert heartbeat.subscriptions == ("sub-1",)
        assert heartbeat.in_flight_count == 5
        assert heartbeat.is_leader is False
        assert heartbeat.load_factor == 0.25

    def test_is_stale_fresh(self) -> None:
        """Test is_stale returns False for fresh heartbeat."""
        heartbeat = HeartbeatMessage(
            instance_id="worker-1",
            timestamp=datetime.now(UTC),
        )

        assert heartbeat.is_stale(max_age_seconds=15.0) is False

    def test_is_stale_old(self) -> None:
        """Test is_stale returns True for old heartbeat."""
        heartbeat = HeartbeatMessage(
            instance_id="worker-1",
            timestamp=datetime.now(UTC) - timedelta(seconds=20),
        )

        assert heartbeat.is_stale(max_age_seconds=15.0) is True

    def test_round_trip(self) -> None:
        """Test serialize/deserialize round trip."""
        original = HeartbeatMessage(
            instance_id="worker-1",
            timestamp=datetime.now(UTC),
            subscriptions=("sub-1",),
            in_flight_count=5,
            is_leader=True,
            load_factor=0.75,
        )

        data = original.to_dict()
        restored = HeartbeatMessage.from_dict(data)

        assert restored.instance_id == original.instance_id
        assert restored.is_leader == original.is_leader
        assert restored.load_factor == original.load_factor


class TestWorkAssignment:
    """Tests for WorkAssignment."""

    def test_create_assignment(self) -> None:
        """Test creating a work assignment."""
        now = datetime.now(UTC)

        assignment = WorkAssignment(
            target_instance_id="worker-2",
            subscriptions=("sub-1", "sub-2"),
            source_instance_id="worker-1",
            assigned_at=now,
            priority=1,
        )

        assert assignment.target_instance_id == "worker-2"
        assert assignment.subscriptions == ("sub-1", "sub-2")
        assert assignment.source_instance_id == "worker-1"
        assert assignment.assigned_at == now
        assert assignment.priority == 1

    def test_to_dict(self) -> None:
        """Test serialization to dictionary."""
        now = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)

        assignment = WorkAssignment(
            target_instance_id="worker-2",
            subscriptions=("sub-1",),
            source_instance_id="worker-1",
            assigned_at=now,
            priority=2,
        )

        result = assignment.to_dict()

        assert result["target_instance_id"] == "worker-2"
        assert result["subscriptions"] == ["sub-1"]
        assert result["source_instance_id"] == "worker-1"
        assert result["assigned_at"] == "2025-01-01T12:00:00+00:00"
        assert result["priority"] == 2

    def test_from_dict(self) -> None:
        """Test deserialization from dictionary."""
        data = {
            "target_instance_id": "worker-2",
            "subscriptions": ["sub-1", "sub-2"],
            "source_instance_id": "worker-1",
            "assigned_at": "2025-01-01T12:00:00+00:00",
            "priority": 3,
        }

        assignment = WorkAssignment.from_dict(data)

        assert assignment.target_instance_id == "worker-2"
        assert assignment.subscriptions == ("sub-1", "sub-2")
        assert assignment.source_instance_id == "worker-1"
        assert assignment.priority == 3

    def test_round_trip(self) -> None:
        """Test serialize/deserialize round trip."""
        original = WorkAssignment(
            target_instance_id="worker-2",
            subscriptions=("sub-1", "sub-2"),
            source_instance_id="worker-1",
            assigned_at=datetime.now(UTC),
            priority=1,
        )

        data = original.to_dict()
        restored = WorkAssignment.from_dict(data)

        assert restored.target_instance_id == original.target_instance_id
        assert restored.subscriptions == original.subscriptions
        assert restored.source_instance_id == original.source_instance_id
        assert restored.priority == original.priority


class TestPeerInfo:
    """Tests for PeerInfo."""

    def test_initial_state(self) -> None:
        """Test initial state of peer info."""
        peer = PeerInfo(instance_id="worker-1")

        assert peer.instance_id == "worker-1"
        assert peer.last_heartbeat is None
        assert peer.shutdown_notification is None
        assert peer.status == "unknown"
        assert peer.subscriptions == ()

    def test_status_healthy(self) -> None:
        """Test status is healthy with recent heartbeat."""
        peer = PeerInfo(
            instance_id="worker-1",
            last_heartbeat=HeartbeatMessage(
                instance_id="worker-1",
                timestamp=datetime.now(UTC),
                subscriptions=("sub-1",),
            ),
        )

        assert peer.status == "healthy"
        assert peer.subscriptions == ("sub-1",)

    def test_status_stale(self) -> None:
        """Test status is stale with old heartbeat."""
        peer = PeerInfo(
            instance_id="worker-1",
            last_heartbeat=HeartbeatMessage(
                instance_id="worker-1",
                timestamp=datetime.now(UTC) - timedelta(seconds=20),
                subscriptions=("sub-1",),
            ),
        )

        assert peer.status == "stale"

    def test_status_draining(self) -> None:
        """Test status is draining with active shutdown notification."""
        now = datetime.now(UTC)
        peer = PeerInfo(
            instance_id="worker-1",
            shutdown_notification=ShutdownNotification(
                instance_id="worker-1",
                intent=ShutdownIntent.GRACEFUL,
                initiated_at=now,
                expected_completion_at=now + timedelta(seconds=30),
                subscriptions=("sub-1",),
            ),
        )

        assert peer.status == "draining"
        assert peer.subscriptions == ("sub-1",)

    def test_status_terminated(self) -> None:
        """Test status is terminated with expired shutdown notification."""
        now = datetime.now(UTC)
        peer = PeerInfo(
            instance_id="worker-1",
            shutdown_notification=ShutdownNotification(
                instance_id="worker-1",
                intent=ShutdownIntent.GRACEFUL,
                initiated_at=now - timedelta(seconds=60),
                expected_completion_at=now - timedelta(seconds=30),
                subscriptions=("sub-1",),
            ),
        )

        assert peer.status == "terminated"


@pytest.mark.asyncio
class TestWorkRedistributionCoordinator:
    """Tests for WorkRedistributionCoordinator."""

    async def test_create_coordinator(self) -> None:
        """Test creating a coordinator."""
        coordinator = WorkRedistributionCoordinator(instance_id="worker-1")

        assert coordinator.instance_id == "worker-1"
        assert coordinator.is_shutting_down is False
        assert coordinator.shutdown_notification is None
        assert coordinator.known_peers == {}
        assert coordinator.healthy_peer_count == 0
        assert coordinator.draining_peers == []

    async def test_create_shutdown_notification(self) -> None:
        """Test creating a shutdown notification."""
        coordinator = WorkRedistributionCoordinator(instance_id="worker-1")

        notification = coordinator.create_shutdown_notification(
            intent=ShutdownIntent.GRACEFUL,
            subscriptions=["sub-1", "sub-2"],
            in_flight_count=10,
            drain_timeout_seconds=30.0,
            metadata={"reason": "scale-down"},
        )

        assert notification.instance_id == "worker-1"
        assert notification.intent == ShutdownIntent.GRACEFUL
        assert notification.subscriptions == ("sub-1", "sub-2")
        assert notification.in_flight_count == 10
        assert notification.metadata == {"reason": "scale-down"}
        assert coordinator.is_shutting_down is True
        assert coordinator.shutdown_notification == notification

    async def test_create_heartbeat_without_leader(self) -> None:
        """Test creating heartbeat without leader elector."""
        coordinator = WorkRedistributionCoordinator(instance_id="worker-1")

        heartbeat = coordinator.create_heartbeat(
            subscriptions=["sub-1"],
            in_flight_count=5,
            load_factor=0.5,
        )

        assert heartbeat.instance_id == "worker-1"
        assert heartbeat.subscriptions == ("sub-1",)
        assert heartbeat.in_flight_count == 5
        assert heartbeat.is_leader is False
        assert heartbeat.load_factor == 0.5

    async def test_create_heartbeat_with_leader(self) -> None:
        """Test creating heartbeat with leader elector."""
        elector = InMemoryLeaderElector(_identity="worker-1")
        await elector.try_acquire()

        coordinator = WorkRedistributionCoordinator(
            instance_id="worker-1",
            leader_elector=elector,
        )

        heartbeat = coordinator.create_heartbeat(
            subscriptions=["sub-1"],
            in_flight_count=5,
            load_factor=0.5,
        )

        assert heartbeat.is_leader is True

    async def test_handle_peer_shutdown(self) -> None:
        """Test handling peer shutdown notification."""
        coordinator = WorkRedistributionCoordinator(instance_id="worker-1")
        received_notifications: list[ShutdownNotification] = []

        async def callback(notification: ShutdownNotification) -> None:
            received_notifications.append(notification)

        coordinator.on_peer_shutdown(callback)

        now = datetime.now(UTC)
        notification = ShutdownNotification(
            instance_id="worker-2",
            intent=ShutdownIntent.GRACEFUL,
            initiated_at=now,
            expected_completion_at=now + timedelta(seconds=30),
            subscriptions=("sub-1",),
        )

        await coordinator.handle_peer_shutdown(notification)

        assert len(received_notifications) == 1
        assert received_notifications[0] == notification
        assert "worker-2" in coordinator.known_peers
        assert coordinator.known_peers["worker-2"].status == "draining"

    async def test_handle_peer_shutdown_ignores_self(self) -> None:
        """Test that own shutdown notification is ignored."""
        coordinator = WorkRedistributionCoordinator(instance_id="worker-1")
        received_notifications: list[ShutdownNotification] = []

        async def callback(notification: ShutdownNotification) -> None:
            received_notifications.append(notification)

        coordinator.on_peer_shutdown(callback)

        now = datetime.now(UTC)
        notification = ShutdownNotification(
            instance_id="worker-1",  # Same as coordinator
            intent=ShutdownIntent.GRACEFUL,
            initiated_at=now,
            expected_completion_at=now + timedelta(seconds=30),
            subscriptions=("sub-1",),
        )

        await coordinator.handle_peer_shutdown(notification)

        assert len(received_notifications) == 0
        assert "worker-1" not in coordinator.known_peers

    async def test_handle_heartbeat(self) -> None:
        """Test handling peer heartbeat."""
        coordinator = WorkRedistributionCoordinator(instance_id="worker-1")
        received_heartbeats: list[HeartbeatMessage] = []

        async def callback(heartbeat: HeartbeatMessage) -> None:
            received_heartbeats.append(heartbeat)

        coordinator.on_heartbeat(callback)

        heartbeat = HeartbeatMessage(
            instance_id="worker-2",
            timestamp=datetime.now(UTC),
            subscriptions=("sub-1",),
            in_flight_count=5,
        )

        await coordinator.handle_heartbeat(heartbeat)

        assert len(received_heartbeats) == 1
        assert received_heartbeats[0] == heartbeat
        assert "worker-2" in coordinator.known_peers
        assert coordinator.known_peers["worker-2"].status == "healthy"
        assert coordinator.healthy_peer_count == 1

    async def test_handle_heartbeat_ignores_self(self) -> None:
        """Test that own heartbeat is ignored."""
        coordinator = WorkRedistributionCoordinator(instance_id="worker-1")
        received_heartbeats: list[HeartbeatMessage] = []

        async def callback(heartbeat: HeartbeatMessage) -> None:
            received_heartbeats.append(heartbeat)

        coordinator.on_heartbeat(callback)

        heartbeat = HeartbeatMessage(
            instance_id="worker-1",  # Same as coordinator
            timestamp=datetime.now(UTC),
        )

        await coordinator.handle_heartbeat(heartbeat)

        assert len(received_heartbeats) == 0
        assert "worker-1" not in coordinator.known_peers

    async def test_handle_work_assignment(self) -> None:
        """Test handling work assignment."""
        coordinator = WorkRedistributionCoordinator(instance_id="worker-2")
        received_assignments: list[WorkAssignment] = []

        async def callback(assignment: WorkAssignment) -> None:
            received_assignments.append(assignment)

        coordinator.on_work_assignment(callback)

        assignment = WorkAssignment(
            target_instance_id="worker-2",
            subscriptions=("sub-1",),
            source_instance_id="worker-1",
            assigned_at=datetime.now(UTC),
        )

        await coordinator.handle_work_assignment(assignment)

        assert len(received_assignments) == 1
        assert received_assignments[0] == assignment

    async def test_handle_work_assignment_ignores_other_target(self) -> None:
        """Test that work assignment for other instance is ignored."""
        coordinator = WorkRedistributionCoordinator(instance_id="worker-1")
        received_assignments: list[WorkAssignment] = []

        async def callback(assignment: WorkAssignment) -> None:
            received_assignments.append(assignment)

        coordinator.on_work_assignment(callback)

        assignment = WorkAssignment(
            target_instance_id="worker-2",  # Not this instance
            subscriptions=("sub-1",),
            source_instance_id="worker-3",
            assigned_at=datetime.now(UTC),
        )

        await coordinator.handle_work_assignment(assignment)

        assert len(received_assignments) == 0

    async def test_check_peer_timeouts(self) -> None:
        """Test checking for peer timeouts."""
        coordinator = WorkRedistributionCoordinator(
            instance_id="worker-1",
            heartbeat_timeout_seconds=15.0,
        )
        timed_out_peers: list[str] = []

        async def callback(peer_id: str) -> None:
            timed_out_peers.append(peer_id)

        coordinator.on_peer_timeout(callback)

        # Add a stale heartbeat
        stale_heartbeat = HeartbeatMessage(
            instance_id="worker-2",
            timestamp=datetime.now(UTC) - timedelta(seconds=20),
            subscriptions=("sub-1",),
        )
        await coordinator.handle_heartbeat(stale_heartbeat)

        # Add a fresh heartbeat
        fresh_heartbeat = HeartbeatMessage(
            instance_id="worker-3",
            timestamp=datetime.now(UTC),
            subscriptions=("sub-2",),
        )
        await coordinator.handle_heartbeat(fresh_heartbeat)

        timed_out = await coordinator.check_peer_timeouts()

        assert timed_out == ["worker-2"]
        assert timed_out_peers == ["worker-2"]

    async def test_remove_peer(self) -> None:
        """Test removing a peer from tracking."""
        coordinator = WorkRedistributionCoordinator(instance_id="worker-1")

        heartbeat = HeartbeatMessage(
            instance_id="worker-2",
            timestamp=datetime.now(UTC),
        )
        await coordinator.handle_heartbeat(heartbeat)

        assert "worker-2" in coordinator.known_peers

        result = coordinator.remove_peer("worker-2")

        assert result is True
        assert "worker-2" not in coordinator.known_peers

    async def test_remove_nonexistent_peer(self) -> None:
        """Test removing a peer that doesn't exist."""
        coordinator = WorkRedistributionCoordinator(instance_id="worker-1")

        result = coordinator.remove_peer("nonexistent")

        assert result is False

    async def test_get_orphaned_subscriptions(self) -> None:
        """Test getting orphaned subscriptions."""
        coordinator = WorkRedistributionCoordinator(instance_id="worker-1")

        # Add a draining peer
        now = datetime.now(UTC)
        notification = ShutdownNotification(
            instance_id="worker-2",
            intent=ShutdownIntent.GRACEFUL,
            initiated_at=now,
            expected_completion_at=now + timedelta(seconds=30),
            subscriptions=("sub-1", "sub-2"),
        )
        await coordinator.handle_peer_shutdown(notification)

        # Add a healthy peer
        heartbeat = HeartbeatMessage(
            instance_id="worker-3",
            timestamp=datetime.now(UTC),
            subscriptions=("sub-3",),
        )
        await coordinator.handle_heartbeat(heartbeat)

        orphaned = coordinator.get_orphaned_subscriptions()

        assert "worker-2" in orphaned
        assert orphaned["worker-2"] == ("sub-1", "sub-2")
        assert "worker-3" not in orphaned

    async def test_draining_peers(self) -> None:
        """Test getting draining peers."""
        coordinator = WorkRedistributionCoordinator(instance_id="worker-1")

        # Add a draining peer
        now = datetime.now(UTC)
        notification = ShutdownNotification(
            instance_id="worker-2",
            intent=ShutdownIntent.GRACEFUL,
            initiated_at=now,
            expected_completion_at=now + timedelta(seconds=30),
            subscriptions=("sub-1",),
        )
        await coordinator.handle_peer_shutdown(notification)

        draining = coordinator.draining_peers

        assert len(draining) == 1
        assert draining[0].instance_id == "worker-2"

    async def test_callback_error_handling(self) -> None:
        """Test that callback errors don't break processing."""
        coordinator = WorkRedistributionCoordinator(instance_id="worker-1")
        successful_callbacks: list[str] = []

        async def failing_callback(notification: ShutdownNotification) -> None:
            raise RuntimeError("Callback failed")

        async def success_callback(notification: ShutdownNotification) -> None:
            successful_callbacks.append(notification.instance_id)

        coordinator.on_peer_shutdown(failing_callback)
        coordinator.on_peer_shutdown(success_callback)

        now = datetime.now(UTC)
        notification = ShutdownNotification(
            instance_id="worker-2",
            intent=ShutdownIntent.GRACEFUL,
            initiated_at=now,
            expected_completion_at=now + timedelta(seconds=30),
            subscriptions=(),
        )

        # Should not raise
        await coordinator.handle_peer_shutdown(notification)

        assert successful_callbacks == ["worker-2"]

    async def test_remove_callbacks(self) -> None:
        """Test removing callbacks."""
        coordinator = WorkRedistributionCoordinator(instance_id="worker-1")

        async def shutdown_cb(n: ShutdownNotification) -> None:
            pass

        async def heartbeat_cb(h: HeartbeatMessage) -> None:
            pass

        async def timeout_cb(p: str) -> None:
            pass

        async def assignment_cb(a: WorkAssignment) -> None:
            pass

        coordinator.on_peer_shutdown(shutdown_cb)
        coordinator.on_heartbeat(heartbeat_cb)
        coordinator.on_peer_timeout(timeout_cb)
        coordinator.on_work_assignment(assignment_cb)

        assert coordinator.remove_peer_shutdown_callback(shutdown_cb) is True
        assert coordinator.remove_peer_shutdown_callback(shutdown_cb) is False

        assert coordinator.remove_heartbeat_callback(heartbeat_cb) is True
        assert coordinator.remove_heartbeat_callback(heartbeat_cb) is False

        assert coordinator.remove_peer_timeout_callback(timeout_cb) is True
        assert coordinator.remove_peer_timeout_callback(timeout_cb) is False

        assert coordinator.remove_work_assignment_callback(assignment_cb) is True
        assert coordinator.remove_work_assignment_callback(assignment_cb) is False

    async def test_initiate_leadership_handoff_without_elector(self) -> None:
        """Test leadership handoff without leader elector."""
        coordinator = WorkRedistributionCoordinator(instance_id="worker-1")

        result = await coordinator.initiate_leadership_handoff()

        assert result is False

    async def test_initiate_leadership_handoff_not_leader(self) -> None:
        """Test leadership handoff when not leader."""
        elector = InMemoryLeaderElector(_identity="worker-1")
        coordinator = WorkRedistributionCoordinator(
            instance_id="worker-1",
            leader_elector=elector,
        )

        result = await coordinator.initiate_leadership_handoff()

        assert result is False

    async def test_initiate_leadership_handoff_success(self) -> None:
        """Test successful leadership handoff."""
        elector = InMemoryLeaderElector(_identity="worker-1")
        await elector.try_acquire()

        coordinator = WorkRedistributionCoordinator(
            instance_id="worker-1",
            leader_elector=elector,
        )

        result = await coordinator.initiate_leadership_handoff()

        assert result is True
        assert elector.is_leader is False


class TestModuleExportsP3003:
    """Tests for P3-003 module exports."""

    def test_all_exports_defined(self) -> None:
        """Test that __all__ contains all P3-003 symbols."""
        from eventsource.subscriptions import coordination

        expected_new = {
            # Topic constants
            "COORDINATION_TOPIC_PREFIX",
            "SHUTDOWN_NOTIFICATIONS_TOPIC",
            "HEARTBEAT_TOPIC",
            "WORK_ASSIGNMENT_TOPIC",
            # Enums
            "ShutdownIntent",
            # Message types
            "ShutdownNotification",
            "HeartbeatMessage",
            "WorkAssignment",
            # Callback types
            "PeerShutdownCallback",
            "HeartbeatCallback",
            "WorkAssignmentCallback",
            "PeerTimeoutCallback",
            # Work redistribution
            "PeerInfo",
            "WorkRedistributionCoordinator",
        }

        # Check all expected symbols are in __all__
        for symbol in expected_new:
            assert symbol in coordination.__all__, f"Missing export: {symbol}"

    def test_exports_from_subscriptions_package(self) -> None:
        """Test that P3-003 types are exported from subscriptions package."""
        from eventsource.subscriptions import (
            COORDINATION_TOPIC_PREFIX,
            HEARTBEAT_TOPIC,
            SHUTDOWN_NOTIFICATIONS_TOPIC,
            WORK_ASSIGNMENT_TOPIC,
            HeartbeatCallback,
            HeartbeatMessage,
            PeerInfo,
            PeerShutdownCallback,
            PeerTimeoutCallback,
            ShutdownIntent,
            ShutdownNotification,
            WorkAssignment,
            WorkAssignmentCallback,
            WorkRedistributionCoordinator,
        )

        # Verify imports work
        assert COORDINATION_TOPIC_PREFIX is not None
        assert SHUTDOWN_NOTIFICATIONS_TOPIC is not None
        assert HEARTBEAT_TOPIC is not None
        assert WORK_ASSIGNMENT_TOPIC is not None
        assert ShutdownIntent is not None
        assert ShutdownNotification is not None
        assert HeartbeatMessage is not None
        assert WorkAssignment is not None
        assert HeartbeatCallback is not None
        assert PeerShutdownCallback is not None
        assert WorkAssignmentCallback is not None
        assert PeerTimeoutCallback is not None
        assert PeerInfo is not None
        assert WorkRedistributionCoordinator is not None
