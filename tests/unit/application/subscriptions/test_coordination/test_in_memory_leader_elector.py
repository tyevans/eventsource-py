"""Tests for InMemoryLeaderElector and SharedLeaderState."""

import asyncio

import pytest

from eventsource.adapters.memory.coordination import (
    InMemoryLeaderElector,
    SharedLeaderState,
)
from eventsource.ports.coordination import (
    LeaderElector,
)


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
    """Tests for InMemoryLeaderElector/SharedLeaderState's relocated home (ADR 0032).

    They no longer live in (or re-export from) eventsource.application.subscriptions --
    their canonical home is eventsource.adapters.memory.coordination, because
    eventsource.application.subscriptions (soon eventsource.application.subscriptions)
    may not import adapters.memory.
    """

    def test_all_exports_include_inmemory(self) -> None:
        """Test that adapters.memory.coordination.__all__ contains both names."""
        from eventsource.adapters.memory import coordination

        core_expected = {
            "InMemoryLeaderElector",
            "SharedLeaderState",
        }
        for symbol in core_expected:
            assert symbol in coordination.__all__, f"Missing core export: {symbol}"

    def test_not_exported_from_subscriptions_package(self) -> None:
        """InMemoryLeaderElector/SharedLeaderState are not re-exported here."""
        import eventsource.application.subscriptions as subscriptions_module

        assert not hasattr(subscriptions_module, "InMemoryLeaderElector")
        assert not hasattr(subscriptions_module, "SharedLeaderState")

    def test_exports_from_adapters_memory_package(self) -> None:
        """Test that InMemoryLeaderElector is exported from adapters.memory package."""
        from eventsource.adapters.memory import (
            InMemoryLeaderElector,
            SharedLeaderState,
        )

        # Just verify imports work
        assert InMemoryLeaderElector is not None
        assert SharedLeaderState is not None
