"""Tests for InMemoryLeaderElector and SharedLeaderState."""

import asyncio

import pytest

from eventsource.adapters.memory.coordination import InMemoryLeaderElector, SharedLeaderState
from eventsource.ports.coordination import LeaderElector


def _pair() -> tuple[SharedLeaderState, InMemoryLeaderElector, InMemoryLeaderElector]:
    state = SharedLeaderState()
    return (
        state,
        InMemoryLeaderElector(_identity="worker-1", shared_state=state),
        InMemoryLeaderElector(_identity="worker-2", shared_state=state),
    )


class TestInMemoryLeaderElector:
    @pytest.mark.asyncio
    async def test_single_instance_becomes_leader(self) -> None:
        elector = InMemoryLeaderElector(_identity="worker-1")
        assert await elector.try_acquire() is True
        assert elector.is_leader is True and elector.current_leader == "worker-1"

    @pytest.mark.asyncio
    async def test_release_leadership(self) -> None:
        elector = InMemoryLeaderElector(_identity="worker-1")
        await elector.try_acquire()
        await elector.release()
        assert elector.is_leader is False

    @pytest.mark.asyncio
    async def test_shared_state_first_wins(self) -> None:
        state, elector1, elector2 = _pair()
        assert await elector1.try_acquire() is True
        assert await elector2.try_acquire() is False
        assert elector1.is_leader is True and elector2.is_leader is False
        assert state.current_leader == "worker-1"

    @pytest.mark.asyncio
    async def test_shared_state_second_wins_after_release(self) -> None:
        state, elector1, elector2 = _pair()
        await elector1.try_acquire()
        await elector1.release()
        assert await elector2.try_acquire() is True
        assert elector1.is_leader is False and elector2.is_leader is True
        assert state.current_leader == "worker-2"

    @pytest.mark.asyncio
    async def test_callback_invoked_on_become_leader(self) -> None:
        elector = InMemoryLeaderElector(_identity="worker-1")
        vals: list[bool] = []

        async def cb(is_leader: bool) -> None:
            vals.append(is_leader)

        elector.on_leader_change(cb)
        await elector.try_acquire()
        assert vals == [True]

    @pytest.mark.asyncio
    async def test_callback_invoked_on_release(self) -> None:
        elector = InMemoryLeaderElector(_identity="worker-1")
        vals: list[bool] = []

        async def cb(is_leader: bool) -> None:
            vals.append(is_leader)

        elector.on_leader_change(cb)
        await elector.try_acquire()
        await elector.release()
        assert vals == [True, False]

    @pytest.mark.asyncio
    async def test_callback_error_doesnt_break_leadership(self) -> None:
        elector = InMemoryLeaderElector(_identity="worker-1")
        invoked = False

        async def failing_cb(is_leader: bool) -> None:
            raise RuntimeError("Callback failed")

        async def success_cb(is_leader: bool) -> None:
            nonlocal invoked
            invoked = True

        elector.on_leader_change(failing_cb)
        elector.on_leader_change(success_cb)
        await elector.try_acquire()
        assert elector.is_leader is True and invoked is True

    @pytest.mark.asyncio
    async def test_force_lose_leadership(self) -> None:
        elector = InMemoryLeaderElector(_identity="worker-1")
        await elector.try_acquire()
        await elector.force_lose_leadership()
        assert elector.is_leader is False

    @pytest.mark.asyncio
    async def test_remove_callback(self) -> None:
        elector = InMemoryLeaderElector(_identity="worker-1")
        vals: list[bool] = []

        async def cb(is_leader: bool) -> None:
            vals.append(is_leader)

        elector.on_leader_change(cb)
        removed = elector.remove_leader_change_callback(cb)
        await elector.try_acquire()
        assert removed is True and vals == []

    @pytest.mark.asyncio
    async def test_concurrent_acquire(self) -> None:
        state = SharedLeaderState()
        electors = [
            InMemoryLeaderElector(_identity=f"worker-{i}", shared_state=state) for i in range(10)
        ]
        results = await asyncio.gather(*(e.try_acquire() for e in electors))
        assert sum(results) == 1 and sum(e.is_leader for e in electors) == 1

    def test_satisfies_protocol(self) -> None:
        assert isinstance(InMemoryLeaderElector(_identity="worker-1"), LeaderElector)

    @pytest.mark.asyncio
    async def test_renew_returns_true_when_leader(self) -> None:
        elector = InMemoryLeaderElector(_identity="worker-1")
        await elector.try_acquire()
        assert await elector.renew() is True

    @pytest.mark.asyncio
    async def test_renew_returns_false_when_not_leader(self) -> None:
        assert await InMemoryLeaderElector(_identity="worker-1").renew() is False

    @pytest.mark.asyncio
    async def test_identity_property(self) -> None:
        assert InMemoryLeaderElector(_identity="my-instance").identity == "my-instance"

    @pytest.mark.asyncio
    async def test_current_leader_returns_none_when_no_leader_single_instance(self) -> None:
        assert InMemoryLeaderElector(_identity="worker-1").current_leader is None

    @pytest.mark.asyncio
    async def test_current_leader_returns_identity_from_shared_state(self) -> None:
        _, elector1, elector2 = _pair()
        await elector1.try_acquire()
        assert elector1.current_leader == "worker-1" and elector2.current_leader == "worker-1"

    @pytest.mark.asyncio
    async def test_try_acquire_returns_true_when_already_leader(self) -> None:
        elector = InMemoryLeaderElector(_identity="worker-1")
        assert await elector.try_acquire() is True
        assert await elector.try_acquire() is True and elector.is_leader is True

    @pytest.mark.asyncio
    async def test_release_when_not_leader_is_safe(self) -> None:
        elector = InMemoryLeaderElector(_identity="worker-1")
        await elector.release()
        assert elector.is_leader is False

    @pytest.mark.asyncio
    async def test_force_lose_leadership_with_shared_state(self) -> None:
        state, elector1, elector2 = _pair()
        await elector1.try_acquire()
        assert state.current_leader == "worker-1"
        await elector1.force_lose_leadership()
        assert elector1.is_leader is False and state.current_leader is None
        assert await elector2.try_acquire() is True
        assert elector2.is_leader is True and state.current_leader == "worker-2"

    @pytest.mark.asyncio
    async def test_callback_invoked_on_force_lose_leadership(self) -> None:
        elector = InMemoryLeaderElector(_identity="worker-1")
        vals: list[bool] = []

        async def cb(is_leader: bool) -> None:
            vals.append(is_leader)

        elector.on_leader_change(cb)
        await elector.try_acquire()
        await elector.force_lose_leadership()
        assert vals == [True, False]

    @pytest.mark.asyncio
    async def test_remove_nonexistent_callback(self) -> None:
        async def cb(is_leader: bool) -> None:
            pass

        assert (
            InMemoryLeaderElector(_identity="worker-1").remove_leader_change_callback(cb) is False
        )

    @pytest.mark.asyncio
    async def test_multiple_callbacks(self) -> None:
        elector = InMemoryLeaderElector(_identity="worker-1")
        cb1, cb2 = [], []

        async def callback1(is_leader: bool) -> None:
            cb1.append(is_leader)

        async def callback2(is_leader: bool) -> None:
            cb2.append(is_leader)

        elector.on_leader_change(callback1)
        elector.on_leader_change(callback2)
        await elector.try_acquire()
        assert cb1 == [True] and cb2 == [True]


class TestSharedLeaderState:
    def test_initial_state_has_no_leader(self) -> None:
        assert SharedLeaderState().current_leader is None

    def test_can_set_leader(self) -> None:
        assert SharedLeaderState(current_leader="worker-1").current_leader == "worker-1"

    def test_can_clear_leader(self) -> None:
        state = SharedLeaderState(current_leader="worker-1")
        state.current_leader = None
        assert state.current_leader is None


class TestModuleExportsWithInMemory:
    def test_all_exports_include_inmemory(self) -> None:
        from eventsource.adapters.memory import coordination

        for symbol in {"InMemoryLeaderElector", "SharedLeaderState"}:
            assert symbol in coordination.__all__, f"Missing core export: {symbol}"

    def test_not_exported_from_subscriptions_package(self) -> None:
        import eventsource.application.subscriptions as subscriptions_module

        assert not hasattr(subscriptions_module, "InMemoryLeaderElector")
        assert not hasattr(subscriptions_module, "SharedLeaderState")

    def test_exports_from_adapters_memory_package(self) -> None:
        from eventsource.adapters.memory import InMemoryLeaderElector, SharedLeaderState

        assert InMemoryLeaderElector is not None and SharedLeaderState is not None
