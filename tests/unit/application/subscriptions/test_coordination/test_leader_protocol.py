"""Tests for LeaderElector protocol definition and mock elector behavior."""

import pytest

from eventsource.ports.coordination import LeaderChangeCallback, LeaderElector


class TestLeaderElectorProtocol:
    def test_protocol_is_runtime_checkable(self) -> None:
        assert getattr(LeaderElector, "_is_runtime_protocol", False)

    def test_protocol_has_required_methods(self) -> None:
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
        for member in expected_members:
            assert hasattr(LeaderElector, member), f"Missing member: {member}"

    def test_mock_implementation_satisfies_protocol(self) -> None:
        class MockLeaderElector:
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

        assert isinstance(MockLeaderElector(), LeaderElector)

    def test_incomplete_implementation_fails_protocol_check(self) -> None:
        class IncompleteElector:
            @property
            def identity(self) -> str:
                return "test"

        assert not isinstance(IncompleteElector(), LeaderElector)


class TestModuleExports:
    def test_all_exports_are_defined(self) -> None:
        from eventsource.application.subscriptions import coordination

        for symbol in ("LeaderElector", "LeaderChangeCallback"):
            assert symbol in coordination.__all__, f"Missing core export: {symbol}"

    def test_exports_from_subscriptions_package(self) -> None:
        from eventsource.application.subscriptions import (
            LeaderChangeCallback,
            LeaderElector,
        )

        assert LeaderElector is not None
        assert LeaderChangeCallback is not None


@pytest.mark.asyncio
class TestMockElectorBehavior:
    async def test_acquire_and_release_workflow(self) -> None:
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
                return True

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

        elector = MockElector(identity="instance-1")
        assert isinstance(elector, LeaderElector)
        elector.on_leader_change(track_callback)

        assert not elector.is_leader and elector.current_leader is None
        assert await elector.try_acquire() is True
        assert elector.is_leader and elector.current_leader == "instance-1"
        assert callbacks_called == [True]
        assert await elector.renew() is True

        await elector.release()
        assert not elector.is_leader and elector.current_leader is None
        assert callbacks_called == [True, False]
        assert elector.remove_leader_change_callback(track_callback) is True
        assert elector.remove_leader_change_callback(track_callback) is False
