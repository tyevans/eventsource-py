"""Tests for LeaderElector protocol definition and mock elector behavior."""

import pytest

from eventsource.ports.coordination import (
    LeaderChangeCallback,
    LeaderElector,
)


class TestLeaderElectorProtocol:
    """Tests for LeaderElector protocol definition."""

    def test_protocol_is_runtime_checkable(self) -> None:
        """Test that protocol can be used with isinstance."""
        # Protocol should be marked as runtime checkable. `_is_runtime_protocol`
        # is the attribute set by @runtime_checkable itself (stable since
        # Python 3.8's typing module) -- unlike `__protocol_attrs__`, it
        # actually reflects runtime-checkability rather than just "is a
        # Protocol" (every Protocol subclass has `_is_protocol = True`,
        # runtime-checkable or not).
        assert getattr(LeaderElector, "_is_runtime_protocol", False)

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


class TestModuleExports:
    """Tests for module exports."""

    def test_all_exports_are_defined(self) -> None:
        """Test that __all__ contains all public symbols.

        InMemoryLeaderElector/SharedLeaderState's canonical home is
        eventsource.adapters.memory.coordination -- see
        test_all_exports_include_inmemory below.
        """
        from eventsource.application.subscriptions import coordination

        # Core leader election exports must be present
        core_expected = {
            "LeaderElector",
            "LeaderChangeCallback",
        }
        for symbol in core_expected:
            assert symbol in coordination.__all__, f"Missing core export: {symbol}"

    def test_exports_from_subscriptions_package(self) -> None:
        """Test that coordination types are exported from subscriptions package."""
        from eventsource.application.subscriptions import (
            LeaderChangeCallback,
            LeaderElector,
        )

        # Just verify imports work
        assert LeaderElector is not None
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
