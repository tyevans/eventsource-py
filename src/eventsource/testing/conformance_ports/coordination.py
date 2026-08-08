"""Conformance suite for the `LeaderElector` port.

Subclass `LeaderElectorConformance` and provide an `elector` fixture
yielding a fresh, non-leading elector, plus a `rival` fixture yielding a
second elector contending for the *same* leadership (or `None` for a
backend that cannot express contention).

Pins the eight `LeaderElector` methods against
`eventsource.ports.coordination`'s documented contract, including the two
cases most likely to rot into an always-succeeds no-op:

- `renew()` returns `False` when this instance is not the leader --
  before ever acquiring, and again after `release()`. A `renew()` that
  reports success unconditionally fails here.
- `on_leader_change` callbacks fire on both edges (gained *and* lost),
  and `remove_leader_change_callback` actually stops delivery.

**Deliberately not pinned here:** lease expiry, fencing tokens, crash
detection, and cross-process exclusion -- none are in `LeaderElector`;
`LeaderElectorWithLease` owns the lease surface and has no suite yet.
"""

from abc import ABC, abstractmethod

import pytest

from eventsource.ports.coordination import LeaderElector


class LeaderElectorConformance(ABC):
    """Conformance suite for `LeaderElector` implementations."""

    @abstractmethod
    @pytest.fixture
    def elector(self) -> object:
        """Yield a fresh elector that does not yet hold leadership."""
        raise NotImplementedError

    @abstractmethod
    @pytest.fixture
    def rival(self) -> object | None:
        """Yield a second elector contending for the same leadership, or `None`."""
        raise NotImplementedError

    async def test_satisfies_the_leader_elector_protocol(self, elector: LeaderElector) -> None:
        assert isinstance(elector, LeaderElector)

    async def test_identity_is_stable_and_non_empty(self, elector: LeaderElector) -> None:
        assert elector.identity
        assert elector.identity == elector.identity

    async def test_is_not_leader_before_acquiring(self, elector: LeaderElector) -> None:
        assert elector.is_leader is False

    async def test_current_leader_does_not_name_us_before_acquiring(
        self, elector: LeaderElector
    ) -> None:
        assert elector.current_leader != elector.identity

    async def test_try_acquire_makes_us_the_leader(self, elector: LeaderElector) -> None:
        assert await elector.try_acquire() is True
        assert elector.is_leader is True
        assert elector.current_leader == elector.identity

    async def test_try_acquire_is_idempotent_for_the_holder(self, elector: LeaderElector) -> None:
        assert await elector.try_acquire() is True
        assert await elector.try_acquire() is True
        assert elector.is_leader is True

    async def test_release_gives_up_leadership(self, elector: LeaderElector) -> None:
        await elector.try_acquire()

        await elector.release()

        assert elector.is_leader is False
        assert elector.current_leader != elector.identity

    async def test_release_when_not_leader_does_not_raise(self, elector: LeaderElector) -> None:
        await elector.release()
        assert elector.is_leader is False

    async def test_release_is_idempotent(self, elector: LeaderElector) -> None:
        await elector.try_acquire()

        await elector.release()
        await elector.release()

        assert elector.is_leader is False

    async def test_renew_returns_false_before_ever_acquiring(self, elector: LeaderElector) -> None:
        assert await elector.renew() is False

    async def test_renew_returns_true_while_leader(self, elector: LeaderElector) -> None:
        await elector.try_acquire()
        assert await elector.renew() is True

    async def test_renew_returns_false_after_release(self, elector: LeaderElector) -> None:
        """The failure case a no-op `renew()` cannot express.

        `ports/coordination.py` documents `renew()` as returning "False if
        not leader or failed". An implementation that unconditionally
        reports success -- or one that never notices leadership was given
        up -- fails here.
        """
        await elector.try_acquire()
        await elector.release()

        assert await elector.renew() is False

    async def test_leader_change_callback_fires_on_both_edges(self, elector: LeaderElector) -> None:
        seen: list[bool] = []

        async def record(is_leader: bool) -> None:
            seen.append(is_leader)

        elector.on_leader_change(record)

        await elector.try_acquire()
        await elector.release()

        assert seen == [True, False]

    async def test_remove_leader_change_callback_stops_delivery(
        self, elector: LeaderElector
    ) -> None:
        seen: list[bool] = []

        async def record(is_leader: bool) -> None:
            seen.append(is_leader)

        elector.on_leader_change(record)
        assert elector.remove_leader_change_callback(record) is True

        await elector.try_acquire()

        assert seen == []

    async def test_remove_unregistered_callback_returns_false(self, elector: LeaderElector) -> None:
        async def never_registered(is_leader: bool) -> None: ...

        assert elector.remove_leader_change_callback(never_registered) is False

    async def test_a_rival_cannot_acquire_while_we_hold_leadership(
        self, elector: LeaderElector, rival: object | None
    ) -> None:
        if rival is None:
            pytest.skip("backend cannot express contention between two electors")
        assert isinstance(rival, LeaderElector)
        assert await elector.try_acquire() is True

        assert await rival.try_acquire() is False
        assert rival.is_leader is False
        assert rival.current_leader == elector.identity

    async def test_a_rival_can_acquire_after_we_release(
        self, elector: LeaderElector, rival: object | None
    ) -> None:
        if rival is None:
            pytest.skip("backend cannot express contention between two electors")
        assert isinstance(rival, LeaderElector)
        await elector.try_acquire()
        await elector.release()

        assert await rival.try_acquire() is True
        assert rival.current_leader == rival.identity

    async def test_a_rivals_renew_fails_while_another_holds_leadership(
        self, elector: LeaderElector, rival: object | None
    ) -> None:
        if rival is None:
            pytest.skip("backend cannot express contention between two electors")
        assert isinstance(rival, LeaderElector)
        await elector.try_acquire()

        assert await rival.renew() is False


__all__ = ["LeaderElectorConformance"]
