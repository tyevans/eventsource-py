"""Conformance suite for the `SupportsClose` lifecycle port.

Subclass `SupportsCloseConformance`, provide a `store` fixture yielding a
fresh instance, implement `use()` so the suite can force the instance to
actually acquire its resource, and answer `caller_owned_case` -- either a
`CallerOwnedResourceCase` describing an instance built over a resource the
caller still owns, or `None` for an adapter that never accepts one.

Pins the two properties `eventsource.ports.lifecycle` states as contract:

- `close()` is idempotent -- calling it more than once must not raise,
  whether or not the resource was ever acquired.
- `close()` releases only resources the object itself owns and created. A
  caller-injected resource must still be usable afterwards, unless the
  caller explicitly handed ownership over (`PostgreSQLEventStore`'s
  `owns_engine` flag).

**Deliberately not pinned here:** whether an instance is reusable after
`close()` (SQLite reopens lazily, a disposed engine does not), and what
`close()` does to in-flight operations. The port promises neither.
"""

from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

import pytest

from eventsource.ports.lifecycle import SupportsClose


@dataclass(frozen=True)
class CallerOwnedResourceCase:
    """An instance built over a resource the caller injected and still owns.

    Attributes:
        store: The instance under test, built over the caller's resource.
        is_resource_usable: Awaitable predicate the suite calls after
            `store.close()`; must report whether the caller's resource is
            still usable in its own right.
        handed_over: A second instance over the *same kind* of resource, but
            constructed with ownership explicitly transferred (e.g.
            `owns_engine=True`), or `None` if the adapter offers no such
            opt-in.
        was_resource_released: Awaitable predicate reporting whether the
            handed-over instance's resource was in fact released. Ignored
            when `handed_over` is `None`.
    """

    store: object
    is_resource_usable: Callable[[], Awaitable[bool]]
    handed_over: object | None = None
    was_resource_released: Callable[[], Awaitable[bool]] | None = None


class SupportsCloseConformance(ABC):
    """Conformance suite for `SupportsClose` implementations."""

    @abstractmethod
    @pytest.fixture
    def store(self) -> object:
        """Yield a fresh, unused instance implementing `SupportsClose`."""
        raise NotImplementedError

    @abstractmethod
    async def use(self, store: object) -> None:
        """Drive one real operation, forcing the instance to acquire its resource.

        Adapters open connections lazily; without this the idempotence
        cases would only ever exercise the never-acquired path.
        """
        raise NotImplementedError

    @abstractmethod
    @pytest.fixture
    def caller_owned_case(self) -> CallerOwnedResourceCase | None:
        """Yield a `CallerOwnedResourceCase`, or `None` if not applicable.

        Return `None` only when the adapter constructs its resource itself
        and never accepts one from the caller.
        """
        raise NotImplementedError

    async def test_satisfies_the_supports_close_protocol(self, store: object) -> None:
        assert isinstance(store, SupportsClose)

    async def test_close_on_an_unused_instance_does_not_raise(self, store: object) -> None:
        assert isinstance(store, SupportsClose)
        await store.close()

    async def test_close_is_idempotent_on_an_unused_instance(self, store: object) -> None:
        assert isinstance(store, SupportsClose)
        await store.close()
        await store.close()

    async def test_close_is_idempotent_after_use(self, store: object) -> None:
        assert isinstance(store, SupportsClose)
        await self.use(store)

        await store.close()
        await store.close()

    async def test_close_does_not_release_a_caller_owned_resource(
        self, caller_owned_case: CallerOwnedResourceCase | None
    ) -> None:
        if caller_owned_case is None:
            pytest.skip("adapter never accepts a caller-owned resource")
        store = caller_owned_case.store
        assert isinstance(store, SupportsClose)

        await store.close()

        assert await caller_owned_case.is_resource_usable() is True, (
            "close() released a resource the caller injected and still owns"
        )

    async def test_close_is_idempotent_for_a_caller_owned_resource(
        self, caller_owned_case: CallerOwnedResourceCase | None
    ) -> None:
        if caller_owned_case is None:
            pytest.skip("adapter never accepts a caller-owned resource")
        store = caller_owned_case.store
        assert isinstance(store, SupportsClose)

        await store.close()
        await store.close()

        assert await caller_owned_case.is_resource_usable() is True

    async def test_close_releases_a_resource_whose_ownership_was_handed_over(
        self, caller_owned_case: CallerOwnedResourceCase | None
    ) -> None:
        if caller_owned_case is None or caller_owned_case.handed_over is None:
            pytest.skip("adapter offers no explicit ownership hand-over")
        store = caller_owned_case.handed_over
        assert isinstance(store, SupportsClose)
        assert caller_owned_case.was_resource_released is not None

        await store.close()

        assert await caller_owned_case.was_resource_released() is True

    async def test_close_is_idempotent_after_ownership_hand_over(
        self, caller_owned_case: CallerOwnedResourceCase | None
    ) -> None:
        if caller_owned_case is None or caller_owned_case.handed_over is None:
            pytest.skip("adapter offers no explicit ownership hand-over")
        store = caller_owned_case.handed_over
        assert isinstance(store, SupportsClose)

        await store.close()
        await store.close()


__all__ = ["CallerOwnedResourceCase", "SupportsCloseConformance"]
