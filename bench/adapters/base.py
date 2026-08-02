"""Adapter contract: how a backend plugs into the harness.

Mirrors the conformance-suite factory pattern (see
src/eventsource/testing/conformance.py): scenarios never know which
backend they run on.
"""

from abc import ABC, abstractmethod

from eventsource.ports.bus import EventBus


class BenchAdapter[T](ABC):
    """Lifecycle: available? -> setup -> (create -> destroy)* -> teardown.

    create() is called once per matrix cell and must return an isolated,
    ready-to-use resource. destroy() releases it.
    """

    name: str = ""

    async def available(self) -> str | None:
        """Return None if this backend can run, else a skip reason."""
        return None

    async def setup(self) -> None:  # noqa: B027 - intentionally optional, not abstract
        """One-time session setup (schema creation, temp dirs)."""

    async def teardown(self) -> None:  # noqa: B027 - intentionally optional, not abstract
        """One-time session cleanup."""

    @abstractmethod
    async def create(self) -> T:
        """Create a fresh resource for one cell."""

    async def destroy(self, resource: T) -> None:  # noqa: B027 - intentionally optional, not abstract
        """Release a resource created by create()."""


class BusAdapter(BenchAdapter[EventBus]):
    """Bus adapters additionally manage consumer delivery.

    Scenarios subscribe handlers first, then call start_delivery();
    ordering matters for broker consumers.
    """

    async def start_delivery(self, bus: EventBus) -> None:
        """Begin delivering published events to subscribers (no-op for memory)."""

    async def stop_delivery(self, bus: EventBus) -> None:
        """Stop the consumer started by start_delivery()."""
