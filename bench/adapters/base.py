"""Adapter contract: how a backend plugs into the harness.

Mirrors the conformance-suite factory pattern (see
src/eventsource/testing/conformance.py): scenarios never know which
backend they run on.
"""

from abc import ABC, abstractmethod
from typing import ClassVar, Generic, TypeVar

from eventsource.bus.interface import EventBus

T = TypeVar("T")


class BenchAdapter(ABC, Generic[T]):
    """Lifecycle: available? -> setup -> (create -> destroy)* -> teardown.

    create() is called once per matrix cell and must return an isolated,
    ready-to-use resource. destroy() releases it.
    """

    name: ClassVar[str] = ""

    async def available(self) -> str | None:
        """Return None if this backend can run, else a skip reason."""
        return None

    async def setup(self) -> None:
        """One-time session setup (schema creation, temp dirs)."""

    async def teardown(self) -> None:
        """One-time session cleanup."""

    @abstractmethod
    async def create(self) -> T:
        """Create a fresh resource for one cell."""

    async def destroy(self, resource: T) -> None:
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
