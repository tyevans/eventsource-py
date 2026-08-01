"""Store lifecycle port: optional resource-release capability.

`close()` is not part of any store port (`FullEventStore`, `SnapshotStore`,
etc.) -- most adapters have no resources of their own to release
(`MemoryEventStore` is pure Python state) and a caller-injected driver
resource (an `AsyncEngine`, a connection pool) is not automatically
something the adapter should tear down. Adapters that do own a releasable
resource implement `SupportsClose` structurally; callers that want to
release it check `isinstance(store, SupportsClose)` rather than
duck-typing `getattr(store, "close", None)`.

Ownership contract: `close()` releases only resources the object itself
owns and created. It must never tear down a resource the caller injected
and still owns -- a store built from a caller-supplied `AsyncEngine`, for
example, must not dispose that engine unless the caller has explicitly
handed over ownership (see `PostgreSQLEventStore`'s `owns_engine`
constructor flag). Implementations must be idempotent: calling `close()`
more than once must not raise.
"""

from typing import Protocol, runtime_checkable


@runtime_checkable
class SupportsClose(Protocol):
    """Optional capability: release resources this object owns.

    A store that has nothing of its own to release simply does not
    implement this Protocol -- there is no default no-op body and no
    `NotImplementedError` fallback.
    """

    async def close(self) -> None:
        """Release resources owned by this object. Idempotent.

        Must not release a resource injected by and still owned by the
        caller (see the module docstring's ownership contract).
        """
        ...


__all__ = ["SupportsClose"]
