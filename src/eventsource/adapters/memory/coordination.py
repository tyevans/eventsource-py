"""In-memory leader election for testing and single-instance deployments."""

import asyncio
import logging
from dataclasses import dataclass, field

from eventsource.ports.coordination import LeaderChangeCallback

logger = logging.getLogger(__name__)


@dataclass
class SharedLeaderState:
    """Shared state for simulated multi-instance leader election in tests."""

    current_leader: str | None = None


@dataclass
class InMemoryLeaderElector:
    """In-memory leader election supporting single-instance and simulated multi-instance modes."""

    _identity: str
    shared_state: "SharedLeaderState | None" = None
    _is_leader: bool = field(default=False, repr=False)
    _callbacks: list[LeaderChangeCallback] = field(default_factory=list, repr=False)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)

    @property
    def identity(self) -> str:
        """Get the unique identity of this elector."""
        return self._identity

    @property
    def is_leader(self) -> bool:
        """Check if this instance is the leader."""
        return self._is_leader

    @property
    def current_leader(self) -> str | None:
        """Get the identity of the current leader."""
        if self.shared_state is not None:
            return self.shared_state.current_leader
        return self._identity if self._is_leader else None

    async def try_acquire(self, timeout: float = 10.0) -> bool:
        """Attempt to acquire leadership."""
        async with self._lock:
            if self.shared_state is not None:
                if self.shared_state.current_leader is None:
                    self.shared_state.current_leader = self._identity
                    await self._set_leadership(True)
                    return True
                return self.shared_state.current_leader == self._identity
            if not self._is_leader:
                await self._set_leadership(True)
            return True

    async def release(self) -> None:
        """Release leadership if currently held."""
        async with self._lock:
            if not self._is_leader:
                return
            if self.shared_state is not None and self.shared_state.current_leader == self._identity:
                self.shared_state.current_leader = None
            await self._set_leadership(False)
            logger.info("Leadership released", extra={"identity": self._identity})

    async def renew(self) -> bool:
        """Renew leadership lease (no-op for in-memory)."""
        return self._is_leader

    def on_leader_change(self, callback: LeaderChangeCallback) -> None:
        """Register callback for leadership changes."""
        self._callbacks.append(callback)

    def remove_leader_change_callback(self, callback: LeaderChangeCallback) -> bool:
        """Remove a registered leadership change callback."""
        try:
            self._callbacks.remove(callback)
            return True
        except ValueError:
            return False

    async def _set_leadership(self, is_leader: bool) -> None:
        """Set leadership state and invoke callbacks."""
        if self._is_leader == is_leader:
            return
        self._is_leader = is_leader
        logger.info(
            "Leadership changed", extra={"identity": self._identity, "is_leader": is_leader}
        )
        for callback in self._callbacks:
            try:
                await callback(is_leader)
            except Exception as e:
                logger.error(
                    "Leader change callback failed",
                    extra={"identity": self._identity, "is_leader": is_leader, "error": str(e)},
                    exc_info=True,
                )

    async def force_lose_leadership(self) -> None:
        """Force this instance to lose leadership (for testing)."""
        async with self._lock:
            if self._is_leader:
                if self.shared_state is not None:
                    self.shared_state.current_leader = None
                await self._set_leadership(False)


__all__ = ["InMemoryLeaderElector", "SharedLeaderState"]
