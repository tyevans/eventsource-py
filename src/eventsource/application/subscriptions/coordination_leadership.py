"""Leadership handoff and work assignment primitives for subscription coordination."""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from eventsource.ports.coordination import LeaderElector
from eventsource.ports.exceptions import TransitionError

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class WorkAssignment:
    """Work assignment message from leader to followers during coordinated redistribution."""

    target_instance_id: str
    subscriptions: tuple[str, ...]
    source_instance_id: str
    assigned_at: datetime
    priority: int = 0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "target_instance_id": self.target_instance_id,
            "subscriptions": list(self.subscriptions),
            "source_instance_id": self.source_instance_id,
            "assigned_at": self.assigned_at.isoformat(),
            "priority": self.priority,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> WorkAssignment:
        """Create from dictionary."""
        return cls(
            target_instance_id=data["target_instance_id"],
            subscriptions=tuple(data["subscriptions"]),
            source_instance_id=data["source_instance_id"],
            assigned_at=datetime.fromisoformat(data["assigned_at"]),
            priority=data.get("priority", 0),
        )


WorkAssignmentCallback = Callable[[WorkAssignment], Awaitable[None]]


async def verify_leadership_lease(elector: LeaderElector | None) -> bool:
    """Verify leadership lease renewal immediately prior to executing cluster operations."""
    if elector is None:
        return True
    try:
        renewed = await elector.renew()
        return bool(renewed and elector.is_leader)
    except Exception as e:
        logger.error(
            "Leadership lease renewal failed",
            extra={"identity": elector.identity, "error": str(e)},
            exc_info=True,
        )
        return False


class WorkRedistributionLeadershipMixin:
    """Mixin providing leadership lease verification and handoff for WorkRedistributionCoordinator."""

    async def verify_leadership_lease(self) -> bool:
        """Verify leadership lease renewal immediately prior to acting."""
        elector: LeaderElector | None = getattr(self, "leader_elector", None)
        if elector is None:
            return False
        return await verify_leadership_lease(elector)

    async def initiate_leadership_handoff(self) -> bool:
        """Initiate leadership handoff if this instance holds a valid renewed leadership lease."""
        elector: LeaderElector | None = getattr(self, "leader_elector", None)
        instance_id: str = getattr(self, "instance_id", "")
        if elector is None:
            return False

        if not await self.verify_leadership_lease():
            logger.warning(
                "Leadership lease expired or not held; handoff skipped",
                extra={"instance_id": instance_id},
            )
            return False

        logger.info("Initiating leadership handoff", extra={"instance_id": instance_id})
        await elector.release()
        return True

    async def create_work_assignment(
        self,
        target_instance_id: str,
        subscriptions: list[str] | tuple[str, ...],
        priority: int = 0,
    ) -> WorkAssignment:
        """Create a work assignment for a peer instance after verifying leadership lease."""
        elector: LeaderElector | None = getattr(self, "leader_elector", None)
        instance_id: str = getattr(self, "instance_id", "")
        if elector is not None and not await self.verify_leadership_lease():
            raise TransitionError(
                f"Instance '{instance_id}' does not hold an active leadership lease; "
                "cannot issue work assignment"
            )

        return WorkAssignment(
            target_instance_id=target_instance_id,
            subscriptions=tuple(subscriptions),
            source_instance_id=instance_id,
            assigned_at=datetime.now(UTC),
            priority=priority,
        )


__all__ = [
    "WorkAssignment",
    "WorkAssignmentCallback",
    "WorkRedistributionLeadershipMixin",
    "verify_leadership_lease",
]
