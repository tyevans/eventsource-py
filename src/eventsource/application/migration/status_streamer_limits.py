"""Resource bounds for migration status streaming (TASK-0486)."""

from __future__ import annotations

from uuid import UUID

MIN_UPDATE_INTERVAL = 0.05
MAX_SUBSCRIBERS = 100


def check_update_interval(update_interval: float) -> None:
    """Reject polling intervals below MIN_UPDATE_INTERVAL seconds."""
    if update_interval < MIN_UPDATE_INTERVAL:
        raise ValueError(
            f"update_interval must be >= {MIN_UPDATE_INTERVAL} seconds, got {update_interval}"
        )


def check_subscriber_capacity(current: int, migration_id: UUID) -> None:
    """Reject a new subscriber once a streamer holds MAX_SUBSCRIBERS subscribers."""
    if current >= MAX_SUBSCRIBERS:
        raise RuntimeError(
            f"StatusStreamer for migration {migration_id} already has {MAX_SUBSCRIBERS} "
            "subscribers; close idle streams before opening more"
        )


__all__ = [
    "MAX_SUBSCRIBERS",
    "MIN_UPDATE_INTERVAL",
    "check_subscriber_capacity",
    "check_update_interval",
]
