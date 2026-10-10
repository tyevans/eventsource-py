"""
Peer tracking and timeout monitoring mixin for WorkRedistributionCoordinator.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from eventsource.application.subscriptions.coordination_shutdown import (
        PeerInfo,
        PeerTimeoutCallback,
    )

logger = logging.getLogger(__name__)


class WorkRedistributionPeerTrackerMixin:
    """
    Mixin providing peer tracking, status inspection, and timeout monitoring.
    """

    @property
    def known_peers(self) -> dict[str, PeerInfo]:
        """Get a copy of known peers."""
        peers: dict[str, PeerInfo] = getattr(self, "_peers", {})
        return dict(peers)

    @property
    def healthy_peer_count(self) -> int:
        """Get count of healthy peers."""
        peers: dict[str, PeerInfo] = getattr(self, "_peers", {})
        return sum(1 for p in peers.values() if p.status == "healthy")

    @property
    def draining_peers(self) -> list[PeerInfo]:
        """Get list of peers that are draining."""
        peers: dict[str, PeerInfo] = getattr(self, "_peers", {})
        return [p for p in peers.values() if p.status == "draining"]

    def remove_peer(self, instance_id: str) -> bool:
        """
        Remove a peer from tracking.

        Args:
            instance_id: ID of peer to remove

        Returns:
            True if peer was removed, False if not found
        """
        peers: dict[str, PeerInfo] = getattr(self, "_peers", {})
        if instance_id in peers:
            del peers[instance_id]
            logger.info(
                "Removed peer from tracking",
                extra={"peer_id": instance_id},
            )
            return True
        return False

    def get_orphaned_subscriptions(self) -> dict[str, tuple[str, ...]]:
        """
        Get subscriptions from peers that are draining or terminated.

        Returns:
            Mapping of peer_id to their subscriptions
        """
        peers: dict[str, PeerInfo] = getattr(self, "_peers", {})
        orphaned: dict[str, tuple[str, ...]] = {}
        for peer_id, peer_info in peers.items():
            if peer_info.status in ("draining", "terminated", "stale") and peer_info.subscriptions:
                orphaned[peer_id] = peer_info.subscriptions
        return orphaned

    async def check_peer_timeouts(self) -> list[str]:
        """
        Check for timed-out peers and invoke callbacks.

        Should be called periodically (e.g., every heartbeat interval).

        Returns:
            List of peer instance IDs that have timed out
        """
        timed_out: list[str] = []
        peers: dict[str, PeerInfo] = getattr(self, "_peers", {})
        lock: asyncio.Lock = getattr(self, "_lock", None) or asyncio.Lock()
        timeout_seconds: float = getattr(self, "heartbeat_timeout_seconds", 15.0)
        timeout_callbacks: list[PeerTimeoutCallback] = getattr(self, "_peer_timeout_callbacks", [])

        async with lock:
            for peer_id, peer_info in list(peers.items()):
                # Skip peers that are already draining/terminated
                if peer_info.shutdown_notification is not None:
                    continue

                # Check if heartbeat is stale
                if peer_info.last_heartbeat is None:
                    continue

                if peer_info.last_heartbeat.is_stale(timeout_seconds):
                    timed_out.append(peer_id)

        # Invoke callbacks outside lock
        for peer_id in timed_out:
            logger.warning(
                "Peer heartbeat timeout",
                extra={
                    "peer_id": peer_id,
                    "timeout_seconds": timeout_seconds,
                },
            )

            for callback in timeout_callbacks:
                try:
                    await callback(peer_id)
                except Exception as e:
                    logger.error(
                        "Peer timeout callback failed",
                        extra={
                            "peer_id": peer_id,
                            "error": str(e),
                        },
                        exc_info=True,
                    )

        return timed_out


__all__ = ["WorkRedistributionPeerTrackerMixin"]
