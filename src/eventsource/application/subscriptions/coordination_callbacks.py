"""
Callback registration mixin for WorkRedistributionCoordinator.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from eventsource.application.subscriptions.coordination_leadership import (
        WorkAssignmentCallback,
    )
    from eventsource.application.subscriptions.coordination_shutdown import (
        HeartbeatCallback,
        PeerShutdownCallback,
        PeerTimeoutCallback,
    )


class WorkRedistributionCallbacksMixin:
    """
    Mixin providing callback registration and removal methods for peer events.

    Expected to be mixed into WorkRedistributionCoordinator which manages
    the underlying callback lists.
    """

    _peer_shutdown_callbacks: list[PeerShutdownCallback]
    _heartbeat_callbacks: list[HeartbeatCallback]
    _peer_timeout_callbacks: list[PeerTimeoutCallback]
    _work_assignment_callbacks: list[WorkAssignmentCallback]

    def on_peer_shutdown(self, callback: PeerShutdownCallback) -> None:
        """
        Register callback for peer shutdown notifications.

        The callback is invoked when a peer broadcasts a shutdown
        notification. Use this to prepare for claiming orphaned work.

        Args:
            callback: Async function called with ShutdownNotification
        """
        self._peer_shutdown_callbacks.append(callback)

    def remove_peer_shutdown_callback(self, callback: PeerShutdownCallback) -> bool:
        """Remove a peer shutdown callback; returns True if removed, False if not found."""
        try:
            self._peer_shutdown_callbacks.remove(callback)
            return True
        except ValueError:
            return False

    def on_heartbeat(self, callback: HeartbeatCallback) -> None:
        """
        Register callback for peer heartbeats.

        Args:
            callback: Async function called with HeartbeatMessage
        """
        self._heartbeat_callbacks.append(callback)

    def remove_heartbeat_callback(self, callback: HeartbeatCallback) -> bool:
        """Remove a heartbeat callback; returns True if removed, False if not found."""
        try:
            self._heartbeat_callbacks.remove(callback)
            return True
        except ValueError:
            return False

    def on_peer_timeout(self, callback: PeerTimeoutCallback) -> None:
        """
        Register callback for peer timeouts.

        The callback is invoked when a peer hasn't sent a heartbeat
        within the timeout period. Use this for crash detection.

        Args:
            callback: Async function called with peer instance_id
        """
        self._peer_timeout_callbacks.append(callback)

    def remove_peer_timeout_callback(self, callback: PeerTimeoutCallback) -> bool:
        """Remove a peer timeout callback; returns True if removed, False if not found."""
        try:
            self._peer_timeout_callbacks.remove(callback)
            return True
        except ValueError:
            return False

    def on_work_assignment(self, callback: WorkAssignmentCallback) -> None:
        """
        Register callback for work assignments.

        The callback is invoked when the leader assigns work to
        this instance.

        Args:
            callback: Async function called with WorkAssignment
        """
        self._work_assignment_callbacks.append(callback)

    def remove_work_assignment_callback(self, callback: WorkAssignmentCallback) -> bool:
        """Remove a work assignment callback; returns True if removed, False if not found."""
        try:
            self._work_assignment_callbacks.remove(callback)
            return True
        except ValueError:
            return False


__all__ = ["WorkRedistributionCallbacksMixin"]
