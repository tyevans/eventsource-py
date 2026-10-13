"""Tests for peer tracking, timeouts, draining, and leadership handoff."""

from datetime import UTC, datetime, timedelta

import pytest

from eventsource.adapters.memory.coordination import InMemoryLeaderElector
from eventsource.application.subscriptions.coordination import (
    HeartbeatMessage,
    ShutdownIntent,
    ShutdownNotification,
    WorkRedistributionCoordinator,
)


@pytest.mark.asyncio
class TestPeerTracking:
    """Tests for peer timeout checking, removal, and draining tracking."""

    async def test_check_peer_timeouts(self) -> None:
        """Test checking for peer timeouts."""
        coordinator = WorkRedistributionCoordinator(
            instance_id="worker-1",
            heartbeat_timeout_seconds=15.0,
        )
        timed_out_peers: list[str] = []

        async def callback(peer_id: str) -> None:
            timed_out_peers.append(peer_id)

        coordinator.on_peer_timeout(callback)

        # Add a stale heartbeat
        stale_heartbeat = HeartbeatMessage(
            instance_id="worker-2",
            timestamp=datetime.now(UTC) - timedelta(seconds=20),
            subscriptions=("sub-1",),
        )
        await coordinator.handle_heartbeat(stale_heartbeat)

        # Add a fresh heartbeat
        fresh_heartbeat = HeartbeatMessage(
            instance_id="worker-3",
            timestamp=datetime.now(UTC),
            subscriptions=("sub-2",),
        )
        await coordinator.handle_heartbeat(fresh_heartbeat)

        timed_out = await coordinator.check_peer_timeouts()

        assert timed_out == ["worker-2"]
        assert timed_out_peers == ["worker-2"]

    async def test_remove_peer(self) -> None:
        """Test removing a peer from tracking."""
        coordinator = WorkRedistributionCoordinator(instance_id="worker-1")

        heartbeat = HeartbeatMessage(
            instance_id="worker-2",
            timestamp=datetime.now(UTC),
        )
        await coordinator.handle_heartbeat(heartbeat)

        assert "worker-2" in coordinator.known_peers

        result = coordinator.remove_peer("worker-2")

        assert result is True
        assert "worker-2" not in coordinator.known_peers

    async def test_remove_nonexistent_peer(self) -> None:
        """Test removing a peer that doesn't exist."""
        coordinator = WorkRedistributionCoordinator(instance_id="worker-1")

        result = coordinator.remove_peer("nonexistent")

        assert result is False

    async def test_get_orphaned_subscriptions(self) -> None:
        """Test getting orphaned subscriptions."""
        coordinator = WorkRedistributionCoordinator(instance_id="worker-1")

        # Add a draining peer
        now = datetime.now(UTC)
        notification = ShutdownNotification(
            instance_id="worker-2",
            intent=ShutdownIntent.GRACEFUL,
            initiated_at=now,
            expected_completion_at=now + timedelta(seconds=30),
            subscriptions=("sub-1", "sub-2"),
        )
        await coordinator.handle_peer_shutdown(notification)

        # Add a healthy peer
        heartbeat = HeartbeatMessage(
            instance_id="worker-3",
            timestamp=datetime.now(UTC),
            subscriptions=("sub-3",),
        )
        await coordinator.handle_heartbeat(heartbeat)

        orphaned = coordinator.get_orphaned_subscriptions()

        assert "worker-2" in orphaned
        assert orphaned["worker-2"] == ("sub-1", "sub-2")
        assert "worker-3" not in orphaned

    async def test_draining_peers(self) -> None:
        """Test getting draining peers."""
        coordinator = WorkRedistributionCoordinator(instance_id="worker-1")

        # Add a draining peer
        now = datetime.now(UTC)
        notification = ShutdownNotification(
            instance_id="worker-2",
            intent=ShutdownIntent.GRACEFUL,
            initiated_at=now,
            expected_completion_at=now + timedelta(seconds=30),
            subscriptions=("sub-1",),
        )
        await coordinator.handle_peer_shutdown(notification)

        draining = coordinator.draining_peers

        assert len(draining) == 1
        assert draining[0].instance_id == "worker-2"


@pytest.mark.asyncio
class TestLeadershipHandoff:
    """Tests for leadership handoff coordination."""

    async def test_initiate_leadership_handoff_without_elector(self) -> None:
        """Test leadership handoff without leader elector."""
        coordinator = WorkRedistributionCoordinator(instance_id="worker-1")

        result = await coordinator.initiate_leadership_handoff()

        assert result is False

    async def test_initiate_leadership_handoff_not_leader(self) -> None:
        """Test leadership handoff when not leader."""
        elector = InMemoryLeaderElector(_identity="worker-1")
        coordinator = WorkRedistributionCoordinator(
            instance_id="worker-1",
            leader_elector=elector,
        )

        result = await coordinator.initiate_leadership_handoff()

        assert result is False

    async def test_initiate_leadership_handoff_success(self) -> None:
        """Test successful leadership handoff."""
        elector = InMemoryLeaderElector(_identity="worker-1")
        await elector.try_acquire()

        coordinator = WorkRedistributionCoordinator(
            instance_id="worker-1",
            leader_elector=elector,
        )

        result = await coordinator.initiate_leadership_handoff()

        assert result is True
        assert elector.is_leader is False
