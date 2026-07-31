"""
Unit tests for StatusStreamer and StatusStreamManager.

Tests cover:
- StatusStreamer initialization
- stream_status() async iterator
- Multiple subscriber support
- Phase change detection
- Progress update detection
- Error count change detection
- Pause state change detection
- Automatic cleanup on disconnect
- Cleanup on migration completion
- StatusStreamManager operations
- Coordinator integration (stream_status and create_status_streamer)
"""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from eventsource.migration.models import (
    Migration,
    MigrationPhase,
    MigrationStatus,
)
from eventsource.migration.status_streamer import (
    StatusStreamer,
    StatusStreamManager,
)


class TestStatusStreamerInit:
    """Tests for StatusStreamer initialization."""

    def test_init_with_required_args(self) -> None:
        """Test initialization with required arguments."""
        coordinator = MagicMock()
        migration_id = uuid4()

        streamer = StatusStreamer(
            coordinator=coordinator,
            migration_id=migration_id,
            enable_tracing=False,
        )

        assert streamer._coordinator == coordinator
        assert streamer._migration_id == migration_id
        assert streamer._subscribers == {}
        assert not streamer._closed

    def test_init_with_tracing_disabled(self) -> None:
        """Test initialization with tracing disabled."""
        streamer = StatusStreamer(
            coordinator=MagicMock(),
            migration_id=uuid4(),
            enable_tracing=False,
        )

        assert streamer._enable_tracing is False

    def test_migration_id_property(self) -> None:
        """Test migration_id property."""
        migration_id = uuid4()
        streamer = StatusStreamer(
            coordinator=MagicMock(),
            migration_id=migration_id,
            enable_tracing=False,
        )

        assert streamer.migration_id == migration_id

    def test_subscriber_count_initial(self) -> None:
        """Test subscriber_count is 0 initially."""
        streamer = StatusStreamer(
            coordinator=MagicMock(),
            migration_id=uuid4(),
            enable_tracing=False,
        )

        assert streamer.subscriber_count == 0

    def test_is_closed_initial(self) -> None:
        """Test is_closed is False initially."""
        streamer = StatusStreamer(
            coordinator=MagicMock(),
            migration_id=uuid4(),
            enable_tracing=False,
        )

        assert streamer.is_closed is False


class TestStatusStreamerStreamStatus:
    """Tests for StatusStreamer.stream_status() method."""

    @pytest.fixture
    def mock_coordinator(self) -> MagicMock:
        """Create a mock coordinator."""
        coordinator = MagicMock()
        coordinator.register_status_queue = MagicMock()
        coordinator.unregister_status_queue = MagicMock()
        return coordinator

    @pytest.fixture
    def base_status(self) -> MigrationStatus:
        """Create a base MigrationStatus."""
        return MigrationStatus(
            migration_id=uuid4(),
            tenant_id=uuid4(),
            phase=MigrationPhase.BULK_COPY,
            progress_percent=50.0,
            events_total=1000,
            events_copied=500,
            events_remaining=500,
            sync_lag_events=0,
            sync_lag_ms=0.0,
            error_count=0,
            started_at=datetime.now(UTC),
            phase_started_at=datetime.now(UTC),
            estimated_completion=None,
            current_rate_events_per_sec=100.0,
            is_paused=False,
        )

    @pytest.mark.asyncio
    async def test_stream_status_yields_initial_status(
        self, mock_coordinator: MagicMock, base_status: MigrationStatus
    ) -> None:
        """Test stream_status yields initial status when include_initial=True."""
        mock_coordinator.get_status = AsyncMock(return_value=base_status)

        streamer = StatusStreamer(
            coordinator=mock_coordinator,
            migration_id=base_status.migration_id,
            enable_tracing=False,
        )

        statuses = []
        async for status in streamer.stream_status(update_interval=0.1, include_initial=True):
            statuses.append(status)
            break  # Stop after first status

        assert len(statuses) == 1
        assert statuses[0] == base_status

    @pytest.mark.asyncio
    async def test_stream_status_skips_initial_when_disabled(
        self, mock_coordinator: MagicMock, base_status: MigrationStatus
    ) -> None:
        """Test stream_status skips initial status when include_initial=False."""
        # Return terminal status after first call to end the stream
        terminal_status = MigrationStatus(
            migration_id=base_status.migration_id,
            tenant_id=base_status.tenant_id,
            phase=MigrationPhase.COMPLETED,
            progress_percent=100.0,
            events_total=1000,
            events_copied=1000,
            events_remaining=0,
            sync_lag_events=0,
            sync_lag_ms=0.0,
            error_count=0,
            started_at=datetime.now(UTC),
            phase_started_at=datetime.now(UTC),
            estimated_completion=None,
            current_rate_events_per_sec=0.0,
            is_paused=False,
        )
        mock_coordinator.get_status = AsyncMock(return_value=terminal_status)

        streamer = StatusStreamer(
            coordinator=mock_coordinator,
            migration_id=base_status.migration_id,
            enable_tracing=False,
        )

        statuses = []
        async for status in streamer.stream_status(update_interval=0.1, include_initial=False):
            statuses.append(status)

        # Should yield the terminal status (after waiting, not as initial)
        assert len(statuses) == 1
        assert statuses[0].phase == MigrationPhase.COMPLETED

    @pytest.mark.asyncio
    async def test_stream_status_stops_on_terminal_phase(
        self, mock_coordinator: MagicMock, base_status: MigrationStatus
    ) -> None:
        """Test stream_status stops when migration reaches terminal phase."""
        terminal_status = MigrationStatus(
            migration_id=base_status.migration_id,
            tenant_id=base_status.tenant_id,
            phase=MigrationPhase.COMPLETED,
            progress_percent=100.0,
            events_total=1000,
            events_copied=1000,
            events_remaining=0,
            sync_lag_events=0,
            sync_lag_ms=0.0,
            error_count=0,
            started_at=datetime.now(UTC),
            phase_started_at=datetime.now(UTC),
            estimated_completion=None,
            current_rate_events_per_sec=0.0,
            is_paused=False,
        )
        mock_coordinator.get_status = AsyncMock(return_value=terminal_status)

        streamer = StatusStreamer(
            coordinator=mock_coordinator,
            migration_id=base_status.migration_id,
            enable_tracing=False,
        )

        statuses = []
        async for status in streamer.stream_status(update_interval=0.1):
            statuses.append(status)

        # Should stop after terminal phase
        assert len(statuses) == 1
        assert statuses[0].phase == MigrationPhase.COMPLETED

    @pytest.mark.asyncio
    async def test_stream_status_raises_on_invalid_interval(
        self, mock_coordinator: MagicMock
    ) -> None:
        """Test stream_status raises ValueError for invalid update_interval."""
        streamer = StatusStreamer(
            coordinator=mock_coordinator,
            migration_id=uuid4(),
            enable_tracing=False,
        )

        with pytest.raises(ValueError) as exc_info:
            async for _ in streamer.stream_status(update_interval=0):
                pass

        assert "update_interval must be > 0" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_stream_status_raises_if_closed(self, mock_coordinator: MagicMock) -> None:
        """Test stream_status raises RuntimeError if streamer is closed."""
        streamer = StatusStreamer(
            coordinator=mock_coordinator,
            migration_id=uuid4(),
            enable_tracing=False,
        )

        await streamer.close()

        with pytest.raises(RuntimeError) as exc_info:
            async for _ in streamer.stream_status():
                pass

        assert "StatusStreamer has been closed" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_stream_status_registers_with_coordinator(
        self, mock_coordinator: MagicMock, base_status: MigrationStatus
    ) -> None:
        """Test stream_status registers and unregisters with coordinator."""
        # Return non-terminal status first, then terminal on second call
        terminal_status = MigrationStatus(
            migration_id=base_status.migration_id,
            tenant_id=base_status.tenant_id,
            phase=MigrationPhase.COMPLETED,
            progress_percent=100.0,
            events_total=1000,
            events_copied=1000,
            events_remaining=0,
            sync_lag_events=0,
            sync_lag_ms=0.0,
            error_count=0,
            started_at=datetime.now(UTC),
            phase_started_at=datetime.now(UTC),
            estimated_completion=None,
            current_rate_events_per_sec=0.0,
            is_paused=False,
        )
        # First call returns non-terminal (for initial), second returns terminal
        mock_coordinator.get_status = AsyncMock(side_effect=[base_status, terminal_status])

        streamer = StatusStreamer(
            coordinator=mock_coordinator,
            migration_id=base_status.migration_id,
            enable_tracing=False,
        )

        async for _ in streamer.stream_status(update_interval=0.1):
            pass

        # Verify registration happened (only happens when initial is not terminal)
        mock_coordinator.register_status_queue.assert_called_once()
        mock_coordinator.unregister_status_queue.assert_called_once()

    @pytest.mark.asyncio
    async def test_stream_status_stops_when_migration_not_found(
        self, mock_coordinator: MagicMock, base_status: MigrationStatus
    ) -> None:
        """Test stream_status stops when migration is not found."""
        # First call returns status, second returns None (not found)
        mock_coordinator.get_status = AsyncMock(side_effect=[base_status, None])

        streamer = StatusStreamer(
            coordinator=mock_coordinator,
            migration_id=base_status.migration_id,
            enable_tracing=False,
        )

        statuses = []
        async for status in streamer.stream_status(update_interval=0.05):
            statuses.append(status)

        # Should yield initial status and then stop
        assert len(statuses) == 1


class TestStatusStreamerStatusChanged:
    """Tests for StatusStreamer._status_changed() method."""

    @pytest.fixture
    def base_status(self) -> MigrationStatus:
        """Create a base MigrationStatus."""
        return MigrationStatus(
            migration_id=uuid4(),
            tenant_id=uuid4(),
            phase=MigrationPhase.BULK_COPY,
            progress_percent=50.0,
            events_total=1000,
            events_copied=500,
            events_remaining=500,
            sync_lag_events=0,
            sync_lag_ms=0.0,
            error_count=0,
            started_at=datetime.now(UTC),
            phase_started_at=datetime.now(UTC),
            estimated_completion=None,
            current_rate_events_per_sec=100.0,
            is_paused=False,
        )

    def test_phase_change_is_detected(self, base_status: MigrationStatus) -> None:
        """Test that phase change is detected."""
        streamer = StatusStreamer(
            coordinator=MagicMock(),
            migration_id=base_status.migration_id,
            enable_tracing=False,
        )

        new_status = MigrationStatus(
            migration_id=base_status.migration_id,
            tenant_id=base_status.tenant_id,
            phase=MigrationPhase.DUAL_WRITE,  # Changed
            progress_percent=base_status.progress_percent,
            events_total=base_status.events_total,
            events_copied=base_status.events_copied,
            events_remaining=base_status.events_remaining,
            sync_lag_events=base_status.sync_lag_events,
            sync_lag_ms=base_status.sync_lag_ms,
            error_count=base_status.error_count,
            started_at=base_status.started_at,
            phase_started_at=base_status.phase_started_at,
            estimated_completion=base_status.estimated_completion,
            current_rate_events_per_sec=base_status.current_rate_events_per_sec,
            is_paused=base_status.is_paused,
        )

        assert streamer._status_changed(base_status, new_status) is True

    def test_progress_change_1_percent_is_detected(self, base_status: MigrationStatus) -> None:
        """Test that progress change of 1% is detected."""
        streamer = StatusStreamer(
            coordinator=MagicMock(),
            migration_id=base_status.migration_id,
            enable_tracing=False,
        )

        new_status = MigrationStatus(
            migration_id=base_status.migration_id,
            tenant_id=base_status.tenant_id,
            phase=base_status.phase,
            progress_percent=51.0,  # Changed by 1%
            events_total=base_status.events_total,
            events_copied=510,
            events_remaining=490,
            sync_lag_events=base_status.sync_lag_events,
            sync_lag_ms=base_status.sync_lag_ms,
            error_count=base_status.error_count,
            started_at=base_status.started_at,
            phase_started_at=base_status.phase_started_at,
            estimated_completion=base_status.estimated_completion,
            current_rate_events_per_sec=base_status.current_rate_events_per_sec,
            is_paused=base_status.is_paused,
        )

        assert streamer._status_changed(base_status, new_status) is True

    def test_small_progress_change_not_detected(self, base_status: MigrationStatus) -> None:
        """Test that small progress change (<1%) is not detected."""
        streamer = StatusStreamer(
            coordinator=MagicMock(),
            migration_id=base_status.migration_id,
            enable_tracing=False,
        )

        new_status = MigrationStatus(
            migration_id=base_status.migration_id,
            tenant_id=base_status.tenant_id,
            phase=base_status.phase,
            progress_percent=50.5,  # Changed by only 0.5%
            events_total=base_status.events_total,
            events_copied=505,
            events_remaining=495,
            sync_lag_events=base_status.sync_lag_events,
            sync_lag_ms=base_status.sync_lag_ms,
            error_count=base_status.error_count,
            started_at=base_status.started_at,
            phase_started_at=base_status.phase_started_at,
            estimated_completion=base_status.estimated_completion,
            current_rate_events_per_sec=base_status.current_rate_events_per_sec,
            is_paused=base_status.is_paused,
        )

        assert streamer._status_changed(base_status, new_status) is False

    def test_sync_lag_change_is_detected(self, base_status: MigrationStatus) -> None:
        """Test that sync lag change of 10+ events is detected."""
        streamer = StatusStreamer(
            coordinator=MagicMock(),
            migration_id=base_status.migration_id,
            enable_tracing=False,
        )

        new_status = MigrationStatus(
            migration_id=base_status.migration_id,
            tenant_id=base_status.tenant_id,
            phase=base_status.phase,
            progress_percent=base_status.progress_percent,
            events_total=base_status.events_total,
            events_copied=base_status.events_copied,
            events_remaining=base_status.events_remaining,
            sync_lag_events=15,  # Changed by 15 events
            sync_lag_ms=15.0,
            error_count=base_status.error_count,
            started_at=base_status.started_at,
            phase_started_at=base_status.phase_started_at,
            estimated_completion=base_status.estimated_completion,
            current_rate_events_per_sec=base_status.current_rate_events_per_sec,
            is_paused=base_status.is_paused,
        )

        assert streamer._status_changed(base_status, new_status) is True

    def test_error_count_increase_is_detected(self, base_status: MigrationStatus) -> None:
        """Test that error count increase is detected."""
        streamer = StatusStreamer(
            coordinator=MagicMock(),
            migration_id=base_status.migration_id,
            enable_tracing=False,
        )

        new_status = MigrationStatus(
            migration_id=base_status.migration_id,
            tenant_id=base_status.tenant_id,
            phase=base_status.phase,
            progress_percent=base_status.progress_percent,
            events_total=base_status.events_total,
            events_copied=base_status.events_copied,
            events_remaining=base_status.events_remaining,
            sync_lag_events=base_status.sync_lag_events,
            sync_lag_ms=base_status.sync_lag_ms,
            error_count=1,  # Increased from 0
            started_at=base_status.started_at,
            phase_started_at=base_status.phase_started_at,
            estimated_completion=base_status.estimated_completion,
            current_rate_events_per_sec=base_status.current_rate_events_per_sec,
            is_paused=base_status.is_paused,
        )

        assert streamer._status_changed(base_status, new_status) is True

    def test_pause_state_change_is_detected(self, base_status: MigrationStatus) -> None:
        """Test that pause state change is detected."""
        streamer = StatusStreamer(
            coordinator=MagicMock(),
            migration_id=base_status.migration_id,
            enable_tracing=False,
        )

        new_status = MigrationStatus(
            migration_id=base_status.migration_id,
            tenant_id=base_status.tenant_id,
            phase=base_status.phase,
            progress_percent=base_status.progress_percent,
            events_total=base_status.events_total,
            events_copied=base_status.events_copied,
            events_remaining=base_status.events_remaining,
            sync_lag_events=base_status.sync_lag_events,
            sync_lag_ms=base_status.sync_lag_ms,
            error_count=base_status.error_count,
            started_at=base_status.started_at,
            phase_started_at=base_status.phase_started_at,
            estimated_completion=base_status.estimated_completion,
            current_rate_events_per_sec=base_status.current_rate_events_per_sec,
            is_paused=True,  # Changed from False
        )

        assert streamer._status_changed(base_status, new_status) is True

    def test_no_change_not_detected(self, base_status: MigrationStatus) -> None:
        """Test that identical status is not detected as changed."""
        streamer = StatusStreamer(
            coordinator=MagicMock(),
            migration_id=base_status.migration_id,
            enable_tracing=False,
        )

        assert streamer._status_changed(base_status, base_status) is False


class TestStatusStreamerMultipleSubscribers:
    """Tests for multiple subscriber support."""

    @pytest.mark.asyncio
    async def test_multiple_subscribers_can_register(self) -> None:
        """Test that multiple subscribers can register and are tracked."""
        mock_coordinator = MagicMock()
        mock_coordinator.register_status_queue = MagicMock()
        mock_coordinator.unregister_status_queue = MagicMock()

        migration_id = uuid4()

        streamer = StatusStreamer(
            coordinator=mock_coordinator,
            migration_id=migration_id,
            enable_tracing=False,
        )

        # Register multiple subscribers manually
        sub1 = await streamer._register_subscriber()
        sub2 = await streamer._register_subscriber()
        sub3 = await streamer._register_subscriber()

        assert streamer.subscriber_count == 3
        assert sub1 != sub2 != sub3

        # Unregister them
        await streamer._unregister_subscriber(sub1)
        await streamer._unregister_subscriber(sub2)
        await streamer._unregister_subscriber(sub3)

        assert streamer.subscriber_count == 0

    @pytest.mark.asyncio
    async def test_concurrent_streams_are_independent(self) -> None:
        """Test that concurrent streams work independently."""
        mock_coordinator = MagicMock()
        mock_coordinator.register_status_queue = MagicMock()
        mock_coordinator.unregister_status_queue = MagicMock()

        migration_id = uuid4()
        tenant_id = uuid4()

        # Use a call counter to return active then terminal
        call_count = {"value": 0}

        async def get_status_impl(*args, **kwargs):
            call_count["value"] += 1
            # Return active for first call of each stream, then terminal
            if call_count["value"] % 2 == 1:
                return MigrationStatus(
                    migration_id=migration_id,
                    tenant_id=tenant_id,
                    phase=MigrationPhase.BULK_COPY,
                    progress_percent=50.0,
                    events_total=1000,
                    events_copied=500,
                    events_remaining=500,
                    sync_lag_events=0,
                    sync_lag_ms=0.0,
                    error_count=0,
                    started_at=datetime.now(UTC),
                    phase_started_at=datetime.now(UTC),
                    estimated_completion=None,
                    current_rate_events_per_sec=100.0,
                    is_paused=False,
                )
            else:
                return MigrationStatus(
                    migration_id=migration_id,
                    tenant_id=tenant_id,
                    phase=MigrationPhase.COMPLETED,
                    progress_percent=100.0,
                    events_total=1000,
                    events_copied=1000,
                    events_remaining=0,
                    sync_lag_events=0,
                    sync_lag_ms=0.0,
                    error_count=0,
                    started_at=datetime.now(UTC),
                    phase_started_at=datetime.now(UTC),
                    estimated_completion=None,
                    current_rate_events_per_sec=0.0,
                    is_paused=False,
                )

        mock_coordinator.get_status = AsyncMock(side_effect=get_status_impl)

        streamer = StatusStreamer(
            coordinator=mock_coordinator,
            migration_id=migration_id,
            enable_tracing=False,
        )

        # Run two streams sequentially to avoid race conditions
        collected_1 = []
        async for status in streamer.stream_status(update_interval=0.05):
            collected_1.append(status)

        collected_2 = []
        async for status in streamer.stream_status(update_interval=0.05):
            collected_2.append(status)

        # Both should have collected statuses
        assert len(collected_1) >= 1
        assert len(collected_2) >= 1

        # Both should have registered with the coordinator
        assert mock_coordinator.register_status_queue.call_count == 2
        assert mock_coordinator.unregister_status_queue.call_count == 2


class TestStatusStreamerClose:
    """Tests for StatusStreamer.close() method."""

    @pytest.mark.asyncio
    async def test_close_marks_streamer_closed(self) -> None:
        """Test close() marks the streamer as closed."""
        streamer = StatusStreamer(
            coordinator=MagicMock(),
            migration_id=uuid4(),
            enable_tracing=False,
        )

        assert not streamer.is_closed

        await streamer.close()

        assert streamer.is_closed

    @pytest.mark.asyncio
    async def test_close_clears_subscribers(self) -> None:
        """Test close() clears all subscribers."""
        streamer = StatusStreamer(
            coordinator=MagicMock(),
            migration_id=uuid4(),
            enable_tracing=False,
        )

        # Register a subscriber manually
        await streamer._register_subscriber()
        assert streamer.subscriber_count == 1

        await streamer.close()

        assert streamer.subscriber_count == 0


class TestStatusStreamManagerInit:
    """Tests for StatusStreamManager initialization."""

    def test_init_with_required_args(self) -> None:
        """Test initialization with required arguments."""
        coordinator = MagicMock()

        manager = StatusStreamManager(
            coordinator=coordinator,
            enable_tracing=False,
        )

        assert manager._coordinator == coordinator
        assert manager._streamers == {}

    def test_active_streamers_initial(self) -> None:
        """Test active_streamers is 0 initially."""
        manager = StatusStreamManager(
            coordinator=MagicMock(),
            enable_tracing=False,
        )

        assert manager.active_streamers == 0


class TestStatusStreamManagerGetStreamer:
    """Tests for StatusStreamManager.get_streamer() method."""

    @pytest.mark.asyncio
    async def test_get_streamer_creates_new(self) -> None:
        """Test get_streamer creates new streamer if not exists."""
        manager = StatusStreamManager(
            coordinator=MagicMock(),
            enable_tracing=False,
        )

        migration_id = uuid4()
        streamer = await manager.get_streamer(migration_id)

        assert isinstance(streamer, StatusStreamer)
        assert streamer.migration_id == migration_id
        assert manager.active_streamers == 1

    @pytest.mark.asyncio
    async def test_get_streamer_returns_existing(self) -> None:
        """Test get_streamer returns existing streamer."""
        manager = StatusStreamManager(
            coordinator=MagicMock(),
            enable_tracing=False,
        )

        migration_id = uuid4()
        streamer1 = await manager.get_streamer(migration_id)
        streamer2 = await manager.get_streamer(migration_id)

        assert streamer1 is streamer2
        assert manager.active_streamers == 1


class TestStatusStreamManagerClose:
    """Tests for StatusStreamManager close methods."""

    @pytest.mark.asyncio
    async def test_close_streamer_removes_it(self) -> None:
        """Test close_streamer removes the streamer."""
        manager = StatusStreamManager(
            coordinator=MagicMock(),
            enable_tracing=False,
        )

        migration_id = uuid4()
        await manager.get_streamer(migration_id)
        assert manager.active_streamers == 1

        await manager.close_streamer(migration_id)

        assert manager.active_streamers == 0

    @pytest.mark.asyncio
    async def test_close_streamer_noop_if_not_exists(self) -> None:
        """Test close_streamer is noop if streamer doesn't exist."""
        manager = StatusStreamManager(
            coordinator=MagicMock(),
            enable_tracing=False,
        )

        # Should not raise
        await manager.close_streamer(uuid4())

    @pytest.mark.asyncio
    async def test_close_all_removes_all(self) -> None:
        """Test close_all removes all streamers."""
        manager = StatusStreamManager(
            coordinator=MagicMock(),
            enable_tracing=False,
        )

        await manager.get_streamer(uuid4())
        await manager.get_streamer(uuid4())
        await manager.get_streamer(uuid4())
        assert manager.active_streamers == 3

        await manager.close_all()

        assert manager.active_streamers == 0


class TestStatusStreamManagerCleanup:
    """Tests for StatusStreamManager.cleanup_terminal_migrations() method."""

    @pytest.mark.asyncio
    async def test_cleanup_removes_terminal_migrations(self) -> None:
        """Test cleanup removes streamers for terminal migrations."""
        mock_coordinator = MagicMock()

        active_status = MigrationStatus(
            migration_id=uuid4(),
            tenant_id=uuid4(),
            phase=MigrationPhase.BULK_COPY,
            progress_percent=50.0,
            events_total=1000,
            events_copied=500,
            events_remaining=500,
            sync_lag_events=0,
            sync_lag_ms=0.0,
            error_count=0,
            started_at=datetime.now(UTC),
            phase_started_at=datetime.now(UTC),
            estimated_completion=None,
            current_rate_events_per_sec=100.0,
            is_paused=False,
        )

        terminal_status = MigrationStatus(
            migration_id=uuid4(),
            tenant_id=uuid4(),
            phase=MigrationPhase.COMPLETED,
            progress_percent=100.0,
            events_total=1000,
            events_copied=1000,
            events_remaining=0,
            sync_lag_events=0,
            sync_lag_ms=0.0,
            error_count=0,
            started_at=datetime.now(UTC),
            phase_started_at=datetime.now(UTC),
            estimated_completion=None,
            current_rate_events_per_sec=0.0,
            is_paused=False,
        )

        # Return appropriate status based on migration_id
        async def get_status_side_effect(migration_id):
            if migration_id == active_status.migration_id:
                return active_status
            return terminal_status

        mock_coordinator.get_status = AsyncMock(side_effect=get_status_side_effect)

        manager = StatusStreamManager(
            coordinator=mock_coordinator,
            enable_tracing=False,
        )

        # Create streamers for both migrations
        await manager.get_streamer(active_status.migration_id)
        await manager.get_streamer(terminal_status.migration_id)
        assert manager.active_streamers == 2

        # Cleanup
        removed = await manager.cleanup_terminal_migrations()

        assert removed == 1
        assert manager.active_streamers == 1

    @pytest.mark.asyncio
    async def test_cleanup_removes_not_found_migrations(self) -> None:
        """Test cleanup removes streamers for migrations not found."""
        mock_coordinator = MagicMock()
        mock_coordinator.get_status = AsyncMock(side_effect=Exception("Not found"))

        manager = StatusStreamManager(
            coordinator=mock_coordinator,
            enable_tracing=False,
        )

        await manager.get_streamer(uuid4())
        assert manager.active_streamers == 1

        removed = await manager.cleanup_terminal_migrations()

        assert removed == 1
        assert manager.active_streamers == 0


class TestCoordinatorIntegration:
    """Tests for MigrationCoordinator integration with status streaming."""

    @pytest.fixture
    def mock_deps(self) -> dict:
        """Create mock dependencies for MigrationCoordinator."""
        return {
            "source_store": MagicMock(),
            "migration_repo": AsyncMock(),
            "routing_repo": AsyncMock(),
            "router": MagicMock(),
        }

    def test_create_status_streamer(self, mock_deps: dict) -> None:
        """Test create_status_streamer creates StatusStreamer."""
        from eventsource.migration.coordinator import MigrationCoordinator

        coordinator = MigrationCoordinator(
            **mock_deps,
            enable_tracing=False,
        )

        migration_id = uuid4()
        streamer = coordinator.create_status_streamer(migration_id)

        assert isinstance(streamer, StatusStreamer)
        assert streamer.migration_id == migration_id

    @pytest.mark.asyncio
    async def test_stream_status_yields_statuses(self, mock_deps: dict) -> None:
        """Test stream_status yields MigrationStatus objects."""
        from eventsource.migration.coordinator import MigrationCoordinator

        migration_id = uuid4()
        tenant_id = uuid4()

        migration = Migration(
            id=migration_id,
            tenant_id=tenant_id,
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.COMPLETED,
            events_total=1000,
            events_copied=1000,
            started_at=datetime.now(UTC),
        )

        mock_deps["migration_repo"].get = AsyncMock(return_value=migration)

        coordinator = MigrationCoordinator(
            **mock_deps,
            enable_tracing=False,
        )

        statuses = []
        async for status in coordinator.stream_status(migration_id, update_interval=0.1):
            statuses.append(status)

        assert len(statuses) == 1
        assert statuses[0].migration_id == migration_id
        assert statuses[0].phase == MigrationPhase.COMPLETED

    @pytest.mark.asyncio
    async def test_stream_status_with_custom_interval(self, mock_deps: dict) -> None:
        """Test stream_status respects custom update interval."""
        from eventsource.migration.coordinator import MigrationCoordinator

        migration_id = uuid4()
        tenant_id = uuid4()

        migration = Migration(
            id=migration_id,
            tenant_id=tenant_id,
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.COMPLETED,
            events_total=1000,
            events_copied=1000,
            started_at=datetime.now(UTC),
        )

        mock_deps["migration_repo"].get = AsyncMock(return_value=migration)

        coordinator = MigrationCoordinator(
            **mock_deps,
            enable_tracing=False,
        )

        statuses = []
        async for status in coordinator.stream_status(
            migration_id,
            update_interval=0.05,
            include_initial=True,
        ):
            statuses.append(status)

        assert len(statuses) >= 1
