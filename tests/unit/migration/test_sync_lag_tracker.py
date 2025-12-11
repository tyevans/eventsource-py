"""
Unit tests for SyncLagTracker.

Tests cover:
- SyncLagTracker initialization
- Lag calculation between source and target stores
- Convergence detection (is_converged, is_sync_ready)
- Lag statistics (average, max, min)
- Convergence trend detection
- Manual lag recording
- Sample history management
- Tracing integration
"""

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest

from eventsource.migration.models import MigrationConfig, SyncLag
from eventsource.migration.sync_lag_tracker import (
    LagSample,
    LagStats,
    SyncLagTracker,
)

# =============================================================================
# Test Fixtures
# =============================================================================


def create_mock_store(global_position: int = 100) -> MagicMock:
    """Create a mock event store with proper async support."""
    store = MagicMock()
    store.get_global_position = AsyncMock(return_value=global_position)
    return store


@pytest.fixture
def source_store() -> MagicMock:
    """Create a mock source event store at position 100."""
    return create_mock_store(global_position=100)


@pytest.fixture
def target_store() -> MagicMock:
    """Create a mock target event store at position 50."""
    return create_mock_store(global_position=50)


@pytest.fixture
def synced_target_store() -> MagicMock:
    """Create a mock target event store that matches source position."""
    return create_mock_store(global_position=100)


@pytest.fixture
def config() -> MigrationConfig:
    """Create a migration config with default thresholds."""
    return MigrationConfig(cutover_max_lag_events=100)


@pytest.fixture
def strict_config() -> MigrationConfig:
    """Create a migration config with strict threshold."""
    return MigrationConfig(cutover_max_lag_events=10)


@pytest.fixture
def tracker(
    source_store: MagicMock,
    target_store: MagicMock,
    config: MigrationConfig,
) -> SyncLagTracker:
    """Create a SyncLagTracker with mock dependencies."""
    return SyncLagTracker(
        source_store=source_store,
        target_store=target_store,
        config=config,
        enable_tracing=False,
    )


# =============================================================================
# Test LagSample Dataclass
# =============================================================================


class TestLagSample:
    """Tests for LagSample dataclass."""

    def test_lag_sample_creation(self) -> None:
        """Test creating a LagSample."""
        lag = SyncLag(
            events=50,
            source_position=100,
            target_position=50,
            timestamp=datetime.now(UTC),
        )
        sample = LagSample(lag=lag)

        assert sample.lag == lag
        assert sample.sampled_at is not None

    def test_lag_sample_with_custom_timestamp(self) -> None:
        """Test creating a LagSample with custom timestamp."""
        lag = SyncLag(
            events=50,
            source_position=100,
            target_position=50,
            timestamp=datetime.now(UTC),
        )
        custom_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
        sample = LagSample(lag=lag, sampled_at=custom_time)

        assert sample.sampled_at == custom_time


# =============================================================================
# Test LagStats Dataclass
# =============================================================================


class TestLagStats:
    """Tests for LagStats dataclass."""

    def test_lag_stats_creation(self) -> None:
        """Test creating LagStats."""
        now = datetime.now(UTC)
        stats = LagStats(
            current_lag=50,
            average_lag=45.5,
            max_lag=100,
            min_lag=10,
            sample_count=20,
            first_sample_at=now - timedelta(hours=1),
            last_sample_at=now,
            is_converging=True,
        )

        assert stats.current_lag == 50
        assert stats.average_lag == 45.5
        assert stats.max_lag == 100
        assert stats.min_lag == 10
        assert stats.sample_count == 20
        assert stats.is_converging is True

    def test_lag_stats_to_dict(self) -> None:
        """Test converting LagStats to dictionary."""
        now = datetime.now(UTC)
        stats = LagStats(
            current_lag=50,
            average_lag=45.5,
            max_lag=100,
            min_lag=10,
            sample_count=20,
            first_sample_at=now,
            last_sample_at=now,
            is_converging=False,
        )

        result = stats.to_dict()

        assert result["current_lag"] == 50
        assert result["average_lag"] == 45.5
        assert result["max_lag"] == 100
        assert result["min_lag"] == 10
        assert result["sample_count"] == 20
        assert result["is_converging"] is False
        assert result["first_sample_at"] == now.isoformat()
        assert result["last_sample_at"] == now.isoformat()

    def test_lag_stats_to_dict_with_none_timestamps(self) -> None:
        """Test converting LagStats with None timestamps."""
        stats = LagStats(
            current_lag=0,
            average_lag=0.0,
            max_lag=0,
            min_lag=0,
            sample_count=0,
            first_sample_at=None,
            last_sample_at=None,
        )

        result = stats.to_dict()

        assert result["first_sample_at"] is None
        assert result["last_sample_at"] is None


# =============================================================================
# Test SyncLagTracker Initialization
# =============================================================================


class TestSyncLagTrackerInit:
    """Tests for SyncLagTracker initialization."""

    def test_init_with_defaults(
        self,
        source_store: MagicMock,
        target_store: MagicMock,
    ) -> None:
        """Test initialization with default parameters."""
        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
        )

        assert tracker.source_store is source_store
        assert tracker.target_store is target_store
        assert tracker.config is not None
        assert tracker.tenant_id is None
        assert tracker.current_lag is None
        assert tracker._max_sample_history == 100

    def test_init_with_config(
        self,
        source_store: MagicMock,
        target_store: MagicMock,
        strict_config: MigrationConfig,
    ) -> None:
        """Test initialization with custom config."""
        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            config=strict_config,
        )

        assert tracker.config is strict_config
        assert tracker.sync_threshold == 10

    def test_init_with_tenant_id(
        self,
        source_store: MagicMock,
        target_store: MagicMock,
    ) -> None:
        """Test initialization with tenant ID."""
        from uuid import uuid4

        tenant_id = uuid4()
        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
        )

        assert tracker.tenant_id == tenant_id

    def test_init_with_custom_sample_history(
        self,
        source_store: MagicMock,
        target_store: MagicMock,
    ) -> None:
        """Test initialization with custom max sample history."""
        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            max_sample_history=50,
        )

        assert tracker._max_sample_history == 50

    def test_init_with_tracing_disabled(
        self,
        source_store: MagicMock,
        target_store: MagicMock,
    ) -> None:
        """Test initialization with tracing disabled."""
        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            enable_tracing=False,
        )

        assert tracker._enable_tracing is False

    def test_properties(
        self,
        tracker: SyncLagTracker,
        source_store: MagicMock,
        target_store: MagicMock,
        config: MigrationConfig,
    ) -> None:
        """Test property accessors."""
        assert tracker.source_store is source_store
        assert tracker.target_store is target_store
        assert tracker.config is config
        assert tracker.sync_threshold == 100


# =============================================================================
# Test Lag Calculation
# =============================================================================


class TestCalculateLag:
    """Tests for calculate_lag method."""

    @pytest.mark.asyncio
    async def test_calculate_lag_basic(
        self,
        tracker: SyncLagTracker,
        source_store: MagicMock,
        target_store: MagicMock,
    ) -> None:
        """Test basic lag calculation."""
        lag = await tracker.calculate_lag()

        assert lag.events == 50  # 100 - 50
        assert lag.source_position == 100
        assert lag.target_position == 50
        assert lag.timestamp is not None
        source_store.get_global_position.assert_called_once()
        target_store.get_global_position.assert_called_once()

    @pytest.mark.asyncio
    async def test_calculate_lag_updates_current(
        self,
        tracker: SyncLagTracker,
    ) -> None:
        """Test that calculate_lag updates current_lag property."""
        assert tracker.current_lag is None

        lag = await tracker.calculate_lag()

        assert tracker.current_lag is lag

    @pytest.mark.asyncio
    async def test_calculate_lag_adds_sample(
        self,
        tracker: SyncLagTracker,
    ) -> None:
        """Test that calculate_lag adds to sample history."""
        assert len(tracker._lag_samples) == 0

        await tracker.calculate_lag()

        assert len(tracker._lag_samples) == 1

    @pytest.mark.asyncio
    async def test_calculate_lag_zero_lag(
        self,
        source_store: MagicMock,
        synced_target_store: MagicMock,
        config: MigrationConfig,
    ) -> None:
        """Test calculating lag when stores are synced."""
        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=synced_target_store,
            config=config,
            enable_tracing=False,
        )

        lag = await tracker.calculate_lag()

        assert lag.events == 0
        assert lag.is_converged is True

    @pytest.mark.asyncio
    async def test_calculate_lag_target_ahead(
        self,
        source_store: MagicMock,
        config: MigrationConfig,
    ) -> None:
        """Test that lag is zero when target is ahead (edge case)."""
        # Target somehow has more events (shouldn't happen but handle gracefully)
        target_store = create_mock_store(global_position=150)
        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            config=config,
            enable_tracing=False,
        )

        lag = await tracker.calculate_lag()

        assert lag.events == 0  # Clamped to zero
        assert lag.source_position == 100
        assert lag.target_position == 150

    @pytest.mark.asyncio
    async def test_calculate_lag_multiple_calls(
        self,
        source_store: MagicMock,
        target_store: MagicMock,
        config: MigrationConfig,
    ) -> None:
        """Test multiple lag calculations."""
        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            config=config,
            enable_tracing=False,
        )

        # First call: 100 - 50 = 50
        await tracker.calculate_lag()

        # Update positions to simulate sync progress
        target_store.get_global_position.return_value = 80
        lag2 = await tracker.calculate_lag()

        assert lag2.events == 20  # 100 - 80
        assert len(tracker._lag_samples) == 2


# =============================================================================
# Test Convergence Detection
# =============================================================================


class TestConvergenceDetection:
    """Tests for convergence detection methods."""

    @pytest.mark.asyncio
    async def test_is_converged_no_measurement(
        self,
        tracker: SyncLagTracker,
    ) -> None:
        """Test is_converged returns False when no measurement taken."""
        assert tracker.is_converged() is False

    @pytest.mark.asyncio
    async def test_is_converged_within_threshold(
        self,
        source_store: MagicMock,
        config: MigrationConfig,
    ) -> None:
        """Test is_converged returns True when within threshold."""
        target_store = create_mock_store(global_position=50)
        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            config=config,  # threshold = 100
            enable_tracing=False,
        )

        await tracker.calculate_lag()  # lag = 50

        assert tracker.is_converged() is True

    @pytest.mark.asyncio
    async def test_is_converged_exceeds_threshold(
        self,
        source_store: MagicMock,
        strict_config: MigrationConfig,
    ) -> None:
        """Test is_converged returns False when exceeding threshold."""
        target_store = create_mock_store(global_position=50)
        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            config=strict_config,  # threshold = 10
            enable_tracing=False,
        )

        await tracker.calculate_lag()  # lag = 50

        assert tracker.is_converged() is False

    @pytest.mark.asyncio
    async def test_is_converged_custom_threshold(
        self,
        tracker: SyncLagTracker,
    ) -> None:
        """Test is_converged with custom max_lag parameter."""
        await tracker.calculate_lag()  # lag = 50

        assert tracker.is_converged(max_lag=100) is True
        assert tracker.is_converged(max_lag=50) is True
        assert tracker.is_converged(max_lag=49) is False
        assert tracker.is_converged(max_lag=10) is False

    @pytest.mark.asyncio
    async def test_is_sync_ready_no_measurement(
        self,
        tracker: SyncLagTracker,
    ) -> None:
        """Test is_sync_ready returns False when no measurement taken."""
        assert tracker.is_sync_ready() is False

    @pytest.mark.asyncio
    async def test_is_sync_ready_within_threshold(
        self,
        source_store: MagicMock,
        config: MigrationConfig,
    ) -> None:
        """Test is_sync_ready returns True when within threshold."""
        target_store = create_mock_store(global_position=50)
        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            config=config,  # threshold = 100
            enable_tracing=False,
        )

        await tracker.calculate_lag()  # lag = 50

        assert tracker.is_sync_ready() is True

    @pytest.mark.asyncio
    async def test_is_sync_ready_exceeds_threshold(
        self,
        source_store: MagicMock,
        strict_config: MigrationConfig,
    ) -> None:
        """Test is_sync_ready returns False when exceeding threshold."""
        target_store = create_mock_store(global_position=50)
        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            config=strict_config,  # threshold = 10
            enable_tracing=False,
        )

        await tracker.calculate_lag()  # lag = 50

        assert tracker.is_sync_ready() is False

    @pytest.mark.asyncio
    async def test_is_fully_converged_no_measurement(
        self,
        tracker: SyncLagTracker,
    ) -> None:
        """Test is_fully_converged returns False when no measurement taken."""
        assert tracker.is_fully_converged() is False

    @pytest.mark.asyncio
    async def test_is_fully_converged_with_lag(
        self,
        tracker: SyncLagTracker,
    ) -> None:
        """Test is_fully_converged returns False when there is lag."""
        await tracker.calculate_lag()  # lag = 50

        assert tracker.is_fully_converged() is False

    @pytest.mark.asyncio
    async def test_is_fully_converged_zero_lag(
        self,
        source_store: MagicMock,
        synced_target_store: MagicMock,
        config: MigrationConfig,
    ) -> None:
        """Test is_fully_converged returns True when lag is zero."""
        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=synced_target_store,
            config=config,
            enable_tracing=False,
        )

        await tracker.calculate_lag()  # lag = 0

        assert tracker.is_fully_converged() is True


# =============================================================================
# Test Lag Statistics
# =============================================================================


class TestTrackerLagStats:
    """Tests for lag statistics methods."""

    def test_get_lag_stats_no_samples(
        self,
        tracker: SyncLagTracker,
    ) -> None:
        """Test get_lag_stats with no samples."""
        stats = tracker.get_lag_stats()

        assert stats.current_lag == 0
        assert stats.average_lag == 0.0
        assert stats.max_lag == 0
        assert stats.min_lag == 0
        assert stats.sample_count == 0
        assert stats.first_sample_at is None
        assert stats.last_sample_at is None
        assert stats.is_converging is False

    @pytest.mark.asyncio
    async def test_get_lag_stats_single_sample(
        self,
        tracker: SyncLagTracker,
    ) -> None:
        """Test get_lag_stats with single sample."""
        await tracker.calculate_lag()  # lag = 50

        stats = tracker.get_lag_stats()

        assert stats.current_lag == 50
        assert stats.average_lag == 50.0
        assert stats.max_lag == 50
        assert stats.min_lag == 50
        assert stats.sample_count == 1
        assert stats.first_sample_at is not None
        assert stats.last_sample_at is not None

    @pytest.mark.asyncio
    async def test_get_lag_stats_multiple_samples(
        self,
        source_store: MagicMock,
        target_store: MagicMock,
        config: MigrationConfig,
    ) -> None:
        """Test get_lag_stats with multiple samples."""
        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            config=config,
            enable_tracing=False,
        )

        # First measurement: 100 - 50 = 50
        await tracker.calculate_lag()

        # Second measurement: 100 - 70 = 30
        target_store.get_global_position.return_value = 70
        await tracker.calculate_lag()

        # Third measurement: 100 - 90 = 10
        target_store.get_global_position.return_value = 90
        await tracker.calculate_lag()

        stats = tracker.get_lag_stats()

        assert stats.current_lag == 10
        assert stats.average_lag == 30.0  # (50 + 30 + 10) / 3
        assert stats.max_lag == 50
        assert stats.min_lag == 10
        assert stats.sample_count == 3

    @pytest.mark.asyncio
    async def test_convergence_trend_detection(
        self,
        source_store: MagicMock,
        target_store: MagicMock,
        config: MigrationConfig,
    ) -> None:
        """Test that convergence trend is detected correctly."""
        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            config=config,
            enable_tracing=False,
        )

        # Create decreasing lag pattern (converging)
        positions = [50, 60, 70, 80, 90, 95]  # target positions
        for pos in positions:
            target_store.get_global_position.return_value = pos
            await tracker.calculate_lag()

        stats = tracker.get_lag_stats()

        assert stats.is_converging is True

    @pytest.mark.asyncio
    async def test_non_convergence_trend_detection(
        self,
        source_store: MagicMock,
        target_store: MagicMock,
        config: MigrationConfig,
    ) -> None:
        """Test that non-convergence (diverging) is detected."""
        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            config=config,
            enable_tracing=False,
        )

        # Create increasing lag pattern (diverging)
        # Source stays at 100, target decreases
        positions = [90, 80, 70, 60, 50, 40]  # target positions
        for pos in positions:
            target_store.get_global_position.return_value = pos
            await tracker.calculate_lag()

        stats = tracker.get_lag_stats()

        assert stats.is_converging is False


# =============================================================================
# Test Sample History Management
# =============================================================================


class TestSampleHistoryManagement:
    """Tests for sample history management."""

    @pytest.mark.asyncio
    async def test_get_sample_history(
        self,
        source_store: MagicMock,
        target_store: MagicMock,
        config: MigrationConfig,
    ) -> None:
        """Test getting sample history."""
        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            config=config,
            enable_tracing=False,
        )

        await tracker.calculate_lag()
        target_store.get_global_position.return_value = 70
        await tracker.calculate_lag()

        history = tracker.get_sample_history()

        assert len(history) == 2
        assert all(isinstance(lag, SyncLag) for lag in history)
        assert history[0].events == 50
        assert history[1].events == 30

    def test_clear_history(
        self,
        tracker: SyncLagTracker,
    ) -> None:
        """Test clearing sample history."""
        # Add some samples manually
        lag = SyncLag(
            events=50,
            source_position=100,
            target_position=50,
            timestamp=datetime.now(UTC),
        )
        tracker.record_lag(lag)

        assert len(tracker._lag_samples) == 1
        assert tracker.current_lag is not None

        cleared = tracker.clear_history()

        assert cleared == 1
        assert len(tracker._lag_samples) == 0
        assert tracker.current_lag is None

    @pytest.mark.asyncio
    async def test_sample_history_max_size(
        self,
        source_store: MagicMock,
        target_store: MagicMock,
        config: MigrationConfig,
    ) -> None:
        """Test that sample history respects max size."""
        max_history = 5
        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            config=config,
            enable_tracing=False,
            max_sample_history=max_history,
        )

        # Add more samples than max
        for i in range(max_history + 3):
            target_store.get_global_position.return_value = 50 + i
            await tracker.calculate_lag()

        assert len(tracker._lag_samples) == max_history


# =============================================================================
# Test Manual Lag Recording
# =============================================================================


class TestManualLagRecording:
    """Tests for manual lag recording."""

    def test_record_lag(
        self,
        tracker: SyncLagTracker,
    ) -> None:
        """Test manually recording a lag measurement."""
        lag = SyncLag(
            events=25,
            source_position=100,
            target_position=75,
            timestamp=datetime.now(UTC),
        )

        tracker.record_lag(lag)

        assert tracker.current_lag is lag
        assert len(tracker._lag_samples) == 1
        assert tracker._lag_samples[0].lag is lag

    def test_record_lag_multiple(
        self,
        tracker: SyncLagTracker,
    ) -> None:
        """Test recording multiple lag measurements."""
        for i in range(3):
            lag = SyncLag(
                events=50 - (i * 10),
                source_position=100,
                target_position=50 + (i * 10),
                timestamp=datetime.now(UTC),
            )
            tracker.record_lag(lag)

        assert len(tracker._lag_samples) == 3
        assert tracker.current_lag.events == 30  # Last recorded

    def test_record_lag_updates_stats(
        self,
        tracker: SyncLagTracker,
    ) -> None:
        """Test that recorded lags update statistics."""
        lags = [50, 40, 30, 20, 10]
        for lag_events in lags:
            lag = SyncLag(
                events=lag_events,
                source_position=100,
                target_position=100 - lag_events,
                timestamp=datetime.now(UTC),
            )
            tracker.record_lag(lag)

        stats = tracker.get_lag_stats()

        assert stats.current_lag == 10
        assert stats.average_lag == 30.0  # (50+40+30+20+10) / 5
        assert stats.max_lag == 50
        assert stats.min_lag == 10


# =============================================================================
# Test Edge Cases
# =============================================================================


class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""

    @pytest.mark.asyncio
    async def test_empty_stores(
        self,
        config: MigrationConfig,
    ) -> None:
        """Test with empty stores (both at position 0)."""
        source_store = create_mock_store(global_position=0)
        target_store = create_mock_store(global_position=0)
        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            config=config,
            enable_tracing=False,
        )

        lag = await tracker.calculate_lag()

        assert lag.events == 0
        assert tracker.is_sync_ready() is True
        assert tracker.is_fully_converged() is True

    @pytest.mark.asyncio
    async def test_large_lag(
        self,
        config: MigrationConfig,
    ) -> None:
        """Test with very large lag."""
        source_store = create_mock_store(global_position=1_000_000)
        target_store = create_mock_store(global_position=0)
        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            config=config,
            enable_tracing=False,
        )

        lag = await tracker.calculate_lag()

        assert lag.events == 1_000_000
        assert tracker.is_sync_ready() is False
        assert tracker.is_converged(max_lag=1_000_000) is True

    @pytest.mark.asyncio
    async def test_exact_threshold(
        self,
        source_store: MagicMock,
        config: MigrationConfig,
    ) -> None:
        """Test when lag is exactly at threshold."""
        # Config threshold is 100, set lag to exactly 100
        target_store = create_mock_store(global_position=0)
        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            config=config,  # threshold = 100
            enable_tracing=False,
        )

        await tracker.calculate_lag()  # lag = 100

        assert tracker.is_converged() is True  # <= threshold
        assert tracker.is_sync_ready() is True

    def test_convergence_with_few_samples(
        self,
        tracker: SyncLagTracker,
    ) -> None:
        """Test convergence detection with very few samples."""
        # Need at least 4 samples for convergence detection
        for i in range(3):
            lag = SyncLag(
                events=50 - (i * 10),
                source_position=100,
                target_position=50 + (i * 10),
                timestamp=datetime.now(UTC),
            )
            tracker.record_lag(lag)

        stats = tracker.get_lag_stats()

        # With fewer than 4 samples, should not detect convergence
        assert stats.is_converging is False


# =============================================================================
# Test with Tenant ID
# =============================================================================


class TestWithTenantId:
    """Tests for tracker with tenant ID."""

    @pytest.mark.asyncio
    async def test_tracker_with_tenant_id(
        self,
        source_store: MagicMock,
        target_store: MagicMock,
        config: MigrationConfig,
    ) -> None:
        """Test tracker operations with tenant ID set."""
        from uuid import uuid4

        tenant_id = uuid4()
        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            config=config,
            tenant_id=tenant_id,
            enable_tracing=False,
        )

        lag = await tracker.calculate_lag()

        assert tracker.tenant_id == tenant_id
        assert lag.events == 50


# =============================================================================
# Test Integration Scenarios
# =============================================================================


class TestIntegrationScenarios:
    """Tests for realistic integration scenarios."""

    @pytest.mark.asyncio
    async def test_typical_migration_scenario(
        self,
        source_store: MagicMock,
        config: MigrationConfig,
    ) -> None:
        """Test a typical migration scenario with decreasing lag."""
        target_store = create_mock_store(global_position=0)
        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            config=config,
            enable_tracing=False,
        )

        # Simulate sync progress
        target_positions = [0, 20, 40, 60, 80, 90, 95, 99, 100]

        for pos in target_positions:
            target_store.get_global_position.return_value = pos
            await tracker.calculate_lag()

        # Final state
        assert tracker.is_sync_ready() is True
        assert tracker.is_fully_converged() is True

        stats = tracker.get_lag_stats()
        assert stats.is_converging is True
        assert stats.current_lag == 0
        assert stats.max_lag == 100
        assert stats.min_lag == 0

    @pytest.mark.asyncio
    async def test_fluctuating_lag_scenario(
        self,
        source_store: MagicMock,
        config: MigrationConfig,
    ) -> None:
        """Test scenario where lag fluctuates but generally improves."""
        target_store = create_mock_store(global_position=0)
        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            config=config,
            enable_tracing=False,
        )

        # Fluctuating but improving (source also moving)
        # Source: 100, Target moves toward it
        target_positions = [0, 30, 25, 50, 45, 70, 65, 90, 95, 100]

        for pos in target_positions:
            target_store.get_global_position.return_value = pos
            await tracker.calculate_lag()

        stats = tracker.get_lag_stats()

        # Despite fluctuations, overall trend is converging
        assert stats.is_converging is True
