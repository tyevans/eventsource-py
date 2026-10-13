"""Unit tests for CutoverManager initialization, successful cutover, and zero-lag defaults."""

from datetime import UTC, datetime

import pytest

from eventsource.application.migration.cutover import CutoverManager
from eventsource.ports import Position
from eventsource.ports.migration.models import (
    SyncLag,
    TenantMigrationState,
    TenantRouting,
)


class TestCutoverManagerInit:
    """Tests for CutoverManager initialization."""

    def test_init_with_defaults(
        self,
        mock_lock_manager,
        mock_router,
        mock_routing_repo,
    ):
        """Test initialization with default parameters."""
        manager = CutoverManager(
            lock_manager=mock_lock_manager,
            router=mock_router,
            routing_repo=mock_routing_repo,
        )

        assert manager._lock_manager is mock_lock_manager
        assert manager._router is mock_router
        assert manager._routing_repo is mock_routing_repo
        assert manager._lock_acquisition_timeout == 0.5
        assert manager._enable_tracing is True

    def test_init_with_custom_timeout(
        self,
        mock_lock_manager,
        mock_router,
        mock_routing_repo,
    ):
        """Test initialization with custom lock timeout."""
        manager = CutoverManager(
            lock_manager=mock_lock_manager,
            router=mock_router,
            routing_repo=mock_routing_repo,
            lock_acquisition_timeout=1.0,
        )

        assert manager._lock_acquisition_timeout == 1.0

    def test_init_with_tracing_disabled(
        self,
        mock_lock_manager,
        mock_router,
        mock_routing_repo,
    ):
        """Test initialization with tracing disabled."""
        manager = CutoverManager(
            lock_manager=mock_lock_manager,
            router=mock_router,
            routing_repo=mock_routing_repo,
            enable_tracing=False,
        )

        assert manager._enable_tracing is False


class TestSuccessfulCutover:
    """Tests for successful cutover scenarios."""

    @pytest.mark.asyncio
    async def test_successful_cutover(
        self,
        cutover_manager,
        tenant_id,
        migration_id,
        target_store_id,
        mock_lag_tracker,
        mock_router,
        mock_routing_repo,
    ):
        """Test successful cutover execution."""
        result = await cutover_manager.execute_cutover(
            migration_id=migration_id,
            tenant_id=tenant_id,
            lag_tracker=mock_lag_tracker,
            target_store_id=target_store_id,
        )

        assert result.success is True
        assert result.duration_ms > 0
        assert result.rolled_back is False
        assert result.error_message is None

        # Verify write pause/resume
        mock_router.pause_writes.assert_called_once_with(tenant_id)
        mock_router.resume_writes.assert_called_once_with(tenant_id)

        # Verify routing updates
        mock_routing_repo.set_migration_state.assert_any_call(
            tenant_id,
            TenantMigrationState.CUTOVER_PAUSED,
            migration_id=migration_id,
        )
        mock_routing_repo.set_migration_state.assert_any_call(
            tenant_id,
            TenantMigrationState.MIGRATED,
            migration_id=migration_id,
        )

        # Verify dual-write interceptor cleared
        mock_router.clear_dual_write_interceptor.assert_called_once_with(tenant_id)

    @pytest.mark.asyncio
    async def test_successful_cutover_records_duration_metric(
        self,
        cutover_manager,
        tenant_id,
        migration_id,
        target_store_id,
        mock_lag_tracker,
    ):
        """execute_cutover() reports through the real MigrationMetrics
        instance for this migration -- not a standalone metrics object the
        test constructs and calls directly."""
        from eventsource.application.migration.metrics import (
            clear_metrics_registry,
            get_migration_metrics,
        )

        clear_metrics_registry()

        result = await cutover_manager.execute_cutover(
            migration_id=migration_id,
            tenant_id=tenant_id,
            lag_tracker=mock_lag_tracker,
            target_store_id=target_store_id,
        )

        assert result.success is True
        snapshot = get_migration_metrics(str(migration_id), str(tenant_id)).get_snapshot()
        assert snapshot.cutover_durations == [result.duration_ms]

        clear_metrics_registry()

    @pytest.mark.asyncio
    async def test_successful_cutover_with_small_lag(
        self,
        cutover_manager,
        tenant_id,
        migration_id,
        target_store_id,
        mock_lag_tracker,
        config,
    ):
        """Test successful cutover with small but acceptable lag."""
        # Set small lag (within threshold)
        lag = SyncLag(
            events=50,  # Below threshold of 100
            source_position=Position(store_id="source", key=(100,)),
            target_position=Position(store_id="target", key=(50,)),
            timestamp=datetime.now(UTC),
        )
        mock_lag_tracker.current_lag = lag

        result = await cutover_manager.execute_cutover(
            migration_id=migration_id,
            tenant_id=tenant_id,
            lag_tracker=mock_lag_tracker,
            target_store_id=target_store_id,
            config=config,
        )

        assert result.success is True

    @pytest.mark.asyncio
    async def test_successful_cutover_with_custom_timeout(
        self,
        cutover_manager,
        tenant_id,
        migration_id,
        target_store_id,
        mock_lag_tracker,
    ):
        """Test cutover with custom timeout."""
        result = await cutover_manager.execute_cutover(
            migration_id=migration_id,
            tenant_id=tenant_id,
            lag_tracker=mock_lag_tracker,
            target_store_id=target_store_id,
            timeout_ms=1000.0,  # 1 second timeout
        )

        assert result.success is True

    @pytest.mark.asyncio
    async def test_cutover_tracks_events_synced(
        self,
        cutover_manager,
        tenant_id,
        migration_id,
        target_store_id,
        mock_lag_tracker,
        config,
    ):
        """Test that cutover tracks events synced during pause."""
        # Initial lag
        initial_lag = SyncLag(
            events=10,
            source_position=Position(store_id="source", key=(100,)),
            target_position=Position(store_id="target", key=(90,)),
            timestamp=datetime.now(UTC),
        )
        # Final lag (improved during pause)
        final_lag = SyncLag(
            events=2,
            source_position=Position(store_id="source", key=(100,)),
            target_position=Position(store_id="target", key=(98,)),
            timestamp=datetime.now(UTC),
        )

        # Set up mock to return different lags on successive calls
        call_count = 0

        async def update_lag(*, since=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                mock_lag_tracker.current_lag = initial_lag
            else:
                mock_lag_tracker.current_lag = final_lag

        mock_lag_tracker.calculate_lag.side_effect = update_lag
        mock_lag_tracker.current_lag = initial_lag  # Initial state

        result = await cutover_manager.execute_cutover(
            migration_id=migration_id,
            tenant_id=tenant_id,
            lag_tracker=mock_lag_tracker,
            target_store_id=target_store_id,
            config=config,  # tolerance is incidental; the subject is events_synced tracking
        )

        assert result.success is True
        assert result.events_synced == 8  # 10 - 2


class TestStrictZeroLagDefault:
    """`cutover_max_lag_events` defaults to 0: no cutover over missing data."""

    @pytest.mark.asyncio
    async def test_default_config_refuses_cutover_on_a_single_missing_event(
        self,
        cutover_manager,
        tenant_id,
        migration_id,
        target_store_id,
        mock_lag_tracker,
        mock_router,
        mock_routing_repo,
    ):
        mock_lag_tracker.current_lag = SyncLag(
            events=1,
            source_position=Position(store_id="source", key=(101,)),
            target_position=Position(store_id="target", key=(100,)),
            timestamp=datetime.now(UTC),
        )

        result = await cutover_manager.execute_cutover(
            migration_id=migration_id,
            tenant_id=tenant_id,
            lag_tracker=mock_lag_tracker,
            target_store_id=target_store_id,
        )

        assert result.success is False
        assert result.rolled_back is True
        # Routing never switched: the source stays authoritative.
        mock_routing_repo.set_routing.assert_not_called()
        mock_router.resume_writes.assert_called_once_with(tenant_id)

    @pytest.mark.asyncio
    async def test_rollback_restores_the_route_not_just_the_state(
        self,
        cutover_manager,
        tenant_id,
        migration_id,
        target_store_id,
        mock_lag_tracker,
        mock_routing_repo,
    ):
        """Rollback used to declare DUAL_WRITE while traffic stayed on the target.

        The route and the migration state are two writes. `_rollback` only
        restored the second, so a "rolled back" tenant still had every request
        going to the store the cutover had failed to complete.
        """

        # `get_routing` reflects whatever the last `set_routing` wrote, so
        # the pre-cutover capture sees the source and the rollback sees the
        # target the switch had already moved to.
        async def current_routing(_tenant_id):
            writes = mock_routing_repo.set_routing.call_args_list
            store_id = writes[-1].args[1] if writes else "source-store"
            return TenantRouting(
                tenant_id=tenant_id,
                store_id=store_id,
                migration_state=TenantMigrationState.DUAL_WRITE,
            )

        mock_routing_repo.get_routing.side_effect = current_routing
        mock_routing_repo.set_migration_state.side_effect = [
            None,  # CUTOVER_PAUSED
            Exception("state write failed"),  # MIGRATED
            None,  # DUAL_WRITE, during rollback
        ]

        result = await cutover_manager.execute_cutover(
            migration_id=migration_id,
            tenant_id=tenant_id,
            lag_tracker=mock_lag_tracker,
            target_store_id=target_store_id,
        )

        assert result.success is False
        assert result.rolled_back is True
        restored_to = [call.args[1] for call in mock_routing_repo.set_routing.call_args_list]
        assert restored_to[-1] == "source-store"

    @pytest.mark.asyncio
    async def test_an_explicit_threshold_still_tolerates_lag(
        self,
        cutover_manager,
        tenant_id,
        migration_id,
        target_store_id,
        mock_lag_tracker,
        config,
    ):
        """The knob survives -- it just stops being the default."""
        mock_lag_tracker.current_lag = SyncLag(
            events=1,
            source_position=Position(store_id="source", key=(101,)),
            target_position=Position(store_id="target", key=(100,)),
            timestamp=datetime.now(UTC),
        )

        result = await cutover_manager.execute_cutover(
            migration_id=migration_id,
            tenant_id=tenant_id,
            lag_tracker=mock_lag_tracker,
            target_store_id=target_store_id,
            config=config,  # cutover_max_lag_events=100
        )

        assert result.success is True
