"""
Unit tests for SubscriptionMigrator.

Tests cover:
- SubscriptionMigrator initialization
- plan_migration() dry-run planning
- migrate_subscriptions() execution
- migrate_tenant_subscriptions() convenience method
- verify_migration() verification
- Error handling and edge cases
- Data model classes (SubscriptionMigrationResult, PlannedMigration, etc.)
"""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from eventsource.migration.exceptions import PositionMappingError
from eventsource.migration.position_mapper import TranslationResult
from eventsource.migration.subscription_migrator import (
    MigrationPlan,
    MigrationSummary,
    PlannedMigration,
    SubscriptionMigrationError,
    SubscriptionMigrationResult,
    SubscriptionMigrator,
)
from eventsource.repositories.checkpoint import CheckpointData


class TestSubscriptionMigratorInit:
    """Tests for SubscriptionMigrator initialization."""

    def test_init_with_dependencies(self) -> None:
        """Test initialization with required dependencies."""
        mock_position_mapper = MagicMock()
        mock_checkpoint_repo = MagicMock()

        migrator = SubscriptionMigrator(
            position_mapper=mock_position_mapper,
            checkpoint_repo=mock_checkpoint_repo,
        )

        assert migrator._position_mapper == mock_position_mapper
        assert migrator._checkpoint_repo == mock_checkpoint_repo

    def test_init_with_tracing_enabled(self) -> None:
        """Test initialization with tracing enabled."""
        mock_position_mapper = MagicMock()
        mock_checkpoint_repo = MagicMock()

        migrator = SubscriptionMigrator(
            position_mapper=mock_position_mapper,
            checkpoint_repo=mock_checkpoint_repo,
            enable_tracing=True,
        )

        assert migrator._position_mapper == mock_position_mapper

    def test_init_with_tracing_disabled(self) -> None:
        """Test initialization with tracing disabled."""
        mock_position_mapper = MagicMock()
        mock_checkpoint_repo = MagicMock()

        migrator = SubscriptionMigrator(
            position_mapper=mock_position_mapper,
            checkpoint_repo=mock_checkpoint_repo,
            enable_tracing=False,
        )

        assert migrator._position_mapper == mock_position_mapper


class TestSubscriptionMigratorPlanMigration:
    """Tests for SubscriptionMigrator.plan_migration method."""

    @pytest.fixture
    def mock_position_mapper(self) -> MagicMock:
        """Create a mock position mapper."""
        return MagicMock()

    @pytest.fixture
    def mock_checkpoint_repo(self) -> MagicMock:
        """Create a mock checkpoint repository."""
        return MagicMock()

    @pytest.fixture
    def migrator(
        self,
        mock_position_mapper: MagicMock,
        mock_checkpoint_repo: MagicMock,
    ) -> SubscriptionMigrator:
        """Create a migrator with mock dependencies."""
        return SubscriptionMigrator(
            position_mapper=mock_position_mapper,
            checkpoint_repo=mock_checkpoint_repo,
            enable_tracing=False,
        )

    @pytest.mark.asyncio
    async def test_plan_migration_returns_plan(
        self,
        migrator: SubscriptionMigrator,
        mock_checkpoint_repo: MagicMock,
        mock_position_mapper: MagicMock,
    ) -> None:
        """Test plan_migration returns a MigrationPlan."""
        migration_id = uuid4()
        tenant_id = uuid4()

        mock_checkpoint_repo.get_position = AsyncMock(return_value=1000)
        mock_position_mapper.translate_position = AsyncMock(
            return_value=TranslationResult(
                source_position=1000,
                target_position=500,
                is_exact=True,
            )
        )

        plan = await migrator.plan_migration(
            migration_id=migration_id,
            tenant_id=tenant_id,
            subscription_names=["OrderProjection"],
        )

        assert isinstance(plan, MigrationPlan)
        assert plan.migration_id == migration_id
        assert plan.tenant_id == tenant_id
        assert plan.migratable_count == 1
        assert len(plan.planned_migrations) == 1
        assert plan.planned_migrations[0].subscription_name == "OrderProjection"

    @pytest.mark.asyncio
    async def test_plan_migration_skips_no_checkpoint(
        self,
        migrator: SubscriptionMigrator,
        mock_checkpoint_repo: MagicMock,
    ) -> None:
        """Test plan_migration skips subscriptions without checkpoints."""
        migration_id = uuid4()
        tenant_id = uuid4()

        mock_checkpoint_repo.get_position = AsyncMock(return_value=None)

        plan = await migrator.plan_migration(
            migration_id=migration_id,
            tenant_id=tenant_id,
            subscription_names=["OrderProjection"],
        )

        assert plan.migratable_count == 0
        assert len(plan.skipped_subscriptions) == 1
        assert "OrderProjection" in plan.skipped_subscriptions

    @pytest.mark.asyncio
    async def test_plan_migration_handles_translation_error(
        self,
        migrator: SubscriptionMigrator,
        mock_checkpoint_repo: MagicMock,
        mock_position_mapper: MagicMock,
    ) -> None:
        """Test plan_migration handles position translation errors."""
        migration_id = uuid4()
        tenant_id = uuid4()

        mock_checkpoint_repo.get_position = AsyncMock(return_value=1000)
        mock_position_mapper.translate_position = AsyncMock(
            side_effect=PositionMappingError(
                "No mapping found",
                migration_id=migration_id,
                source_position=1000,
                reason="no_mapping",
            )
        )

        plan = await migrator.plan_migration(
            migration_id=migration_id,
            tenant_id=tenant_id,
            subscription_names=["OrderProjection"],
        )

        # Should still include in planned_migrations with warning
        assert plan.migratable_count == 1
        assert len(plan.planned_migrations) == 1
        assert plan.planned_migrations[0].warning is not None
        assert plan.planned_migrations[0].planned_target_position is None

    @pytest.mark.asyncio
    async def test_plan_migration_with_nearest_translation(
        self,
        migrator: SubscriptionMigrator,
        mock_checkpoint_repo: MagicMock,
        mock_position_mapper: MagicMock,
    ) -> None:
        """Test plan_migration handles nearest position translation."""
        migration_id = uuid4()
        tenant_id = uuid4()

        mock_checkpoint_repo.get_position = AsyncMock(return_value=1050)
        mock_position_mapper.translate_position = AsyncMock(
            return_value=TranslationResult(
                source_position=1050,
                target_position=500,
                is_exact=False,
                nearest_source_position=1000,
            )
        )

        plan = await migrator.plan_migration(
            migration_id=migration_id,
            tenant_id=tenant_id,
            subscription_names=["OrderProjection"],
        )

        assert plan.migratable_count == 1
        pm = plan.planned_migrations[0]
        assert pm.is_exact_translation is False
        assert pm.nearest_source_position == 1000
        assert pm.warning is not None
        assert "nearest" in pm.warning.lower()

    @pytest.mark.asyncio
    async def test_plan_migration_multiple_subscriptions(
        self,
        migrator: SubscriptionMigrator,
        mock_checkpoint_repo: MagicMock,
        mock_position_mapper: MagicMock,
    ) -> None:
        """Test plan_migration with multiple subscriptions."""
        migration_id = uuid4()
        tenant_id = uuid4()

        # Return different positions for each subscription
        positions = {
            "OrderProjection": 1000,
            "InventoryProjection": 2000,
            "NoCheckpoint": None,
        }
        mock_checkpoint_repo.get_position = AsyncMock(side_effect=lambda name: positions.get(name))
        mock_position_mapper.translate_position = AsyncMock(
            return_value=TranslationResult(
                source_position=1000,
                target_position=500,
                is_exact=True,
            )
        )

        plan = await migrator.plan_migration(
            migration_id=migration_id,
            tenant_id=tenant_id,
            subscription_names=["OrderProjection", "InventoryProjection", "NoCheckpoint"],
        )

        assert plan.total_subscriptions == 3
        assert plan.migratable_count == 2
        assert len(plan.skipped_subscriptions) == 1


class TestSubscriptionMigratorMigrateSubscriptions:
    """Tests for SubscriptionMigrator.migrate_subscriptions method."""

    @pytest.fixture
    def mock_position_mapper(self) -> MagicMock:
        """Create a mock position mapper."""
        return MagicMock()

    @pytest.fixture
    def mock_checkpoint_repo(self) -> MagicMock:
        """Create a mock checkpoint repository."""
        return MagicMock()

    @pytest.fixture
    def migrator(
        self,
        mock_position_mapper: MagicMock,
        mock_checkpoint_repo: MagicMock,
    ) -> SubscriptionMigrator:
        """Create a migrator with mock dependencies."""
        return SubscriptionMigrator(
            position_mapper=mock_position_mapper,
            checkpoint_repo=mock_checkpoint_repo,
            enable_tracing=False,
        )

    @pytest.mark.asyncio
    async def test_migrate_subscriptions_dry_run(
        self,
        migrator: SubscriptionMigrator,
        mock_checkpoint_repo: MagicMock,
        mock_position_mapper: MagicMock,
    ) -> None:
        """Test migrate_subscriptions in dry_run mode."""
        migration_id = uuid4()
        tenant_id = uuid4()

        mock_checkpoint_repo.get_position = AsyncMock(return_value=1000)
        mock_position_mapper.translate_position = AsyncMock(
            return_value=TranslationResult(
                source_position=1000,
                target_position=500,
                is_exact=True,
            )
        )

        summary = await migrator.migrate_subscriptions(
            migration_id=migration_id,
            tenant_id=tenant_id,
            subscription_names=["OrderProjection"],
            dry_run=True,
        )

        # Should not call save_position in dry_run mode
        mock_checkpoint_repo.save_position.assert_not_called()

        assert isinstance(summary, MigrationSummary)
        assert summary.migration_id == migration_id
        assert summary.successful_count == 1

    @pytest.mark.asyncio
    async def test_migrate_subscriptions_executes(
        self,
        migrator: SubscriptionMigrator,
        mock_checkpoint_repo: MagicMock,
        mock_position_mapper: MagicMock,
    ) -> None:
        """Test migrate_subscriptions actually migrates checkpoints."""
        migration_id = uuid4()
        tenant_id = uuid4()
        event_id = uuid4()

        mock_checkpoint_repo.get_position = AsyncMock(return_value=1000)
        mock_checkpoint_repo.get_all_checkpoints = AsyncMock(
            return_value=[
                CheckpointData(
                    projection_name="OrderProjection",
                    last_event_id=event_id,
                    last_event_type="OrderCreated",
                    global_position=1000,
                )
            ]
        )
        mock_checkpoint_repo.save_position = AsyncMock()
        mock_position_mapper.translate_position = AsyncMock(
            return_value=TranslationResult(
                source_position=1000,
                target_position=500,
                is_exact=True,
            )
        )

        summary = await migrator.migrate_subscriptions(
            migration_id=migration_id,
            tenant_id=tenant_id,
            subscription_names=["OrderProjection"],
            dry_run=False,
        )

        # Should call save_position
        mock_checkpoint_repo.save_position.assert_called_once_with(
            subscription_id="OrderProjection",
            position=500,
            event_id=event_id,
            event_type="OrderCreated",
        )

        assert summary.successful_count == 1
        assert summary.failed_count == 0
        assert summary.all_successful

    @pytest.mark.asyncio
    async def test_migrate_subscriptions_handles_missing_checkpoint_data(
        self,
        migrator: SubscriptionMigrator,
        mock_checkpoint_repo: MagicMock,
        mock_position_mapper: MagicMock,
    ) -> None:
        """Test migrate_subscriptions skips incomplete checkpoint data."""
        migration_id = uuid4()
        tenant_id = uuid4()

        mock_checkpoint_repo.get_position = AsyncMock(return_value=1000)
        mock_checkpoint_repo.get_all_checkpoints = AsyncMock(
            return_value=[
                CheckpointData(
                    projection_name="OrderProjection",
                    last_event_id=None,  # Missing event_id
                    last_event_type=None,
                    global_position=1000,
                )
            ]
        )

        summary = await migrator.migrate_subscriptions(
            migration_id=migration_id,
            tenant_id=tenant_id,
            subscription_names=["OrderProjection"],
            dry_run=False,
        )

        # Should skip due to incomplete data
        assert summary.skipped_count == 1
        assert summary.successful_count == 0

    @pytest.mark.asyncio
    async def test_migrate_subscriptions_handles_translation_error(
        self,
        migrator: SubscriptionMigrator,
        mock_checkpoint_repo: MagicMock,
        mock_position_mapper: MagicMock,
    ) -> None:
        """Test migrate_subscriptions handles position translation errors."""
        migration_id = uuid4()
        tenant_id = uuid4()
        event_id = uuid4()

        mock_checkpoint_repo.get_position = AsyncMock(return_value=1000)
        mock_checkpoint_repo.get_all_checkpoints = AsyncMock(
            return_value=[
                CheckpointData(
                    projection_name="OrderProjection",
                    last_event_id=event_id,
                    last_event_type="OrderCreated",
                    global_position=1000,
                )
            ]
        )
        mock_position_mapper.translate_position = AsyncMock(
            side_effect=PositionMappingError(
                "No mapping found",
                migration_id=migration_id,
                source_position=1000,
                reason="no_mapping",
            )
        )

        summary = await migrator.migrate_subscriptions(
            migration_id=migration_id,
            tenant_id=tenant_id,
            subscription_names=["OrderProjection"],
            dry_run=False,
        )

        assert summary.failed_count == 1
        assert summary.successful_count == 0
        assert not summary.all_successful
        assert summary.results[0].error_message is not None

    @pytest.mark.asyncio
    async def test_migrate_subscriptions_handles_save_error(
        self,
        migrator: SubscriptionMigrator,
        mock_checkpoint_repo: MagicMock,
        mock_position_mapper: MagicMock,
    ) -> None:
        """Test migrate_subscriptions handles checkpoint save errors."""
        migration_id = uuid4()
        tenant_id = uuid4()
        event_id = uuid4()

        mock_checkpoint_repo.get_position = AsyncMock(return_value=1000)
        mock_checkpoint_repo.get_all_checkpoints = AsyncMock(
            return_value=[
                CheckpointData(
                    projection_name="OrderProjection",
                    last_event_id=event_id,
                    last_event_type="OrderCreated",
                    global_position=1000,
                )
            ]
        )
        mock_checkpoint_repo.save_position = AsyncMock(side_effect=Exception("Database error"))
        mock_position_mapper.translate_position = AsyncMock(
            return_value=TranslationResult(
                source_position=1000,
                target_position=500,
                is_exact=True,
            )
        )

        summary = await migrator.migrate_subscriptions(
            migration_id=migration_id,
            tenant_id=tenant_id,
            subscription_names=["OrderProjection"],
            dry_run=False,
        )

        assert summary.failed_count == 1
        assert summary.results[0].error_message is not None
        assert "Database error" in summary.results[0].error_message


class TestSubscriptionMigratorMigrateTenantSubscriptions:
    """Tests for SubscriptionMigrator.migrate_tenant_subscriptions method."""

    @pytest.fixture
    def mock_position_mapper(self) -> MagicMock:
        """Create a mock position mapper."""
        return MagicMock()

    @pytest.fixture
    def mock_checkpoint_repo(self) -> MagicMock:
        """Create a mock checkpoint repository."""
        return MagicMock()

    @pytest.fixture
    def migrator(
        self,
        mock_position_mapper: MagicMock,
        mock_checkpoint_repo: MagicMock,
    ) -> SubscriptionMigrator:
        """Create a migrator with mock dependencies."""
        return SubscriptionMigrator(
            position_mapper=mock_position_mapper,
            checkpoint_repo=mock_checkpoint_repo,
            enable_tracing=False,
        )

    @pytest.mark.asyncio
    async def test_migrate_tenant_with_explicit_names(
        self,
        migrator: SubscriptionMigrator,
        mock_checkpoint_repo: MagicMock,
        mock_position_mapper: MagicMock,
    ) -> None:
        """Test migrate_tenant_subscriptions with explicit subscription names."""
        migration_id = uuid4()
        tenant_id = uuid4()
        event_id = uuid4()

        mock_checkpoint_repo.get_position = AsyncMock(return_value=1000)
        mock_checkpoint_repo.get_all_checkpoints = AsyncMock(
            return_value=[
                CheckpointData(
                    projection_name="OrderProjection",
                    last_event_id=event_id,
                    last_event_type="OrderCreated",
                    global_position=1000,
                )
            ]
        )
        mock_checkpoint_repo.save_position = AsyncMock()
        mock_position_mapper.translate_position = AsyncMock(
            return_value=TranslationResult(
                source_position=1000,
                target_position=500,
                is_exact=True,
            )
        )

        summary = await migrator.migrate_tenant_subscriptions(
            migration_id=migration_id,
            tenant_id=tenant_id,
            subscription_names=["OrderProjection"],
        )

        assert summary.successful_count == 1

    @pytest.mark.asyncio
    async def test_migrate_tenant_discovers_subscriptions(
        self,
        migrator: SubscriptionMigrator,
        mock_checkpoint_repo: MagicMock,
        mock_position_mapper: MagicMock,
    ) -> None:
        """Test migrate_tenant_subscriptions discovers subscriptions from checkpoints."""
        migration_id = uuid4()
        tenant_id = uuid4()
        event_id = uuid4()

        mock_checkpoint_repo.get_all_checkpoints = AsyncMock(
            return_value=[
                CheckpointData(
                    projection_name="OrderProjection",
                    last_event_id=event_id,
                    last_event_type="OrderCreated",
                    global_position=1000,
                ),
                CheckpointData(
                    projection_name="InventoryProjection",
                    last_event_id=event_id,
                    last_event_type="InventoryUpdated",
                    global_position=2000,
                ),
            ]
        )
        mock_checkpoint_repo.get_position = AsyncMock(return_value=1000)
        mock_checkpoint_repo.save_position = AsyncMock()
        mock_position_mapper.translate_position = AsyncMock(
            return_value=TranslationResult(
                source_position=1000,
                target_position=500,
                is_exact=True,
            )
        )

        summary = await migrator.migrate_tenant_subscriptions(
            migration_id=migration_id,
            tenant_id=tenant_id,
            subscription_names=None,  # Should discover from checkpoints
        )

        # Should have discovered both subscriptions
        assert summary.successful_count == 2


class TestSubscriptionMigratorVerifyMigration:
    """Tests for SubscriptionMigrator.verify_migration method."""

    @pytest.fixture
    def mock_position_mapper(self) -> MagicMock:
        """Create a mock position mapper."""
        return MagicMock()

    @pytest.fixture
    def mock_checkpoint_repo(self) -> MagicMock:
        """Create a mock checkpoint repository."""
        return MagicMock()

    @pytest.fixture
    def migrator(
        self,
        mock_position_mapper: MagicMock,
        mock_checkpoint_repo: MagicMock,
    ) -> SubscriptionMigrator:
        """Create a migrator with mock dependencies."""
        return SubscriptionMigrator(
            position_mapper=mock_position_mapper,
            checkpoint_repo=mock_checkpoint_repo,
            enable_tracing=False,
        )

    @pytest.mark.asyncio
    async def test_verify_migration_all_present(
        self,
        migrator: SubscriptionMigrator,
        mock_checkpoint_repo: MagicMock,
    ) -> None:
        """Test verify_migration returns True for all present checkpoints."""
        migration_id = uuid4()

        mock_checkpoint_repo.get_position = AsyncMock(return_value=500)

        results = await migrator.verify_migration(
            migration_id=migration_id,
            subscription_names=["OrderProjection", "InventoryProjection"],
        )

        assert all(results.values())
        assert len(results) == 2

    @pytest.mark.asyncio
    async def test_verify_migration_some_missing(
        self,
        migrator: SubscriptionMigrator,
        mock_checkpoint_repo: MagicMock,
    ) -> None:
        """Test verify_migration returns False for missing checkpoints."""
        migration_id = uuid4()

        positions = {
            "OrderProjection": 500,
            "InventoryProjection": None,
        }
        mock_checkpoint_repo.get_position = AsyncMock(side_effect=lambda name: positions.get(name))

        results = await migrator.verify_migration(
            migration_id=migration_id,
            subscription_names=["OrderProjection", "InventoryProjection"],
        )

        assert results["OrderProjection"] is True
        assert results["InventoryProjection"] is False


class TestSubscriptionMigrationResultDataclass:
    """Tests for SubscriptionMigrationResult dataclass."""

    def test_result_success(self) -> None:
        """Test SubscriptionMigrationResult for successful migration."""
        now = datetime.now(UTC)
        result = SubscriptionMigrationResult(
            subscription_name="OrderProjection",
            success=True,
            source_position=1000,
            target_position=500,
            is_exact_translation=True,
            migrated_at=now,
        )

        assert result.subscription_name == "OrderProjection"
        assert result.success is True
        assert result.source_position == 1000
        assert result.target_position == 500
        assert result.is_exact_translation is True
        assert result.error_message is None

    def test_result_failure(self) -> None:
        """Test SubscriptionMigrationResult for failed migration."""
        result = SubscriptionMigrationResult(
            subscription_name="OrderProjection",
            success=False,
            source_position=1000,
            error_message="Translation failed",
        )

        assert result.success is False
        assert result.error_message == "Translation failed"
        assert result.target_position is None

    def test_result_to_dict(self) -> None:
        """Test SubscriptionMigrationResult.to_dict serialization."""
        now = datetime.now(UTC)
        result = SubscriptionMigrationResult(
            subscription_name="OrderProjection",
            success=True,
            source_position=1000,
            target_position=500,
            migrated_at=now,
        )

        d = result.to_dict()

        assert d["subscription_name"] == "OrderProjection"
        assert d["success"] is True
        assert d["source_position"] == 1000
        assert d["target_position"] == 500
        assert d["migrated_at"] == now.isoformat()

    def test_result_frozen(self) -> None:
        """Test SubscriptionMigrationResult is immutable."""
        result = SubscriptionMigrationResult(
            subscription_name="OrderProjection",
            success=True,
            source_position=1000,
        )

        with pytest.raises(AttributeError):
            result.success = False  # type: ignore[misc]


class TestPlannedMigrationDataclass:
    """Tests for PlannedMigration dataclass."""

    def test_planned_migration_exact(self) -> None:
        """Test PlannedMigration for exact translation."""
        planned = PlannedMigration(
            subscription_name="OrderProjection",
            current_position=1000,
            planned_target_position=500,
            is_exact_translation=True,
        )

        assert planned.subscription_name == "OrderProjection"
        assert planned.current_position == 1000
        assert planned.planned_target_position == 500
        assert planned.is_exact_translation is True
        assert planned.warning is None

    def test_planned_migration_with_warning(self) -> None:
        """Test PlannedMigration with warning."""
        planned = PlannedMigration(
            subscription_name="OrderProjection",
            current_position=1050,
            planned_target_position=500,
            is_exact_translation=False,
            nearest_source_position=1000,
            warning="Using nearest position mapping",
        )

        assert planned.is_exact_translation is False
        assert planned.nearest_source_position == 1000
        assert planned.warning is not None

    def test_planned_migration_to_dict(self) -> None:
        """Test PlannedMigration.to_dict serialization."""
        planned = PlannedMigration(
            subscription_name="OrderProjection",
            current_position=1000,
            planned_target_position=500,
        )

        d = planned.to_dict()

        assert d["subscription_name"] == "OrderProjection"
        assert d["current_position"] == 1000
        assert d["planned_target_position"] == 500


class TestMigrationPlanDataclass:
    """Tests for MigrationPlan dataclass."""

    def test_migration_plan_creation(self) -> None:
        """Test MigrationPlan creation."""
        migration_id = uuid4()
        tenant_id = uuid4()

        planned = PlannedMigration(
            subscription_name="OrderProjection",
            current_position=1000,
            planned_target_position=500,
        )

        plan = MigrationPlan(
            migration_id=migration_id,
            tenant_id=tenant_id,
            planned_migrations=[planned],
            total_subscriptions=2,
            migratable_count=1,
            skipped_subscriptions=["NoCheckpoint"],
        )

        assert plan.migration_id == migration_id
        assert plan.tenant_id == tenant_id
        assert plan.migratable_count == 1
        assert len(plan.skipped_subscriptions) == 1

    def test_migration_plan_to_dict(self) -> None:
        """Test MigrationPlan.to_dict serialization."""
        migration_id = uuid4()
        tenant_id = uuid4()

        plan = MigrationPlan(
            migration_id=migration_id,
            tenant_id=tenant_id,
            planned_migrations=[],
            total_subscriptions=0,
            migratable_count=0,
        )

        d = plan.to_dict()

        assert d["migration_id"] == str(migration_id)
        assert d["tenant_id"] == str(tenant_id)
        assert isinstance(d["created_at"], str)


class TestMigrationSummaryDataclass:
    """Tests for MigrationSummary dataclass."""

    def test_migration_summary_creation(self) -> None:
        """Test MigrationSummary creation."""
        migration_id = uuid4()
        tenant_id = uuid4()

        result = SubscriptionMigrationResult(
            subscription_name="OrderProjection",
            success=True,
            source_position=1000,
            target_position=500,
        )

        summary = MigrationSummary(
            migration_id=migration_id,
            tenant_id=tenant_id,
            results=[result],
            successful_count=1,
            failed_count=0,
            duration_ms=100.0,
        )

        assert summary.migration_id == migration_id
        assert summary.all_successful is True

    def test_migration_summary_not_all_successful(self) -> None:
        """Test MigrationSummary.all_successful returns False on failures."""
        migration_id = uuid4()
        tenant_id = uuid4()

        summary = MigrationSummary(
            migration_id=migration_id,
            tenant_id=tenant_id,
            results=[],
            successful_count=1,
            failed_count=1,
        )

        assert summary.all_successful is False

    def test_migration_summary_empty(self) -> None:
        """Test MigrationSummary.all_successful returns False when empty."""
        migration_id = uuid4()
        tenant_id = uuid4()

        summary = MigrationSummary(
            migration_id=migration_id,
            tenant_id=tenant_id,
            results=[],
            successful_count=0,
            failed_count=0,
        )

        assert summary.all_successful is False

    def test_migration_summary_to_dict(self) -> None:
        """Test MigrationSummary.to_dict serialization."""
        migration_id = uuid4()
        tenant_id = uuid4()
        now = datetime.now(UTC)

        summary = MigrationSummary(
            migration_id=migration_id,
            tenant_id=tenant_id,
            results=[],
            successful_count=1,
            failed_count=0,
            started_at=now,
            completed_at=now,
            duration_ms=100.0,
        )

        d = summary.to_dict()

        assert d["migration_id"] == str(migration_id)
        assert d["all_successful"] is True
        assert d["duration_ms"] == 100.0


class TestSubscriptionMigrationErrorException:
    """Tests for SubscriptionMigrationError exception."""

    def test_exception_creation(self) -> None:
        """Test SubscriptionMigrationError creation."""
        migration_id = uuid4()

        error = SubscriptionMigrationError(
            message="Migration failed",
            migration_id=migration_id,
            subscription_name="OrderProjection",
            reason="translation_failed",
        )

        assert error.subscription_name == "OrderProjection"
        assert error.reason == "translation_failed"
        assert error.migration_id == migration_id
        assert error.recoverable is True

    def test_exception_str(self) -> None:
        """Test SubscriptionMigrationError string representation."""
        migration_id = uuid4()

        error = SubscriptionMigrationError(
            message="Migration failed",
            migration_id=migration_id,
            subscription_name="OrderProjection",
        )

        assert "Migration failed" in str(error)
        assert str(migration_id) in str(error)


class TestSubscriptionMigratorWorkflows:
    """Integration-style tests for subscription migration workflows."""

    @pytest.fixture
    def mock_position_mapper(self) -> MagicMock:
        """Create a mock position mapper."""
        return MagicMock()

    @pytest.fixture
    def mock_checkpoint_repo(self) -> MagicMock:
        """Create a mock checkpoint repository."""
        return MagicMock()

    @pytest.fixture
    def migrator(
        self,
        mock_position_mapper: MagicMock,
        mock_checkpoint_repo: MagicMock,
    ) -> SubscriptionMigrator:
        """Create a migrator with mock dependencies."""
        return SubscriptionMigrator(
            position_mapper=mock_position_mapper,
            checkpoint_repo=mock_checkpoint_repo,
            enable_tracing=False,
        )

    @pytest.mark.asyncio
    async def test_full_migration_workflow(
        self,
        migrator: SubscriptionMigrator,
        mock_checkpoint_repo: MagicMock,
        mock_position_mapper: MagicMock,
    ) -> None:
        """Test complete migration workflow: plan -> execute -> verify."""
        migration_id = uuid4()
        tenant_id = uuid4()
        event_id = uuid4()

        # Setup mocks
        mock_checkpoint_repo.get_position = AsyncMock(return_value=1000)
        mock_checkpoint_repo.get_all_checkpoints = AsyncMock(
            return_value=[
                CheckpointData(
                    projection_name="OrderProjection",
                    last_event_id=event_id,
                    last_event_type="OrderCreated",
                    global_position=1000,
                )
            ]
        )
        mock_checkpoint_repo.save_position = AsyncMock()
        mock_position_mapper.translate_position = AsyncMock(
            return_value=TranslationResult(
                source_position=1000,
                target_position=500,
                is_exact=True,
            )
        )

        # Step 1: Plan migration
        plan = await migrator.plan_migration(
            migration_id=migration_id,
            tenant_id=tenant_id,
            subscription_names=["OrderProjection"],
        )

        assert plan.migratable_count == 1
        assert plan.planned_migrations[0].planned_target_position == 500

        # Step 2: Execute migration
        summary = await migrator.migrate_subscriptions(
            migration_id=migration_id,
            tenant_id=tenant_id,
            subscription_names=["OrderProjection"],
        )

        assert summary.successful_count == 1
        assert summary.all_successful

        # Step 3: Verify migration
        # Update mock to return new position
        mock_checkpoint_repo.get_position = AsyncMock(return_value=500)

        verification = await migrator.verify_migration(
            migration_id=migration_id,
            subscription_names=["OrderProjection"],
        )

        assert verification["OrderProjection"] is True

    @pytest.mark.asyncio
    async def test_partial_failure_workflow(
        self,
        migrator: SubscriptionMigrator,
        mock_checkpoint_repo: MagicMock,
        mock_position_mapper: MagicMock,
    ) -> None:
        """Test migration workflow with partial failures."""
        migration_id = uuid4()
        tenant_id = uuid4()
        event_id = uuid4()

        # Setup mocks - one succeeds, one fails
        positions = {
            "OrderProjection": 1000,
            "FailingProjection": 2000,
        }
        mock_checkpoint_repo.get_position = AsyncMock(side_effect=lambda name: positions.get(name))
        mock_checkpoint_repo.get_all_checkpoints = AsyncMock(
            return_value=[
                CheckpointData(
                    projection_name="OrderProjection",
                    last_event_id=event_id,
                    last_event_type="OrderCreated",
                    global_position=1000,
                ),
                CheckpointData(
                    projection_name="FailingProjection",
                    last_event_id=event_id,
                    last_event_type="OrderCreated",
                    global_position=2000,
                ),
            ]
        )
        mock_checkpoint_repo.save_position = AsyncMock()

        def translate_position_effect(migration_id, source_position, use_nearest=True):
            if source_position == 1000:
                return TranslationResult(
                    source_position=1000,
                    target_position=500,
                    is_exact=True,
                )
            else:
                raise PositionMappingError(
                    "No mapping found",
                    migration_id=migration_id,
                    source_position=source_position,
                    reason="no_mapping",
                )

        mock_position_mapper.translate_position = AsyncMock(side_effect=translate_position_effect)

        # Execute migration
        summary = await migrator.migrate_subscriptions(
            migration_id=migration_id,
            tenant_id=tenant_id,
            subscription_names=["OrderProjection", "FailingProjection"],
        )

        assert summary.successful_count == 1
        assert summary.failed_count == 1
        assert not summary.all_successful

        # Verify the successful one was saved
        mock_checkpoint_repo.save_position.assert_called_once()
