"""Tests for subscription migration dataclasses, exceptions, and workflows."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from eventsource.application.migration.exceptions import PositionMappingError
from eventsource.application.migration.subscription_migrator import (
    MigrationPlan,
    MigrationSummary,
    PlannedMigration,
    SubscriptionMigrationError,
    SubscriptionMigrationResult,
    SubscriptionMigrator,
)
from tests.unit.application.migration.test_subscription_migrator._fixtures import (
    exact_translation,
    make_checkpoint_data,
    source_pos,
    target_pos,
)


class TestSubscriptionMigrationResultDataclass:
    def test_result_success(self) -> None:
        result = SubscriptionMigrationResult(
            "OrderProjection", True, source_pos(1000), target_pos(500), True, datetime.now(UTC)
        )
        assert result.subscription_name == "OrderProjection"
        assert result.success is True
        assert result.source_position == source_pos(1000)
        assert result.target_position == target_pos(500)
        assert result.is_exact_translation is True
        assert result.error_message is None

    def test_result_failure(self) -> None:
        result = SubscriptionMigrationResult(
            "OrderProjection", False, source_pos(1000), error_message="Translation failed"
        )
        assert result.success is False
        assert result.error_message == "Translation failed"
        assert result.target_position is None

    def test_result_to_dict(self) -> None:
        now = datetime.now(UTC)
        result = SubscriptionMigrationResult(
            "OrderProjection", True, source_pos(1000), target_pos(500), migrated_at=now
        )
        d = result.to_dict()
        assert d["subscription_name"] == "OrderProjection"
        assert d["success"] is True
        assert d["source_position"] == source_pos(1000).to_str()
        assert d["target_position"] == target_pos(500).to_str()
        assert d["migrated_at"] == now.isoformat()

    def test_result_frozen(self) -> None:
        result = SubscriptionMigrationResult("OrderProjection", True, source_pos(1000))
        with pytest.raises(AttributeError):
            result.success = False  # type: ignore[misc]


class TestPlannedMigrationDataclass:
    def test_planned_migration_exact(self) -> None:
        planned = PlannedMigration("OrderProjection", source_pos(1000), target_pos(500), True)
        assert planned.subscription_name == "OrderProjection"
        assert planned.current_position == source_pos(1000)
        assert planned.planned_target_position == target_pos(500)
        assert planned.is_exact_translation is True
        assert planned.warning is None

    def test_planned_migration_with_warning(self) -> None:
        planned = PlannedMigration(
            "OrderProjection",
            source_pos(1050),
            target_pos(500),
            False,
            source_pos(1000),
            "Using nearest position mapping",
        )
        assert planned.is_exact_translation is False
        assert planned.nearest_source_position == source_pos(1000)
        assert planned.warning is not None

    def test_planned_migration_to_dict(self) -> None:
        planned = PlannedMigration("OrderProjection", source_pos(1000), target_pos(500))
        d = planned.to_dict()
        assert d["subscription_name"] == "OrderProjection"
        assert d["current_position"] == source_pos(1000).to_str()
        assert d["planned_target_position"] == target_pos(500).to_str()


class TestMigrationPlanDataclass:
    def test_migration_plan_creation(self) -> None:
        migration_id, tenant_id = uuid4(), uuid4()
        planned = PlannedMigration("OrderProjection", source_pos(1000), target_pos(500))
        plan = MigrationPlan(
            migration_id,
            tenant_id,
            [planned],
            total_subscriptions=2,
            migratable_count=1,
            skipped_subscriptions=["NoCheckpoint"],
        )
        assert plan.migration_id == migration_id
        assert plan.tenant_id == tenant_id
        assert plan.migratable_count == 1
        assert len(plan.skipped_subscriptions) == 1

    def test_migration_plan_to_dict(self) -> None:
        migration_id, tenant_id = uuid4(), uuid4()
        plan = MigrationPlan(migration_id, tenant_id, [], 0, 0)
        d = plan.to_dict()
        assert d["migration_id"] == str(migration_id)
        assert d["tenant_id"] == str(tenant_id)
        assert isinstance(d["created_at"], str)


class TestMigrationSummaryDataclass:
    def test_migration_summary_creation(self) -> None:
        migration_id, tenant_id = uuid4(), uuid4()
        result = SubscriptionMigrationResult(
            "OrderProjection", True, source_pos(1000), target_pos(500)
        )
        summary = MigrationSummary(migration_id, tenant_id, [result], 1, 0, duration_ms=100.0)
        assert summary.migration_id == migration_id
        assert summary.all_successful is True

    def test_migration_summary_not_all_successful(self) -> None:
        summary = MigrationSummary(uuid4(), uuid4(), [], 1, 1)
        assert summary.all_successful is False

    def test_migration_summary_empty(self) -> None:
        summary = MigrationSummary(uuid4(), uuid4(), [], 0, 0)
        assert summary.all_successful is False

    def test_migration_summary_to_dict(self) -> None:
        migration_id, tenant_id, now = uuid4(), uuid4(), datetime.now(UTC)
        summary = MigrationSummary(
            migration_id, tenant_id, [], 1, 0, started_at=now, completed_at=now, duration_ms=100.0
        )
        d = summary.to_dict()
        assert d["migration_id"] == str(migration_id)
        assert d["all_successful"] is True
        assert d["duration_ms"] == 100.0


class TestSubscriptionMigrationErrorException:
    def test_exception_creation(self) -> None:
        migration_id = uuid4()
        error = SubscriptionMigrationError(
            "Migration failed", migration_id, "OrderProjection", "translation_failed"
        )
        assert error.subscription_name == "OrderProjection"
        assert error.reason == "translation_failed"
        assert error.migration_id == migration_id
        assert error.recoverable is True

    def test_exception_str(self) -> None:
        migration_id = uuid4()
        error = SubscriptionMigrationError("Migration failed", migration_id, "OrderProjection")
        assert "Migration failed" in str(error)
        assert str(migration_id) in str(error)


class TestSubscriptionMigratorWorkflows:
    @pytest.mark.asyncio
    async def test_full_migration_workflow(
        self,
        migrator: SubscriptionMigrator,
        mock_checkpoint_repo: MagicMock,
        mock_position_mapper: MagicMock,
    ) -> None:
        migration_id, tenant_id, event_id = uuid4(), uuid4(), uuid4()
        mock_checkpoint_repo.get_position = AsyncMock(return_value=source_pos(1000))
        mock_checkpoint_repo.get_all_checkpoints = AsyncMock(
            return_value=[make_checkpoint_data(last_event_id=event_id)]
        )
        mock_checkpoint_repo.save_position = AsyncMock()
        mock_position_mapper.translate_position = AsyncMock(
            return_value=exact_translation(1000, 500)
        )

        plan = await migrator.plan_migration(migration_id, tenant_id, ["OrderProjection"])
        assert plan.migratable_count == 1
        assert plan.planned_migrations[0].planned_target_position == target_pos(500)

        summary = await migrator.migrate_subscriptions(migration_id, tenant_id, ["OrderProjection"])
        assert summary.successful_count == 1
        assert summary.all_successful

        mock_checkpoint_repo.get_position = AsyncMock(return_value=source_pos(500))
        verification = await migrator.verify_migration(migration_id, ["OrderProjection"])
        assert verification["OrderProjection"] is True

    @pytest.mark.asyncio
    async def test_partial_failure_workflow(
        self,
        migrator: SubscriptionMigrator,
        mock_checkpoint_repo: MagicMock,
        mock_position_mapper: MagicMock,
    ) -> None:
        migration_id, tenant_id, event_id = uuid4(), uuid4(), uuid4()
        positions = {"OrderProjection": source_pos(1000), "FailingProjection": source_pos(2000)}
        mock_checkpoint_repo.get_position = AsyncMock(side_effect=lambda name: positions.get(name))
        mock_checkpoint_repo.get_all_checkpoints = AsyncMock(
            return_value=[
                make_checkpoint_data("OrderProjection", event_id, "OrderCreated", source_pos(1000)),
                make_checkpoint_data(
                    "FailingProjection", event_id, "OrderCreated", source_pos(2000)
                ),
            ]
        )
        mock_checkpoint_repo.save_position = AsyncMock()

        def translate_position_effect(migration_id, source_position, use_nearest=True):
            if source_position == source_pos(1000):
                return exact_translation(1000, 500)
            raise PositionMappingError(
                "No mapping found",
                migration_id=migration_id,
                source_position=source_position,
                reason="no_mapping",
            )

        mock_position_mapper.translate_position = AsyncMock(side_effect=translate_position_effect)
        summary = await migrator.migrate_subscriptions(
            migration_id, tenant_id, ["OrderProjection", "FailingProjection"]
        )
        assert summary.successful_count == 1
        assert summary.failed_count == 1
        assert not summary.all_successful
        mock_checkpoint_repo.save_position.assert_called_once()
