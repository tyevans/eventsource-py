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
        r = SubscriptionMigrationResult("OrderProjection", True, source_pos(1000), target_pos(500), True, datetime.now(UTC))  # fmt: skip
        assert (r.subscription_name, r.success) == ("OrderProjection", True)
        assert (r.source_position, r.target_position) == (source_pos(1000), target_pos(500))
        assert (r.is_exact_translation, r.error_message) == (True, None)

    def test_result_failure(self) -> None:
        r = SubscriptionMigrationResult("OrderProjection", False, source_pos(1000), error_message="Translation failed")  # fmt: skip
        assert (r.success, r.error_message, r.target_position) == (False, "Translation failed", None)  # fmt: skip

    def test_result_to_dict(self) -> None:
        now = datetime.now(UTC)
        d = SubscriptionMigrationResult("OrderProjection", True, source_pos(1000), target_pos(500), migrated_at=now).to_dict()  # fmt: skip
        assert (d["subscription_name"], d["success"]) == ("OrderProjection", True)
        assert d["source_position"] == source_pos(1000).to_str()
        assert d["target_position"] == target_pos(500).to_str()
        assert d["migrated_at"] == now.isoformat()

    def test_result_frozen(self) -> None:
        result = SubscriptionMigrationResult("OrderProjection", True, source_pos(1000))
        with pytest.raises(AttributeError):
            result.success = False  # type: ignore[misc]


class TestPlannedMigrationDataclass:
    def test_planned_migration_exact(self) -> None:
        p = PlannedMigration("OrderProjection", source_pos(1000), target_pos(500), True)
        assert (p.subscription_name, p.is_exact_translation, p.warning) == ("OrderProjection", True, None)  # fmt: skip
        assert (p.current_position, p.planned_target_position) == (source_pos(1000), target_pos(500))  # fmt: skip

    def test_planned_migration_with_warning(self) -> None:
        p = PlannedMigration("OrderProjection", source_pos(1050), target_pos(500), False, source_pos(1000), "Warn")  # fmt: skip
        assert p.is_exact_translation is False and p.nearest_source_position == source_pos(1000)
        assert p.warning is not None

    def test_planned_migration_to_dict(self) -> None:
        d = PlannedMigration("OrderProjection", source_pos(1000), target_pos(500)).to_dict()
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
        assert (plan.migration_id, plan.tenant_id) == (migration_id, tenant_id)
        assert plan.migratable_count == 1 and len(plan.skipped_subscriptions) == 1

    def test_migration_plan_to_dict(self) -> None:
        migration_id, tenant_id = uuid4(), uuid4()
        d = MigrationPlan(migration_id, tenant_id, [], 0, 0).to_dict()
        assert (d["migration_id"], d["tenant_id"]) == (str(migration_id), str(tenant_id))
        assert isinstance(d["created_at"], str)


class TestMigrationSummaryDataclass:
    def test_migration_summary_creation(self) -> None:
        migration_id, tenant_id = uuid4(), uuid4()
        res = SubscriptionMigrationResult("OrderProjection", True, source_pos(1000), target_pos(500))  # fmt: skip
        summary = MigrationSummary(migration_id, tenant_id, [res], 1, 0, duration_ms=100.0)
        assert (summary.migration_id, summary.all_successful) == (migration_id, True)

    def test_migration_summary_not_all_successful(self) -> None:
        assert MigrationSummary(uuid4(), uuid4(), [], 1, 1).all_successful is False

    def test_migration_summary_empty(self) -> None:
        assert MigrationSummary(uuid4(), uuid4(), [], 0, 0).all_successful is False

    def test_migration_summary_to_dict(self) -> None:
        migration_id, tenant_id, now = uuid4(), uuid4(), datetime.now(UTC)
        d = MigrationSummary(migration_id, tenant_id, [], 1, 0, started_at=now, completed_at=now, duration_ms=100.0).to_dict()  # fmt: skip
        assert (d["migration_id"], d["all_successful"], d["duration_ms"]) == (str(migration_id), True, 100.0)  # fmt: skip


class TestSubscriptionMigrationErrorException:
    def test_exception_creation(self) -> None:
        migration_id = uuid4()
        e = SubscriptionMigrationError("Migration failed", migration_id, "OrderProjection", "translation_failed")  # fmt: skip
        assert (e.subscription_name, e.reason) == ("OrderProjection", "translation_failed")
        assert (e.migration_id, e.recoverable) == (migration_id, True)

    def test_exception_str(self) -> None:
        migration_id = uuid4()
        error = SubscriptionMigrationError("Migration failed", migration_id, "OrderProjection")
        assert "Migration failed" in str(error) and str(migration_id) in str(error)


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
        mock_checkpoint_repo.get_all_checkpoints = AsyncMock(return_value=[make_checkpoint_data(last_event_id=event_id)])  # fmt: skip
        mock_checkpoint_repo.save_position = AsyncMock()
        mock_position_mapper.translate_position = AsyncMock(return_value=exact_translation(1000, 500))  # fmt: skip

        plan = await migrator.plan_migration(migration_id, tenant_id, ["OrderProjection"])
        assert plan.migratable_count == 1
        assert plan.planned_migrations[0].planned_target_position == target_pos(500)

        summary = await migrator.migrate_subscriptions(migration_id, tenant_id, ["OrderProjection"])
        assert summary.successful_count == 1 and summary.all_successful

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
                ),  # fmt: skip
            ]
        )
        mock_checkpoint_repo.save_position = AsyncMock()

        def translate_position_effect(migration_id, source_position, use_nearest=True):
            if source_position == source_pos(1000):
                return exact_translation(1000, 500)
            raise PositionMappingError("No mapping found", migration_id, source_position, "no_mapping")  # fmt: skip

        mock_position_mapper.translate_position = AsyncMock(side_effect=translate_position_effect)
        summary = await migrator.migrate_subscriptions(
            migration_id, tenant_id, ["OrderProjection", "FailingProjection"]
        )
        assert (summary.successful_count, summary.failed_count, summary.all_successful) == (1, 1, False)  # fmt: skip
        mock_checkpoint_repo.save_position.assert_called_once()
