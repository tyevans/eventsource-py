"""Tests for SubscriptionMigrator subscription migration and verification."""

from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from eventsource.application.migration.exceptions import PositionMappingError
from eventsource.application.migration.subscription_migrator import (
    MigrationSummary,
    SubscriptionMigrationError,
    SubscriptionMigrator,
)
from eventsource.ports.checkpoints import CheckpointData
from eventsource.ports.positions import Position
from tests.unit.application.migration.test_subscription_migrator._fixtures import (
    exact_translation,
    make_checkpoint_data,
    source_pos,
    target_pos,
)


def _wire_exact_translation(mock_position_mapper: MagicMock, src: int = 1000, dst: int = 500):
    mock_position_mapper.translate_position = AsyncMock(return_value=exact_translation(src, dst))


def _wire_checkpoint_repo(
    mock_checkpoint_repo: MagicMock, cps: list[CheckpointData] | None = None, pos: int = 1000
) -> None:
    mock_checkpoint_repo.get_position = AsyncMock(return_value=source_pos(pos))
    if cps is not None:
        mock_checkpoint_repo.get_all_checkpoints = AsyncMock(return_value=cps)
    mock_checkpoint_repo.save_position = AsyncMock()


class TestSubscriptionMigratorMigrateSubscriptions:
    @pytest.mark.asyncio
    async def test_migrate_subscriptions_dry_run(
        self, migrator: SubscriptionMigrator, mock_checkpoint_repo, mock_position_mapper
    ) -> None:
        migration_id, tenant_id = uuid4(), uuid4()
        mock_checkpoint_repo.get_position = AsyncMock(return_value=source_pos(1000))
        _wire_exact_translation(mock_position_mapper)
        summary = await migrator.migrate_subscriptions(
            migration_id, tenant_id, ["OrderProjection"], dry_run=True
        )
        mock_checkpoint_repo.save_position.assert_not_called()
        assert isinstance(summary, MigrationSummary)
        assert summary.migration_id == migration_id and summary.successful_count == 1

    @pytest.mark.asyncio
    async def test_migrate_subscriptions_executes(
        self, migrator: SubscriptionMigrator, mock_checkpoint_repo, mock_position_mapper
    ) -> None:
        migration_id, tenant_id, event_id = uuid4(), uuid4(), uuid4()
        cp = CheckpointData("OrderProjection", event_id, "OrderCreated", position=source_pos(1000))
        _wire_checkpoint_repo(mock_checkpoint_repo, [cp])
        _wire_exact_translation(mock_position_mapper)
        summary = await migrator.migrate_subscriptions(
            migration_id, tenant_id, ["OrderProjection"], dry_run=False
        )
        mock_checkpoint_repo.save_position.assert_called_once_with(
            subscription_id="OrderProjection",
            position=target_pos(500),
            event_id=event_id,
            event_type="OrderCreated",
        )
        assert (summary.successful_count, summary.failed_count) == (1, 0) and summary.all_successful
        saved_position = mock_checkpoint_repo.save_position.call_args.kwargs["position"]
        assert isinstance(saved_position, Position) and not isinstance(saved_position, int)
        res = summary.results[0]
        assert res.source_position == source_pos(1000) and isinstance(res.source_position, Position)
        assert res.target_position == target_pos(500) and isinstance(res.target_position, Position)

    @pytest.mark.asyncio
    async def test_migrate_subscriptions_handles_missing_checkpoint_data(
        self, migrator: SubscriptionMigrator, mock_checkpoint_repo, mock_position_mapper
    ) -> None:
        cp = make_checkpoint_data(
            projection_name="OrderProjection", last_event_id=None, last_event_type=None
        )
        _wire_checkpoint_repo(mock_checkpoint_repo, [cp])
        summary = await migrator.migrate_subscriptions(
            uuid4(), uuid4(), ["OrderProjection"], dry_run=False
        )
        assert summary.skipped_count == 1 and summary.successful_count == 0

    @pytest.mark.asyncio
    async def test_migrate_subscriptions_handles_translation_error(
        self, migrator: SubscriptionMigrator, mock_checkpoint_repo, mock_position_mapper
    ) -> None:
        migration_id = uuid4()
        _wire_checkpoint_repo(mock_checkpoint_repo, [make_checkpoint_data(last_event_id=uuid4())])
        err = PositionMappingError(
            "No mapping found",
            migration_id=migration_id,
            source_position=source_pos(1000),
            reason="no_mapping",
        )
        mock_position_mapper.translate_position = AsyncMock(side_effect=err)
        summary = await migrator.migrate_subscriptions(
            migration_id, uuid4(), ["OrderProjection"], dry_run=False
        )
        assert summary.failed_count == 1 and summary.successful_count == 0
        assert not summary.all_successful and summary.results[0].error_message is not None

    @pytest.mark.asyncio
    async def test_migrate_subscriptions_handles_save_error(
        self, migrator: SubscriptionMigrator, mock_checkpoint_repo, mock_position_mapper
    ) -> None:
        _wire_checkpoint_repo(mock_checkpoint_repo, [make_checkpoint_data(last_event_id=uuid4())])
        mock_checkpoint_repo.save_position = AsyncMock(side_effect=Exception("Database error"))
        _wire_exact_translation(mock_position_mapper)
        summary = await migrator.migrate_subscriptions(
            uuid4(), uuid4(), ["OrderProjection"], dry_run=False
        )
        assert summary.failed_count == 1 and summary.results[0].error_message is not None
        assert "Database error" in summary.results[0].error_message

    @pytest.mark.asyncio
    async def test_migrate_subscriptions_raises_on_critical_checkpoint_repo_error(
        self, migrator: SubscriptionMigrator, mock_checkpoint_repo
    ) -> None:
        migration_id = uuid4()
        mock_checkpoint_repo.get_position = AsyncMock(
            side_effect=ConnectionError("checkpoint store unreachable")
        )
        with pytest.raises(SubscriptionMigrationError) as exc_info:
            await migrator.migrate_subscriptions(
                migration_id, uuid4(), ["OrderProjection"], dry_run=False
            )
        assert exc_info.value.subscription_name == "OrderProjection"
        assert exc_info.value.migration_id == migration_id
        assert "checkpoint store unreachable" in str(exc_info.value)


class TestSubscriptionMigratorMigrateTenantSubscriptions:
    @pytest.mark.asyncio
    async def test_migrate_tenant_with_explicit_names(
        self, migrator: SubscriptionMigrator, mock_checkpoint_repo, mock_position_mapper
    ) -> None:
        _wire_checkpoint_repo(mock_checkpoint_repo, [make_checkpoint_data(last_event_id=uuid4())])
        _wire_exact_translation(mock_position_mapper)
        summary = await migrator.migrate_tenant_subscriptions(uuid4(), uuid4(), ["OrderProjection"])
        assert summary.successful_count == 1

    @pytest.mark.asyncio
    async def test_migrate_tenant_discovers_subscriptions(
        self, migrator: SubscriptionMigrator, mock_checkpoint_repo, mock_position_mapper
    ) -> None:
        eid = uuid4()
        cps = [
            make_checkpoint_data("OrderProjection", eid, "OrderCreated", source_pos(1000)),
            make_checkpoint_data("InventoryProjection", eid, "InventoryUpdated", source_pos(2000)),
        ]
        _wire_checkpoint_repo(mock_checkpoint_repo, cps)
        _wire_exact_translation(mock_position_mapper)
        summary = await migrator.migrate_tenant_subscriptions(uuid4(), uuid4(), None)
        assert summary.successful_count == 2


class TestSubscriptionMigratorVerifyMigration:
    @pytest.mark.asyncio
    async def test_verify_migration_all_present(
        self, migrator: SubscriptionMigrator, mock_checkpoint_repo
    ) -> None:
        mock_checkpoint_repo.get_position = AsyncMock(return_value=source_pos(500))
        results = await migrator.verify_migration(
            uuid4(), ["OrderProjection", "InventoryProjection"]
        )
        assert all(results.values()) and len(results) == 2

    @pytest.mark.asyncio
    async def test_verify_migration_some_missing(
        self, migrator: SubscriptionMigrator, mock_checkpoint_repo
    ) -> None:
        positions = {"OrderProjection": source_pos(500), "InventoryProjection": None}
        mock_checkpoint_repo.get_position = AsyncMock(side_effect=lambda name: positions.get(name))
        results = await migrator.verify_migration(
            uuid4(), ["OrderProjection", "InventoryProjection"]
        )
        assert results["OrderProjection"] is True and results["InventoryProjection"] is False
