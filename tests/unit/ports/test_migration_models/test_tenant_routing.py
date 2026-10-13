"""Tests for TenantRouting dataclass."""

from uuid import uuid4

from eventsource.ports.migration.models import TenantMigrationState, TenantRouting


class TestTenantRouting:
    """Tests for TenantRouting dataclass."""

    def test_creation(self) -> None:
        """Test creating a TenantRouting."""
        tenant_id = uuid4()
        routing = TenantRouting(
            tenant_id=tenant_id,
            store_id="shared",
        )
        assert routing.tenant_id == tenant_id
        assert routing.store_id == "shared"
        assert routing.migration_state == TenantMigrationState.NORMAL
        assert routing.active_migration_id is None

    def test_is_migrating(self) -> None:
        """Test is_migrating property."""
        routing = TenantRouting(
            tenant_id=uuid4(),
            store_id="shared",
            migration_state=TenantMigrationState.DUAL_WRITE,
        )
        assert routing.is_migrating is True

        routing.migration_state = TenantMigrationState.NORMAL
        assert routing.is_migrating is False

    def test_effective_store_id_normal(self) -> None:
        """Test effective_store_id in NORMAL state."""
        routing = TenantRouting(
            tenant_id=uuid4(),
            store_id="shared",
            migration_state=TenantMigrationState.NORMAL,
        )
        assert routing.effective_store_id == "shared"

    def test_effective_store_id_migrated(self) -> None:
        """Test effective_store_id after migration."""
        routing = TenantRouting(
            tenant_id=uuid4(),
            store_id="shared",
            target_store_id="dedicated",
            migration_state=TenantMigrationState.MIGRATED,
        )
        assert routing.effective_store_id == "dedicated"

    def test_effective_store_id_migrated_no_target(self) -> None:
        """Test effective_store_id falls back to store_id if no target."""
        routing = TenantRouting(
            tenant_id=uuid4(),
            store_id="shared",
            migration_state=TenantMigrationState.MIGRATED,
        )
        assert routing.effective_store_id == "shared"

    def test_can_transition_to(self) -> None:
        """Test can_transition_to delegates to migration_state."""
        routing = TenantRouting(
            tenant_id=uuid4(),
            store_id="shared",
            migration_state=TenantMigrationState.NORMAL,
        )
        assert routing.can_transition_to(TenantMigrationState.BULK_COPY) is True
        assert routing.can_transition_to(TenantMigrationState.MIGRATED) is False
