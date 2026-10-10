"""Rollback operations mixin for CutoverManager."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING
from uuid import UUID

from eventsource.observability import ATTR_TENANT_ID, Tracer
from eventsource.observability.attributes import ATTR_MIGRATION_ID
from eventsource.ports.migration.models import TenantMigrationState

if TYPE_CHECKING:
    from eventsource.ports.migration.repositories import TenantRoutingRepository

logger = logging.getLogger(__name__)

ATTR_CUTOVER_MIGRATION_ID = "eventsource.cutover.migration_id"


class CutoverRollbackMixin:
    """Mixin providing rollback logic after cutover failure."""

    _tracer: Tracer
    _routing_repo: TenantRoutingRepository

    async def _rollback(
        self,
        tenant_id: UUID,
        migration_id: UUID,
        source_store_id: str | None,
    ) -> bool:
        """
        Rollback to dual-write state after cutover failure.

        Restores *both* halves of the switch: the route back to the source
        store and the migration state to DUAL_WRITE, so the migration can
        continue or be retried. Restoring only the state -- the previous
        behavior -- declared the tenant DUAL_WRITE while leaving its traffic
        on the target store.

        Args:
            tenant_id: Tenant being migrated.
            migration_id: ID of the migration.
            source_store_id: The store the tenant was routed to before the
                cutover began, or None if no routing existed to restore.

        Returns:
            True if rollback was successful, False otherwise.
        """
        with self._tracer.span(
            "eventsource.cutover.rollback",
            {
                ATTR_TENANT_ID: str(tenant_id),
                ATTR_CUTOVER_MIGRATION_ID: str(migration_id),
                ATTR_MIGRATION_ID: str(migration_id),
            },
        ):
            try:
                # Only rewrite the route if it actually moved. Most cutover
                # failures happen before step 9, and an unconditional write
                # would turn a repository outage during validation into a
                # failed rollback.
                if source_store_id is not None:
                    current = await self._routing_repo.get_routing(tenant_id)
                    if current is not None and current.store_id != source_store_id:
                        if hasattr(self._routing_repo, "switch_routing"):
                            await self._routing_repo.switch_routing(
                                tenant_id,
                                source_store_id,
                                state=TenantMigrationState.DUAL_WRITE,
                                migration_id=migration_id,
                            )
                        else:
                            await self._routing_repo.set_routing(tenant_id, source_store_id)
                            await self._routing_repo.set_migration_state(
                                tenant_id,
                                TenantMigrationState.DUAL_WRITE,
                                migration_id=migration_id,
                            )
                        logger.info(
                            "Rolled back tenant %s to DUAL_WRITE state on store %s",
                            tenant_id,
                            source_store_id,
                        )
                        return True

                await self._routing_repo.set_migration_state(
                    tenant_id,
                    TenantMigrationState.DUAL_WRITE,
                    migration_id=migration_id,
                )
                logger.info(
                    "Rolled back tenant %s to DUAL_WRITE state on store %s",
                    tenant_id,
                    source_store_id,
                )
                return True

            except Exception as e:
                logger.error(
                    "Failed to rollback tenant %s: %s",
                    tenant_id,
                    e,
                )
                return False


__all__ = [
    "CutoverRollbackMixin",
]
