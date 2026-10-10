"""
Subscription migration post-cutover verification operations.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING
from uuid import UUID

from eventsource.application.subscriptions.subscription import render_position

if TYPE_CHECKING:
    from eventsource.observability import Tracer
    from eventsource.ports.checkpoints import CheckpointRepository

logger = logging.getLogger(__name__)


class SubscriptionMigratorVerificationMixin:
    """Mixin providing subscription migration checkpoint verification."""

    _checkpoint_repo: CheckpointRepository
    _tracer: Tracer

    async def verify_migration(
        self,
        migration_id: UUID,
        subscription_names: list[str],
    ) -> dict[str, bool]:
        """
        Verify that subscription checkpoints were migrated correctly.

        Checks that each subscription's checkpoint position exists
        and is valid after migration.

        Args:
            migration_id: ID of the migration.
            subscription_names: Names of subscriptions to verify.

        Returns:
            Dictionary mapping subscription names to verification status.

        Example:
            >>> verification = await migrator.verify_migration(
            ...     migration_id=migration_id,
            ...     subscription_names=["OrderProjection"],
            ... )
            >>> all_verified = all(verification.values())
        """
        with self._tracer.span(
            "eventsource.subscription_migrator.verify_migration",
            {
                "migration.id": str(migration_id),
                "subscription_count": len(subscription_names),
            },
        ):
            results: dict[str, bool] = {}

            for name in subscription_names:
                position = await self._checkpoint_repo.get_position(name)
                results[name] = position is not None

                if position is None:
                    logger.warning(
                        "Subscription checkpoint not found after migration",
                        extra={
                            "subscription": name,
                            "migration_id": str(migration_id),
                        },
                    )
                else:
                    logger.debug(
                        "Subscription checkpoint verified",
                        extra={
                            "subscription": name,
                            "migration_id": str(migration_id),
                            "position": render_position(position),
                        },
                    )

            verified_count = sum(1 for v in results.values() if v)
            logger.info(
                "Migration verification completed",
                extra={
                    "migration_id": str(migration_id),
                    "verified_count": verified_count,
                    "total_count": len(subscription_names),
                },
            )

            return results


__all__ = ["SubscriptionMigratorVerificationMixin"]
