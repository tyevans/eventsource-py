"""ConsistencyVerifier - Verifies data integrity between stores (P3-003)."""

from __future__ import annotations

import logging
import time
from datetime import UTC, datetime
from uuid import UUID

from eventsource.application.migration.consistency_models import (
    ConsistencyViolation,
    StreamConsistency,
    VerificationLevel,
    VerificationReport,
)
from eventsource.application.migration.consistency_stream_checker import (
    ConsistencyStreamCheckerMixin,
)
from eventsource.application.migration.exceptions import ConsistencyError
from eventsource.observability import Tracer, create_tracer
from eventsource.ports import FullEventStore

logger = logging.getLogger(__name__)


class ConsistencyVerifier(ConsistencyStreamCheckerMixin):
    """Verifies data consistency between source and target stores (COUNT, HASH, FULL)."""

    def __init__(
        self,
        source_store: FullEventStore,
        target_store: FullEventStore,
        *,
        tracer: Tracer | None = None,
        enable_tracing: bool = True,
    ) -> None:
        """Initialize the consistency verifier with source and target event stores."""
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled
        self._source = source_store
        self._target = target_store

    async def verify_tenant_consistency(
        self,
        tenant_id: UUID,
        level: VerificationLevel = VerificationLevel.HASH,
        sample_percentage: float = 100.0,
    ) -> VerificationReport:
        """Verify consistency of all data for a tenant between source and target stores."""
        if not 0 < sample_percentage <= 100:
            raise ValueError(
                f"sample_percentage must be between 0 and 100, got {sample_percentage}"
            )

        with self._tracer.span(
            "eventsource.consistency_verifier.verify_tenant",
            {
                "tenant_id": str(tenant_id),
                "level": level.value,
                "sample_percentage": sample_percentage,
            },
        ):
            start_time = time.monotonic()
            logger.info(
                "Starting consistency verification for tenant %s, level=%s, sample=%.1f%%",
                tenant_id,
                level.value,
                sample_percentage,
            )

            violations: list[ConsistencyViolation] = []
            stream_results: list[StreamConsistency] = []

            try:
                source_events = await self._collect_tenant_events(self._source, tenant_id)
                target_events = await self._collect_tenant_events(self._target, tenant_id)
                source_count, target_count = len(source_events), len(target_events)

                if source_count != target_count:
                    violations.append(
                        ConsistencyViolation(
                            violation_type="total_count_mismatch",
                            source_value=str(source_count),
                            target_value=str(target_count),
                            details=f"Expected {source_count} events, found {target_count}",
                        )
                    )

                source_by_stream = self._group_events_by_stream(source_events)
                target_by_stream = self._group_events_by_stream(target_events)
                all_streams = set(source_by_stream.keys()) | set(target_by_stream.keys())

                for stream_id in all_streams:
                    stream_result, stream_violations = await self._verify_stream(
                        stream_id,
                        source_by_stream.get(stream_id, []),
                        target_by_stream.get(stream_id, []),
                        level,
                        sample_percentage,
                    )
                    stream_results.append(stream_result)
                    violations.extend(stream_violations)

                duration = time.monotonic() - start_time
                streams_consistent = sum(1 for s in stream_results if s.is_consistent)
                is_consistent = len(violations) == 0

                report = VerificationReport(
                    tenant_id=tenant_id,
                    verification_level=level,
                    is_consistent=is_consistent,
                    source_event_count=source_count,
                    target_event_count=target_count,
                    streams_verified=len(stream_results),
                    streams_consistent=streams_consistent,
                    streams_inconsistent=len(stream_results) - streams_consistent,
                    sample_percentage=sample_percentage,
                    violations=violations,
                    stream_results=stream_results,
                    duration_seconds=duration,
                    verified_at=datetime.now(UTC),
                )

                if is_consistent:
                    logger.info(
                        "Consistency verification passed for tenant %s: %d events, %d streams in %.2fs",
                        tenant_id,
                        source_count,
                        len(stream_results),
                        duration,
                    )
                else:
                    logger.warning(
                        "Consistency verification FAILED for tenant %s: %d violations found in %.2fs",
                        tenant_id,
                        len(violations),
                        duration,
                    )

                return report

            except Exception as e:
                logger.error("Consistency verification error for tenant %s: %s", tenant_id, e)
                raise ConsistencyError(
                    message=f"Verification failed: {e}",
                    migration_id=UUID("00000000-0000-0000-0000-000000000000"),
                    details=str(e),
                ) from e

    async def verify_event_checksums(
        self,
        tenant_id: UUID,
        sample_percentage: float = 100.0,
    ) -> tuple[bool, list[ConsistencyViolation]]:
        """Verify event SHA-256 checksums match between stores."""
        with self._tracer.span(
            "eventsource.consistency_verifier.verify_checksums",
            {"tenant_id": str(tenant_id), "sample_percentage": sample_percentage},
        ):
            report = await self.verify_tenant_consistency(
                tenant_id, level=VerificationLevel.HASH, sample_percentage=sample_percentage
            )
            hash_violations = [
                v
                for v in report.violations
                if v.violation_type in ("hash_mismatch", "event_missing")
            ]
            return len(hash_violations) == 0, hash_violations

    async def verify_aggregate_versions(
        self,
        tenant_id: UUID,
    ) -> tuple[bool, list[ConsistencyViolation]]:
        """Verify aggregate versions (event counts) are consistent across stores."""
        with self._tracer.span(
            "eventsource.consistency_verifier.verify_aggregate_versions",
            {"tenant_id": str(tenant_id)},
        ):
            report = await self.verify_tenant_consistency(
                tenant_id, level=VerificationLevel.COUNT, sample_percentage=100.0
            )
            version_violations = [
                v
                for v in report.violations
                if v.violation_type in ("version_mismatch", "count_mismatch", "stream_missing")
            ]
            return len(version_violations) == 0, version_violations


__all__ = [
    "ConsistencyVerifier",
    "ConsistencyViolation",
    "StreamConsistency",
    "VerificationLevel",
    "VerificationReport",
]
