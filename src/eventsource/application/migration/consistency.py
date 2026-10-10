"""
ConsistencyVerifier - Verifies data integrity between stores.

The ConsistencyVerifier ensures data integrity during migration by
comparing events between source and target stores. It performs both
count-based and hash-based verification to detect any inconsistencies.

This module is implemented as part of task P3-003.

Responsibilities:
    - Verify event counts match between stores
    - Verify event content matches (optional hash verification)
    - Identify specific streams with inconsistencies
    - Report detailed verification results
    - Support incremental verification for large datasets
    - Support sampling for performance with large datasets

Verification Levels:
    - COUNT: Verify event counts per stream (fast)
    - HASH: Verify event content hashes (thorough)
    - FULL: Verify complete event data (slowest, most thorough)

Usage:
    >>> from eventsource.application.migration import ConsistencyVerifier
    >>>
    >>> verifier = ConsistencyVerifier(source_store, target_store)
    >>>
    >>> # Verify consistency for tenant
    >>> result = await verifier.verify_tenant_consistency(
    ...     tenant_id=tenant_id,
    ...     level=VerificationLevel.HASH,
    ... )
    >>>
    >>> if result.is_consistent:
    ...     print("Verification passed")
    ... else:
    ...     for violation in result.violations:
    ...         print(f"Mismatch: {violation}")

See Also:
    - Task: P3-003-consistency-verifier.md
    - FRD: docs/tasks/multi-tenant-live-migration/multi-tenant-live-migration.md
"""

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
    """
    Verifies data consistency between source and target stores.

    Ensures data integrity during migration through count and hash
    verification, preventing cutover with inconsistent data.

    The verifier supports three levels of verification:
    - COUNT: Fast count-based verification
    - HASH: SHA-256 hash verification of event content
    - FULL: Complete event data comparison

    For large datasets, sampling can be used to verify a percentage
    of events while maintaining statistical confidence.

    Example:
        >>> verifier = ConsistencyVerifier(source_store, target_store)
        >>>
        >>> # Full verification
        >>> report = await verifier.verify_tenant_consistency(
        ...     tenant_id=tenant_id,
        ...     level=VerificationLevel.HASH,
        ... )
        >>>
        >>> # Sampled verification for large tenants
        >>> report = await verifier.verify_tenant_consistency(
        ...     tenant_id=tenant_id,
        ...     level=VerificationLevel.HASH,
        ...     sample_percentage=10.0,  # Verify 10% of events
        ... )
        >>>
        >>> if not report.is_consistent:
        ...     for violation in report.violations:
        ...         logger.error("Consistency violation: %s", violation)

    Attributes:
        _source: Source event store to verify from.
        _target: Target event store to verify against.
    """

    def __init__(
        self,
        source_store: FullEventStore,
        target_store: FullEventStore,
        *,
        tracer: Tracer | None = None,
        enable_tracing: bool = True,
    ) -> None:
        """
        Initialize the consistency verifier.

        Args:
            source_store: FullEventStore to verify from (source of truth).
            target_store: FullEventStore to verify against (migration target).
            tracer: Optional custom Tracer instance. If not provided, one is
                   created based on enable_tracing setting.
            enable_tracing: Whether to enable OpenTelemetry tracing.
                          Ignored if tracer is explicitly provided.
        """
        # Composition-based tracing (replaces TracingMixin)
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
        """
        Verify consistency of all data for a tenant.

        Compares events between source and target stores to ensure
        all data has been correctly migrated.

        Args:
            tenant_id: The tenant UUID to verify.
            level: Verification thoroughness level.
            sample_percentage: Percentage of events to sample (1-100).
                Use 100 for complete verification, lower values for
                faster verification of large datasets.

        Returns:
            VerificationReport with detailed results.

        Raises:
            ValueError: If sample_percentage is invalid.
            ConsistencyError: If verification fails due to store errors.
        """
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
                # Get all events from source and target
                source_events = await self._collect_tenant_events(self._source, tenant_id)
                target_events = await self._collect_tenant_events(self._target, tenant_id)

                source_count = len(source_events)
                target_count = len(target_events)

                # Check total counts
                if source_count != target_count:
                    violations.append(
                        ConsistencyViolation(
                            violation_type="total_count_mismatch",
                            source_value=str(source_count),
                            target_value=str(target_count),
                            details=f"Expected {source_count} events, found {target_count}",
                        )
                    )

                # Group events by stream
                source_by_stream = self._group_events_by_stream(source_events)
                target_by_stream = self._group_events_by_stream(target_events)

                # Find all streams
                all_streams = set(source_by_stream.keys()) | set(target_by_stream.keys())

                # Verify each stream
                for stream_id in all_streams:
                    source_stream_events = source_by_stream.get(stream_id, [])
                    target_stream_events = target_by_stream.get(stream_id, [])

                    stream_result, stream_violations = await self._verify_stream(
                        stream_id,
                        source_stream_events,
                        target_stream_events,
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
                        "Consistency verification passed for tenant %s: "
                        "%d events, %d streams in %.2fs",
                        tenant_id,
                        source_count,
                        len(stream_results),
                        duration,
                    )
                else:
                    logger.warning(
                        "Consistency verification FAILED for tenant %s: "
                        "%d violations found in %.2fs",
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
        """
        Verify event checksums match between stores.

        Computes SHA-256 hashes of event content and compares them.
        This is a convenience method that performs HASH-level verification.

        Args:
            tenant_id: The tenant UUID to verify.
            sample_percentage: Percentage of events to sample.

        Returns:
            Tuple of (all_match, violations_list).
        """
        with self._tracer.span(
            "eventsource.consistency_verifier.verify_checksums",
            {"tenant_id": str(tenant_id), "sample_percentage": sample_percentage},
        ):
            report = await self.verify_tenant_consistency(
                tenant_id,
                level=VerificationLevel.HASH,
                sample_percentage=sample_percentage,
            )

            # Filter to only hash-related violations
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
        """
        Verify aggregate versions are consistent.

        Checks that each aggregate has the same version (event count)
        in both source and target stores.

        Args:
            tenant_id: The tenant UUID to verify.

        Returns:
            Tuple of (all_match, violations_list).
        """
        with self._tracer.span(
            "eventsource.consistency_verifier.verify_aggregate_versions",
            {"tenant_id": str(tenant_id)},
        ):
            report = await self.verify_tenant_consistency(
                tenant_id,
                level=VerificationLevel.COUNT,
                sample_percentage=100.0,
            )

            # Filter to only version-related violations
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
