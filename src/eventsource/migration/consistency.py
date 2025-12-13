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
    >>> from eventsource.migration import ConsistencyVerifier
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

import hashlib
import logging
import random
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import TYPE_CHECKING, Any
from uuid import UUID

from eventsource.migration.exceptions import ConsistencyError
from eventsource.observability import Tracer, create_tracer
from eventsource.stores.interface import ReadOptions, StoredEvent

if TYPE_CHECKING:
    from eventsource.stores.interface import EventStore

logger = logging.getLogger(__name__)


class VerificationLevel(Enum):
    """
    Verification thoroughness levels.

    Each level provides different trade-offs between speed and thoroughness:

    Attributes:
        COUNT: Verify event counts only (fastest).
            Checks that source and target have the same number of events
            per aggregate stream. Does not verify content.

        HASH: Verify event content via hashing (balanced).
            Computes SHA-256 hashes of event data and compares them.
            Good balance of speed and thoroughness.

        FULL: Verify complete event data (slowest, most thorough).
            Compares all event fields directly. Most thorough but
            slowest for large datasets.
    """

    COUNT = "count"
    """Verify event counts only (fastest)."""

    HASH = "hash"
    """Verify event content via hashing (balanced)."""

    FULL = "full"
    """Verify complete event data (slowest, most thorough)."""


@dataclass(frozen=True)
class StreamConsistency:
    """
    Consistency status for a single stream (aggregate).

    Attributes:
        stream_id: The stream identifier (aggregate_id:aggregate_type).
        aggregate_id: The aggregate UUID.
        aggregate_type: The aggregate type name.
        source_count: Number of events in source store.
        target_count: Number of events in target store.
        source_version: Latest version in source store.
        target_version: Latest version in target store.
        is_consistent: Whether the stream is consistent.
        hash_match: Whether content hashes match (if hash verification done).
        mismatched_positions: List of positions with mismatches (for FULL level).
    """

    stream_id: str
    aggregate_id: UUID
    aggregate_type: str
    source_count: int
    target_count: int
    source_version: int
    target_version: int
    is_consistent: bool
    hash_match: bool | None = None
    mismatched_positions: list[int] = field(default_factory=list)

    @property
    def count_mismatch(self) -> int:
        """Get the difference in event counts."""
        return abs(self.source_count - self.target_count)

    @property
    def version_mismatch(self) -> int:
        """Get the difference in versions."""
        return abs(self.source_version - self.target_version)


@dataclass(frozen=True)
class ConsistencyViolation:
    """
    Represents a specific consistency violation.

    Provides detailed information about what inconsistency was detected
    to help with debugging and remediation.

    Attributes:
        violation_type: Type of violation detected.
        stream_id: Stream where violation occurred (if applicable).
        source_value: Value in source store.
        target_value: Value in target store.
        position: Event position where violation occurred (if applicable).
        details: Additional details about the violation.
    """

    violation_type: str
    stream_id: str | None = None
    source_value: str | None = None
    target_value: str | None = None
    position: int | None = None
    details: str | None = None

    def __str__(self) -> str:
        """Human-readable violation description."""
        parts = [f"[{self.violation_type}]"]
        if self.stream_id:
            parts.append(f"stream={self.stream_id}")
        if self.position is not None:
            parts.append(f"position={self.position}")
        if self.source_value is not None and self.target_value is not None:
            parts.append(f"source={self.source_value}, target={self.target_value}")
        if self.details:
            parts.append(f"({self.details})")
        return " ".join(parts)


@dataclass(frozen=True)
class VerificationReport:
    """
    Complete verification report for a tenant migration.

    Provides comprehensive results of consistency verification including
    counts, violations, and statistics for monitoring and debugging.

    Attributes:
        tenant_id: The tenant that was verified.
        verification_level: The level of verification performed.
        is_consistent: Whether all data is consistent.
        source_event_count: Total events in source store.
        target_event_count: Total events in target store.
        streams_verified: Number of streams (aggregates) verified.
        streams_consistent: Number of consistent streams.
        streams_inconsistent: Number of inconsistent streams.
        sample_percentage: Percentage of events sampled (100 for full verification).
        violations: List of specific violations found.
        stream_results: Detailed results per stream.
        duration_seconds: Time taken for verification.
        verified_at: When verification was performed.
    """

    tenant_id: UUID
    verification_level: VerificationLevel
    is_consistent: bool
    source_event_count: int
    target_event_count: int
    streams_verified: int
    streams_consistent: int
    streams_inconsistent: int
    sample_percentage: float
    violations: list[ConsistencyViolation]
    stream_results: list[StreamConsistency]
    duration_seconds: float
    verified_at: datetime

    @property
    def event_count_match(self) -> bool:
        """Check if total event counts match."""
        return self.source_event_count == self.target_event_count

    @property
    def consistency_percentage(self) -> float:
        """Calculate percentage of consistent streams."""
        if self.streams_verified == 0:
            return 100.0
        return (self.streams_consistent / self.streams_verified) * 100

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "tenant_id": str(self.tenant_id),
            "verification_level": self.verification_level.value,
            "is_consistent": self.is_consistent,
            "source_event_count": self.source_event_count,
            "target_event_count": self.target_event_count,
            "streams_verified": self.streams_verified,
            "streams_consistent": self.streams_consistent,
            "streams_inconsistent": self.streams_inconsistent,
            "sample_percentage": self.sample_percentage,
            "violations": [str(v) for v in self.violations],
            "duration_seconds": self.duration_seconds,
            "verified_at": self.verified_at.isoformat(),
        }


class ConsistencyVerifier:
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
        source_store: EventStore,
        target_store: EventStore,
        *,
        tracer: Tracer | None = None,
        enable_tracing: bool = True,
    ) -> None:
        """
        Initialize the consistency verifier.

        Args:
            source_store: EventStore to verify from (source of truth).
            target_store: EventStore to verify against (migration target).
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

    async def _collect_tenant_events(
        self,
        store: EventStore,
        tenant_id: UUID,
    ) -> list[StoredEvent]:
        """
        Collect all events for a tenant from a store.

        Args:
            store: The event store to read from.
            tenant_id: The tenant UUID.

        Returns:
            List of StoredEvent instances.
        """
        events: list[StoredEvent] = []
        options = ReadOptions(tenant_id=tenant_id)

        async for event in store.read_all(options):
            events.append(event)

        return events

    def _group_events_by_stream(
        self,
        events: list[StoredEvent],
    ) -> dict[str, list[StoredEvent]]:
        """
        Group events by their stream ID.

        Args:
            events: List of events to group.

        Returns:
            Dictionary mapping stream_id to list of events.
        """
        grouped: dict[str, list[StoredEvent]] = {}

        for event in events:
            stream_id = event.stream_id
            if stream_id not in grouped:
                grouped[stream_id] = []
            grouped[stream_id].append(event)

        # Sort events within each stream by position
        for stream_id in grouped:
            grouped[stream_id].sort(key=lambda e: e.stream_position)

        return grouped

    async def _verify_stream(
        self,
        stream_id: str,
        source_events: list[StoredEvent],
        target_events: list[StoredEvent],
        level: VerificationLevel,
        sample_percentage: float,
    ) -> tuple[StreamConsistency, list[ConsistencyViolation]]:
        """
        Verify a single stream's consistency.

        Args:
            stream_id: The stream identifier.
            source_events: Events from source store.
            target_events: Events from target store.
            level: Verification level.
            sample_percentage: Percentage to sample.

        Returns:
            Tuple of (StreamConsistency, list of violations).
        """
        violations: list[ConsistencyViolation] = []
        mismatched_positions: list[int] = []

        # Parse stream_id to get aggregate info
        parts = stream_id.rsplit(":", 1)
        if len(parts) == 2:
            aggregate_id = UUID(parts[0])
            aggregate_type = parts[1]
        else:
            # Fallback for malformed stream_id
            aggregate_id = UUID("00000000-0000-0000-0000-000000000000")
            aggregate_type = stream_id

        source_count = len(source_events)
        target_count = len(target_events)
        source_version = source_events[-1].stream_position if source_events else 0
        target_version = target_events[-1].stream_position if target_events else 0

        # Check if stream is missing
        if source_count > 0 and target_count == 0:
            violations.append(
                ConsistencyViolation(
                    violation_type="stream_missing",
                    stream_id=stream_id,
                    source_value=str(source_count),
                    target_value="0",
                    details="Stream exists in source but not in target",
                )
            )
        elif source_count == 0 and target_count > 0:
            violations.append(
                ConsistencyViolation(
                    violation_type="stream_extra",
                    stream_id=stream_id,
                    source_value="0",
                    target_value=str(target_count),
                    details="Stream exists in target but not in source",
                )
            )

        # Count verification
        if source_count != target_count:
            violations.append(
                ConsistencyViolation(
                    violation_type="count_mismatch",
                    stream_id=stream_id,
                    source_value=str(source_count),
                    target_value=str(target_count),
                    details=f"Event count mismatch: {source_count} vs {target_count}",
                )
            )

        # Version verification
        if source_version != target_version:
            violations.append(
                ConsistencyViolation(
                    violation_type="version_mismatch",
                    stream_id=stream_id,
                    source_value=str(source_version),
                    target_value=str(target_version),
                    details=f"Version mismatch: {source_version} vs {target_version}",
                )
            )

        # Hash or full verification (if requested and counts match)
        hash_match: bool | None = None
        if (
            level in (VerificationLevel.HASH, VerificationLevel.FULL)
            and source_count == target_count
            and source_count > 0
        ):
            # Apply sampling
            events_to_verify = self._sample_events(source_events, target_events, sample_percentage)

            for source_event, target_event in events_to_verify:
                if level == VerificationLevel.HASH:
                    source_hash = self._compute_event_hash(source_event)
                    target_hash = self._compute_event_hash(target_event)

                    if source_hash != target_hash:
                        hash_match = False
                        mismatched_positions.append(source_event.stream_position)
                        violations.append(
                            ConsistencyViolation(
                                violation_type="hash_mismatch",
                                stream_id=stream_id,
                                source_value=source_hash[:16],
                                target_value=target_hash[:16],
                                position=source_event.stream_position,
                                details="Event content hash mismatch",
                            )
                        )
                elif level == VerificationLevel.FULL:
                    mismatch = self._compare_events_full(source_event, target_event)
                    if mismatch:
                        mismatched_positions.append(source_event.stream_position)
                        violations.append(
                            ConsistencyViolation(
                                violation_type="content_mismatch",
                                stream_id=stream_id,
                                position=source_event.stream_position,
                                details=mismatch,
                            )
                        )

            if hash_match is None and level == VerificationLevel.HASH:
                hash_match = True  # All verified events matched

        is_consistent = len(violations) == 0

        return (
            StreamConsistency(
                stream_id=stream_id,
                aggregate_id=aggregate_id,
                aggregate_type=aggregate_type,
                source_count=source_count,
                target_count=target_count,
                source_version=source_version,
                target_version=target_version,
                is_consistent=is_consistent,
                hash_match=hash_match,
                mismatched_positions=mismatched_positions,
            ),
            violations,
        )

    def _sample_events(
        self,
        source_events: list[StoredEvent],
        target_events: list[StoredEvent],
        sample_percentage: float,
    ) -> list[tuple[StoredEvent, StoredEvent]]:
        """
        Sample events for verification.

        When sample_percentage < 100, randomly selects a subset of events
        to verify while ensuring statistical coverage.

        Args:
            source_events: Events from source store.
            target_events: Events from target store.
            sample_percentage: Percentage to sample.

        Returns:
            List of (source_event, target_event) tuples to verify.
        """
        if len(source_events) != len(target_events):
            # Can't sample if counts don't match
            return []

        # Create pairs
        pairs = list(zip(source_events, target_events, strict=False))

        if sample_percentage >= 100.0:
            return pairs

        # Calculate sample size
        sample_size = max(1, int(len(pairs) * sample_percentage / 100))

        # Random sample with fixed seed for reproducibility in tests
        # In production, you might want to use random.sample(pairs, sample_size)
        if sample_size >= len(pairs):
            return pairs

        # Use reservoir sampling for large datasets
        return random.sample(pairs, sample_size)  # nosec B311 - statistical sampling, not security

    def _compute_event_hash(self, event: StoredEvent) -> str:
        """
        Compute SHA-256 hash of event content.

        Hashes the core event data: event_id, event_type, aggregate_id,
        aggregate_type, and the event's data payload.

        Args:
            event: The stored event to hash.

        Returns:
            Hex-encoded SHA-256 hash string.
        """
        hasher = hashlib.sha256()

        # Hash event identity
        hasher.update(str(event.event_id).encode())
        hasher.update(event.event_type.encode())
        hasher.update(str(event.aggregate_id).encode())
        hasher.update(event.aggregate_type.encode())
        hasher.update(str(event.stream_position).encode())

        # Hash event data if available
        underlying = event.event
        if hasattr(underlying, "model_dump"):
            # Pydantic model
            data = underlying.model_dump(mode="json")
            hasher.update(str(sorted(data.items())).encode())
        elif hasattr(underlying, "__dict__"):
            # Regular object
            data = {k: v for k, v in underlying.__dict__.items() if not k.startswith("_")}
            hasher.update(str(sorted(data.items())).encode())

        return hasher.hexdigest()

    def _compare_events_full(
        self,
        source_event: StoredEvent,
        target_event: StoredEvent,
    ) -> str | None:
        """
        Fully compare two events for equality.

        Compares all significant fields between source and target events.

        Args:
            source_event: Event from source store.
            target_event: Event from target store.

        Returns:
            Description of mismatch if found, None if events match.
        """
        # Compare event IDs
        if source_event.event_id != target_event.event_id:
            return f"event_id mismatch: {source_event.event_id} vs {target_event.event_id}"

        # Compare event types
        if source_event.event_type != target_event.event_type:
            return f"event_type mismatch: {source_event.event_type} vs {target_event.event_type}"

        # Compare aggregate IDs
        if source_event.aggregate_id != target_event.aggregate_id:
            return (
                f"aggregate_id mismatch: {source_event.aggregate_id} vs {target_event.aggregate_id}"
            )

        # Compare aggregate types
        if source_event.aggregate_type != target_event.aggregate_type:
            return (
                f"aggregate_type mismatch: "
                f"{source_event.aggregate_type} vs {target_event.aggregate_type}"
            )

        # Compare stream positions
        if source_event.stream_position != target_event.stream_position:
            return (
                f"stream_position mismatch: "
                f"{source_event.stream_position} vs {target_event.stream_position}"
            )

        # Compare event data using hashes as fallback
        source_hash = self._compute_event_hash(source_event)
        target_hash = self._compute_event_hash(target_event)

        if source_hash != target_hash:
            return "Event data content differs"

        return None


__all__ = [
    "VerificationLevel",
    "StreamConsistency",
    "ConsistencyViolation",
    "VerificationReport",
    "ConsistencyVerifier",
]
