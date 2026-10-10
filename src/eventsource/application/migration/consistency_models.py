"""Data models and enums for migration consistency verification."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any
from uuid import UUID


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


__all__ = [
    "ConsistencyViolation",
    "StreamConsistency",
    "VerificationLevel",
    "VerificationReport",
]
