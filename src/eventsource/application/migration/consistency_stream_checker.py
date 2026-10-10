"""Stream checking and comparison mixin for ConsistencyVerifier."""

from __future__ import annotations

import hashlib
import random
from uuid import UUID

from eventsource.application.migration.consistency_models import (
    ConsistencyViolation,
    StreamConsistency,
    VerificationLevel,
)
from eventsource.ports import EventEnvelope, FeedReadOptions, FullEventStore


class ConsistencyStreamCheckerMixin:
    """Mixin providing stream collection, grouping, comparison, and sampling logic."""

    async def _collect_tenant_events(
        self,
        store: FullEventStore,
        tenant_id: UUID,
    ) -> list[EventEnvelope]:
        """
        Collect all events for a tenant from a store.

        Args:
            store: The event store to read from.
            tenant_id: The tenant UUID.

        Returns:
            List of EventEnvelope instances.
        """
        events: list[EventEnvelope] = []

        async for envelope in store.read_all(None, FeedReadOptions(tenant_id=tenant_id)):
            events.append(envelope)

        return events

    def _group_events_by_stream(
        self,
        events: list[EventEnvelope],
    ) -> dict[str, list[EventEnvelope]]:
        """
        Group events by their stream ID.

        Args:
            events: List of events to group.

        Returns:
            Dictionary mapping stream_id to list of events.
        """
        grouped: dict[str, list[EventEnvelope]] = {}

        for envelope in events:
            # Same wire format the legacy `stream_id` string used.
            stream_id = envelope.stream_id.render()
            if stream_id not in grouped:
                grouped[stream_id] = []
            grouped[stream_id].append(envelope)

        # Sort events within each stream by version
        for stream_id in grouped:
            grouped[stream_id].sort(key=lambda e: e.stream_version)

        return grouped

    async def _verify_stream(
        self,
        stream_id: str,
        source_events: list[EventEnvelope],
        target_events: list[EventEnvelope],
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
        source_version = source_events[-1].stream_version if source_events else 0
        target_version = target_events[-1].stream_version if target_events else 0

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
                        mismatched_positions.append(source_event.stream_version)
                        violations.append(
                            ConsistencyViolation(
                                violation_type="hash_mismatch",
                                stream_id=stream_id,
                                source_value=source_hash[:16],
                                target_value=target_hash[:16],
                                position=source_event.stream_version,
                                details="Event content hash mismatch",
                            )
                        )
                elif level == VerificationLevel.FULL:
                    mismatch = self._compare_events_full(source_event, target_event)
                    if mismatch:
                        mismatched_positions.append(source_event.stream_version)
                        violations.append(
                            ConsistencyViolation(
                                violation_type="content_mismatch",
                                stream_id=stream_id,
                                position=source_event.stream_version,
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
        source_events: list[EventEnvelope],
        target_events: list[EventEnvelope],
        sample_percentage: float,
    ) -> list[tuple[EventEnvelope, EventEnvelope]]:
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

    def _compute_event_hash(self, event: EventEnvelope) -> str:
        """
        Compute SHA-256 hash of event content.

        Hashes the core event data: event_id, event_type, aggregate_id,
        aggregate_type, and the event's data payload.

        Args:
            event: The envelope to hash.

        Returns:
            Hex-encoded SHA-256 hash string.
        """
        hasher = hashlib.sha256()

        # Hash event identity
        hasher.update(str(event.event.event_id).encode())
        hasher.update(event.event.event_type.encode())
        hasher.update(str(event.stream_id.aggregate_id).encode())
        hasher.update(event.stream_id.category.encode())
        hasher.update(str(event.stream_version).encode())

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
        source_event: EventEnvelope,
        target_event: EventEnvelope,
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
        if source_event.event.event_id != target_event.event.event_id:
            return (
                f"event_id mismatch: {source_event.event.event_id} vs {target_event.event.event_id}"
            )

        # Compare event types
        if source_event.event.event_type != target_event.event.event_type:
            return (
                f"event_type mismatch: "
                f"{source_event.event.event_type} vs {target_event.event.event_type}"
            )

        # Compare aggregate IDs
        if source_event.stream_id.aggregate_id != target_event.stream_id.aggregate_id:
            return (
                f"aggregate_id mismatch: "
                f"{source_event.stream_id.aggregate_id} vs {target_event.stream_id.aggregate_id}"
            )

        # Compare aggregate types
        if source_event.stream_id.category != target_event.stream_id.category:
            return (
                f"aggregate_type mismatch: "
                f"{source_event.stream_id.category} vs {target_event.stream_id.category}"
            )

        # Compare stream versions
        if source_event.stream_version != target_event.stream_version:
            return (
                f"stream_version mismatch: "
                f"{source_event.stream_version} vs {target_event.stream_version}"
            )

        # Compare event data using hashes as fallback
        source_hash = self._compute_event_hash(source_event)
        target_hash = self._compute_event_hash(target_event)

        if source_hash != target_hash:
            return "Event data content differs"

        return None


__all__ = [
    "ConsistencyStreamCheckerMixin",
]
