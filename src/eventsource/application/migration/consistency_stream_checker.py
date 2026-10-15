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
        self, store: FullEventStore, tenant_id: UUID
    ) -> list[EventEnvelope]:
        """Collect all events for a tenant from a store."""
        return [
            envelope
            async for envelope in store.read_all(None, FeedReadOptions(tenant_id=tenant_id))
        ]

    def _group_events_by_stream(
        self, events: list[EventEnvelope]
    ) -> dict[str, list[EventEnvelope]]:
        """Group events by their rendered stream ID, sorted by stream version."""
        grouped: dict[str, list[EventEnvelope]] = {}
        for envelope in events:
            grouped.setdefault(envelope.stream_id.render(), []).append(envelope)

        for stream_events in grouped.values():
            stream_events.sort(key=lambda e: e.stream_version)
        return grouped

    async def _verify_stream(
        self,
        stream_id: str,
        source_events: list[EventEnvelope],
        target_events: list[EventEnvelope],
        level: VerificationLevel,
        sample_percentage: float,
    ) -> tuple[StreamConsistency, list[ConsistencyViolation]]:
        """Verify a single stream's consistency and return (StreamConsistency, violations)."""
        violations: list[ConsistencyViolation] = []
        mismatched_positions: list[int] = []

        parts = stream_id.rsplit(":", 1)
        if len(parts) == 2:
            aggregate_id, aggregate_type = UUID(parts[0]), parts[1]
        else:
            aggregate_id = UUID("00000000-0000-0000-0000-000000000000")
            aggregate_type = stream_id

        source_count, target_count = len(source_events), len(target_events)
        source_version = source_events[-1].stream_version if source_events else 0
        target_version = target_events[-1].stream_version if target_events else 0

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

        hash_match: bool | None = None
        if (
            level in (VerificationLevel.HASH, VerificationLevel.FULL)
            and source_count == target_count > 0
        ):
            for source_event, target_event in self._sample_events(
                source_events, target_events, sample_percentage
            ):
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
                hash_match = True

        return (
            StreamConsistency(
                stream_id=stream_id,
                aggregate_id=aggregate_id,
                aggregate_type=aggregate_type,
                source_count=source_count,
                target_count=target_count,
                source_version=source_version,
                target_version=target_version,
                is_consistent=len(violations) == 0,
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
        """Sample event pairs for verification according to sample_percentage."""
        if len(source_events) != len(target_events):
            return []

        pairs = list(zip(source_events, target_events, strict=False))
        if sample_percentage >= 100.0:
            return pairs

        sample_size = max(1, int(len(pairs) * sample_percentage / 100))
        if sample_size >= len(pairs):
            return pairs

        return random.sample(pairs, sample_size)  # nosec B311 - statistical sampling, not security

    def _compute_event_hash(self, event: EventEnvelope) -> str:
        """Compute SHA-256 hex digest of core event envelope fields and payload."""
        hasher = hashlib.sha256()
        hasher.update(str(event.event.event_id).encode())
        hasher.update(event.event.event_type.encode())
        hasher.update(str(event.stream_id.aggregate_id).encode())
        hasher.update(event.stream_id.category.encode())
        hasher.update(str(event.stream_version).encode())

        underlying = event.event
        if hasattr(underlying, "model_dump"):
            data = underlying.model_dump(mode="json")
            hasher.update(str(sorted(data.items())).encode())
        elif hasattr(underlying, "__dict__"):
            data = {k: v for k, v in underlying.__dict__.items() if not k.startswith("_")}
            hasher.update(str(sorted(data.items())).encode())

        return hasher.hexdigest()

    def _compare_events_full(
        self, source_event: EventEnvelope, target_event: EventEnvelope
    ) -> str | None:
        """Compare all significant fields between source and target events."""
        checks = [
            ("event_id", source_event.event.event_id, target_event.event.event_id),
            ("event_type", source_event.event.event_type, target_event.event.event_type),
            (
                "aggregate_id",
                source_event.stream_id.aggregate_id,
                target_event.stream_id.aggregate_id,
            ),
            ("aggregate_type", source_event.stream_id.category, target_event.stream_id.category),
            ("stream_version", source_event.stream_version, target_event.stream_version),
        ]
        for field_name, src_val, tgt_val in checks:
            if src_val != tgt_val:
                return f"{field_name} mismatch: {src_val} vs {tgt_val}"

        if self._compute_event_hash(source_event) != self._compute_event_hash(target_event):
            return "Event data content differs"

        return None


__all__ = ["ConsistencyStreamCheckerMixin"]
