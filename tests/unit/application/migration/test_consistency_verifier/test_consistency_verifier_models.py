"""Unit tests for consistency verification data models."""

from datetime import UTC, datetime
from uuid import uuid4

import pytest

from eventsource.application.migration.consistency import (
    ConsistencyViolation,
    StreamConsistency,
    VerificationLevel,
    VerificationReport,
)


class TestVerificationLevel:
    """Tests for VerificationLevel enum."""

    def test_count_level(self) -> None:
        """Test COUNT verification level."""
        level = VerificationLevel.COUNT
        assert level.value == "count"

    def test_hash_level(self) -> None:
        """Test HASH verification level."""
        level = VerificationLevel.HASH
        assert level.value == "hash"

    def test_full_level(self) -> None:
        """Test FULL verification level."""
        level = VerificationLevel.FULL
        assert level.value == "full"


class TestStreamConsistency:
    """Tests for StreamConsistency dataclass."""

    def test_consistent_stream(self) -> None:
        """Test creating a consistent stream result."""
        stream_id = f"{uuid4()}:TestAggregate"
        agg_id = uuid4()
        result = StreamConsistency(
            stream_id=stream_id,
            aggregate_id=agg_id,
            aggregate_type="TestAggregate",
            source_count=10,
            target_count=10,
            source_version=10,
            target_version=10,
            is_consistent=True,
            hash_match=True,
        )
        assert result.is_consistent is True
        assert result.count_mismatch == 0
        assert result.version_mismatch == 0

    def test_inconsistent_stream_count(self) -> None:
        """Test creating an inconsistent stream with count mismatch."""
        stream_id = f"{uuid4()}:TestAggregate"
        agg_id = uuid4()
        result = StreamConsistency(
            stream_id=stream_id,
            aggregate_id=agg_id,
            aggregate_type="TestAggregate",
            source_count=10,
            target_count=8,
            source_version=10,
            target_version=8,
            is_consistent=False,
        )
        assert result.is_consistent is False
        assert result.count_mismatch == 2
        assert result.version_mismatch == 2

    def test_stream_is_frozen(self) -> None:
        """Test StreamConsistency is immutable."""
        stream_id = f"{uuid4()}:TestAggregate"
        agg_id = uuid4()
        result = StreamConsistency(
            stream_id=stream_id,
            aggregate_id=agg_id,
            aggregate_type="TestAggregate",
            source_count=10,
            target_count=10,
            source_version=10,
            target_version=10,
            is_consistent=True,
        )
        with pytest.raises(AttributeError):
            result.source_count = 20  # type: ignore


class TestConsistencyViolation:
    """Tests for ConsistencyViolation dataclass."""

    def test_violation_string_representation(self) -> None:
        """Test violation string representation."""
        violation = ConsistencyViolation(
            violation_type="count_mismatch",
            stream_id="stream-123:TestAggregate",
            source_value="10",
            target_value="8",
            details="Event count mismatch",
        )
        violation_str = str(violation)
        assert "[count_mismatch]" in violation_str
        assert "stream=stream-123:TestAggregate" in violation_str
        assert "source=10" in violation_str
        assert "target=8" in violation_str

    def test_violation_with_position(self) -> None:
        """Test violation with position (stream_version, not global position)."""
        violation = ConsistencyViolation(
            violation_type="hash_mismatch",
            stream_id="stream-123:TestAggregate",
            position=5,
            details="Hash mismatch at position 5",
        )
        violation_str = str(violation)
        assert "position=5" in violation_str

    def test_violation_is_frozen(self) -> None:
        """Test ConsistencyViolation is immutable."""
        violation = ConsistencyViolation(
            violation_type="count_mismatch",
        )
        with pytest.raises(AttributeError):
            violation.violation_type = "other"  # type: ignore


class TestVerificationReport:
    """Tests for VerificationReport dataclass."""

    def test_consistent_report(self) -> None:
        """Test creating a consistent verification report."""
        tenant_id = uuid4()
        report = VerificationReport(
            tenant_id=tenant_id,
            verification_level=VerificationLevel.HASH,
            is_consistent=True,
            source_event_count=100,
            target_event_count=100,
            streams_verified=5,
            streams_consistent=5,
            streams_inconsistent=0,
            sample_percentage=100.0,
            violations=[],
            stream_results=[],
            duration_seconds=1.5,
            verified_at=datetime.now(UTC),
        )
        assert report.is_consistent is True
        assert report.event_count_match is True
        assert report.consistency_percentage == 100.0

    def test_inconsistent_report(self) -> None:
        """Test creating an inconsistent verification report."""
        tenant_id = uuid4()
        report = VerificationReport(
            tenant_id=tenant_id,
            verification_level=VerificationLevel.HASH,
            is_consistent=False,
            source_event_count=100,
            target_event_count=90,
            streams_verified=5,
            streams_consistent=3,
            streams_inconsistent=2,
            sample_percentage=100.0,
            violations=[
                ConsistencyViolation(violation_type="count_mismatch"),
            ],
            stream_results=[],
            duration_seconds=1.5,
            verified_at=datetime.now(UTC),
        )
        assert report.is_consistent is False
        assert report.event_count_match is False
        assert report.consistency_percentage == 60.0

    def test_report_to_dict(self) -> None:
        """Test report to_dict serialization."""
        tenant_id = uuid4()
        report = VerificationReport(
            tenant_id=tenant_id,
            verification_level=VerificationLevel.HASH,
            is_consistent=True,
            source_event_count=100,
            target_event_count=100,
            streams_verified=5,
            streams_consistent=5,
            streams_inconsistent=0,
            sample_percentage=100.0,
            violations=[],
            stream_results=[],
            duration_seconds=1.5,
            verified_at=datetime.now(UTC),
        )
        data = report.to_dict()
        assert data["tenant_id"] == str(tenant_id)
        assert data["verification_level"] == "hash"
        assert data["is_consistent"] is True
        assert data["source_event_count"] == 100

    def test_report_consistency_percentage_with_zero_streams(self) -> None:
        """Test consistency percentage when no streams verified."""
        tenant_id = uuid4()
        report = VerificationReport(
            tenant_id=tenant_id,
            verification_level=VerificationLevel.COUNT,
            is_consistent=True,
            source_event_count=0,
            target_event_count=0,
            streams_verified=0,
            streams_consistent=0,
            streams_inconsistent=0,
            sample_percentage=100.0,
            violations=[],
            stream_results=[],
            duration_seconds=0.1,
            verified_at=datetime.now(UTC),
        )
        assert report.consistency_percentage == 100.0
