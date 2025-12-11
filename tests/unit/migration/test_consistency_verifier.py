"""
Unit tests for ConsistencyVerifier implementation.

Tests cover:
- VerificationLevel enum
- StreamConsistency dataclass
- ConsistencyViolation dataclass
- VerificationReport dataclass
- ConsistencyVerifier initialization
- verify_tenant_consistency() method
- verify_event_checksums() method
- verify_aggregate_versions() method
- Sampling functionality
- Hash computation
- Full event comparison
"""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest

from eventsource.events.base import DomainEvent
from eventsource.migration.consistency import (
    ConsistencyVerifier,
    ConsistencyViolation,
    StreamConsistency,
    VerificationLevel,
    VerificationReport,
)
from eventsource.migration.exceptions import ConsistencyError
from eventsource.stores.interface import StoredEvent


# Test event class for testing
class TestEvent(DomainEvent):
    """Test event for unit tests."""

    event_type: str = "TestEvent"
    aggregate_type: str = "TestAggregate"
    value: str = "test"


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
        """Test violation with position."""
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


class TestConsistencyVerifierInit:
    """Tests for ConsistencyVerifier initialization."""

    def test_init_with_stores(self) -> None:
        """Test initialization with stores."""
        source_store = MagicMock()
        target_store = MagicMock()

        verifier = ConsistencyVerifier(
            source_store=source_store,
            target_store=target_store,
        )

        assert verifier._source == source_store
        assert verifier._target == target_store

    def test_init_with_tracing_disabled(self) -> None:
        """Test initialization with tracing disabled."""
        source_store = MagicMock()
        target_store = MagicMock()

        verifier = ConsistencyVerifier(
            source_store=source_store,
            target_store=target_store,
            enable_tracing=False,
        )

        assert verifier._enable_tracing is False


class TestConsistencyVerifierVerifyTenant:
    """Tests for ConsistencyVerifier.verify_tenant_consistency() method."""

    @pytest.fixture
    def tenant_id(self) -> UUID:
        """Create a tenant ID for testing."""
        return uuid4()

    @pytest.fixture
    def source_store(self) -> AsyncMock:
        """Create a mock source store."""
        return AsyncMock()

    @pytest.fixture
    def target_store(self) -> AsyncMock:
        """Create a mock target store."""
        return AsyncMock()

    def _create_stored_events(
        self,
        count: int,
        tenant_id: UUID,
        aggregate_id: UUID | None = None,
        value_prefix: str = "test",
    ) -> list[StoredEvent]:
        """Create a list of stored events for testing."""
        events = []
        agg_id = aggregate_id or uuid4()
        for i in range(count):
            event = TestEvent(
                aggregate_id=agg_id,
                tenant_id=tenant_id,
                value=f"{value_prefix}_{i}",
            )
            stored = StoredEvent(
                event=event,
                stream_id=f"{agg_id}:TestAggregate",
                stream_position=i + 1,
                global_position=i + 1,
                stored_at=datetime.now(UTC),
            )
            events.append(stored)
        return events

    @pytest.mark.asyncio
    async def test_verify_consistent_tenant(
        self,
        tenant_id: UUID,
        source_store: AsyncMock,
        target_store: AsyncMock,
    ) -> None:
        """Test verifying a consistent tenant."""
        agg_id = uuid4()
        events = self._create_stored_events(5, tenant_id, agg_id)

        async def source_generator(options):
            for event in events:
                yield event

        async def target_generator(options):
            for event in events:
                yield event

        source_store.read_all = source_generator
        target_store.read_all = target_generator

        verifier = ConsistencyVerifier(
            source_store=source_store,
            target_store=target_store,
            enable_tracing=False,
        )

        report = await verifier.verify_tenant_consistency(
            tenant_id,
            level=VerificationLevel.HASH,
        )

        assert report.is_consistent is True
        assert report.source_event_count == 5
        assert report.target_event_count == 5
        assert report.streams_verified == 1
        assert report.streams_consistent == 1
        assert len(report.violations) == 0

    @pytest.mark.asyncio
    async def test_verify_count_mismatch(
        self,
        tenant_id: UUID,
        source_store: AsyncMock,
        target_store: AsyncMock,
    ) -> None:
        """Test detecting count mismatch."""
        agg_id = uuid4()
        source_events = self._create_stored_events(10, tenant_id, agg_id)
        target_events = self._create_stored_events(8, tenant_id, agg_id)

        async def source_generator(options):
            for event in source_events:
                yield event

        async def target_generator(options):
            for event in target_events:
                yield event

        source_store.read_all = source_generator
        target_store.read_all = target_generator

        verifier = ConsistencyVerifier(
            source_store=source_store,
            target_store=target_store,
            enable_tracing=False,
        )

        report = await verifier.verify_tenant_consistency(
            tenant_id,
            level=VerificationLevel.COUNT,
        )

        assert report.is_consistent is False
        assert report.source_event_count == 10
        assert report.target_event_count == 8
        assert any(v.violation_type == "total_count_mismatch" for v in report.violations)

    @pytest.mark.asyncio
    async def test_verify_empty_stores(
        self,
        tenant_id: UUID,
        source_store: AsyncMock,
        target_store: AsyncMock,
    ) -> None:
        """Test verifying empty stores."""

        async def empty_generator(options):
            return
            yield

        source_store.read_all = empty_generator
        target_store.read_all = empty_generator

        verifier = ConsistencyVerifier(
            source_store=source_store,
            target_store=target_store,
            enable_tracing=False,
        )

        report = await verifier.verify_tenant_consistency(tenant_id)

        assert report.is_consistent is True
        assert report.source_event_count == 0
        assert report.target_event_count == 0
        assert report.streams_verified == 0

    @pytest.mark.asyncio
    async def test_verify_missing_stream_in_target(
        self,
        tenant_id: UUID,
        source_store: AsyncMock,
        target_store: AsyncMock,
    ) -> None:
        """Test detecting stream missing in target."""
        agg_id = uuid4()
        source_events = self._create_stored_events(5, tenant_id, agg_id)

        async def source_generator(options):
            for event in source_events:
                yield event

        async def target_generator(options):
            return
            yield

        source_store.read_all = source_generator
        target_store.read_all = target_generator

        verifier = ConsistencyVerifier(
            source_store=source_store,
            target_store=target_store,
            enable_tracing=False,
        )

        report = await verifier.verify_tenant_consistency(tenant_id)

        assert report.is_consistent is False
        assert any(v.violation_type == "stream_missing" for v in report.violations)

    @pytest.mark.asyncio
    async def test_verify_with_multiple_streams(
        self,
        tenant_id: UUID,
        source_store: AsyncMock,
        target_store: AsyncMock,
    ) -> None:
        """Test verifying multiple streams."""
        agg1_id = uuid4()
        agg2_id = uuid4()
        source_events1 = self._create_stored_events(3, tenant_id, agg1_id)
        source_events2 = self._create_stored_events(4, tenant_id, agg2_id)
        all_source = source_events1 + source_events2

        async def source_generator(options):
            for event in all_source:
                yield event

        async def target_generator(options):
            for event in all_source:
                yield event

        source_store.read_all = source_generator
        target_store.read_all = target_generator

        verifier = ConsistencyVerifier(
            source_store=source_store,
            target_store=target_store,
            enable_tracing=False,
        )

        report = await verifier.verify_tenant_consistency(tenant_id)

        assert report.is_consistent is True
        assert report.streams_verified == 2
        assert report.streams_consistent == 2

    @pytest.mark.asyncio
    async def test_verify_invalid_sample_percentage(
        self,
        tenant_id: UUID,
        source_store: AsyncMock,
        target_store: AsyncMock,
    ) -> None:
        """Test invalid sample percentage raises error."""
        verifier = ConsistencyVerifier(
            source_store=source_store,
            target_store=target_store,
            enable_tracing=False,
        )

        with pytest.raises(ValueError, match="sample_percentage"):
            await verifier.verify_tenant_consistency(
                tenant_id,
                sample_percentage=0,
            )

        with pytest.raises(ValueError, match="sample_percentage"):
            await verifier.verify_tenant_consistency(
                tenant_id,
                sample_percentage=101,
            )


class TestConsistencyVerifierHashVerification:
    """Tests for hash-based verification."""

    @pytest.fixture
    def tenant_id(self) -> UUID:
        """Create a tenant ID for testing."""
        return uuid4()

    @pytest.fixture
    def source_store(self) -> AsyncMock:
        """Create a mock source store."""
        return AsyncMock()

    @pytest.fixture
    def target_store(self) -> AsyncMock:
        """Create a mock target store."""
        return AsyncMock()

    def _create_stored_event(
        self,
        tenant_id: UUID,
        aggregate_id: UUID,
        position: int,
        value: str = "test",
    ) -> StoredEvent:
        """Create a single stored event."""
        event = TestEvent(
            aggregate_id=aggregate_id,
            tenant_id=tenant_id,
            value=value,
        )
        return StoredEvent(
            event=event,
            stream_id=f"{aggregate_id}:TestAggregate",
            stream_position=position,
            global_position=position,
            stored_at=datetime.now(UTC),
        )

    @pytest.mark.asyncio
    async def test_verify_hash_mismatch(
        self,
        tenant_id: UUID,
        source_store: AsyncMock,
        target_store: AsyncMock,
    ) -> None:
        """Test detecting hash mismatch when event content differs."""
        agg_id = uuid4()
        source_event = self._create_stored_event(tenant_id, agg_id, 1, "source_value")
        target_event = self._create_stored_event(tenant_id, agg_id, 1, "different_value")

        async def source_generator(options):
            yield source_event

        async def target_generator(options):
            yield target_event

        source_store.read_all = source_generator
        target_store.read_all = target_generator

        verifier = ConsistencyVerifier(
            source_store=source_store,
            target_store=target_store,
            enable_tracing=False,
        )

        report = await verifier.verify_tenant_consistency(
            tenant_id,
            level=VerificationLevel.HASH,
        )

        assert report.is_consistent is False
        hash_violations = [v for v in report.violations if v.violation_type == "hash_mismatch"]
        assert len(hash_violations) >= 1


class TestConsistencyVerifierFullVerification:
    """Tests for full verification level."""

    @pytest.fixture
    def tenant_id(self) -> UUID:
        """Create a tenant ID for testing."""
        return uuid4()

    @pytest.fixture
    def source_store(self) -> AsyncMock:
        """Create a mock source store."""
        return AsyncMock()

    @pytest.fixture
    def target_store(self) -> AsyncMock:
        """Create a mock target store."""
        return AsyncMock()

    def _create_stored_event(
        self,
        tenant_id: UUID,
        aggregate_id: UUID,
        position: int,
        value: str = "test",
    ) -> StoredEvent:
        """Create a single stored event."""
        event = TestEvent(
            aggregate_id=aggregate_id,
            tenant_id=tenant_id,
            value=value,
        )
        return StoredEvent(
            event=event,
            stream_id=f"{aggregate_id}:TestAggregate",
            stream_position=position,
            global_position=position,
            stored_at=datetime.now(UTC),
        )

    @pytest.mark.asyncio
    async def test_full_verification_matches(
        self,
        tenant_id: UUID,
        source_store: AsyncMock,
        target_store: AsyncMock,
    ) -> None:
        """Test full verification with matching events."""
        agg_id = uuid4()
        # Use the SAME stored event for both source and target
        # This simulates correctly migrated data
        stored_event = self._create_stored_event(tenant_id, agg_id, 1, "value")

        async def source_generator(options):
            yield stored_event

        async def target_generator(options):
            yield stored_event

        source_store.read_all = source_generator
        target_store.read_all = target_generator

        verifier = ConsistencyVerifier(
            source_store=source_store,
            target_store=target_store,
            enable_tracing=False,
        )

        report = await verifier.verify_tenant_consistency(
            tenant_id,
            level=VerificationLevel.FULL,
        )

        assert report.is_consistent is True


class TestConsistencyVerifierSampling:
    """Tests for sampling functionality."""

    @pytest.fixture
    def tenant_id(self) -> UUID:
        """Create a tenant ID for testing."""
        return uuid4()

    @pytest.fixture
    def source_store(self) -> AsyncMock:
        """Create a mock source store."""
        return AsyncMock()

    @pytest.fixture
    def target_store(self) -> AsyncMock:
        """Create a mock target store."""
        return AsyncMock()

    def _create_stored_events(
        self,
        count: int,
        tenant_id: UUID,
        aggregate_id: UUID,
    ) -> list[StoredEvent]:
        """Create a list of stored events."""
        events = []
        for i in range(count):
            event = TestEvent(
                aggregate_id=aggregate_id,
                tenant_id=tenant_id,
                value=f"test_{i}",
            )
            stored = StoredEvent(
                event=event,
                stream_id=f"{aggregate_id}:TestAggregate",
                stream_position=i + 1,
                global_position=i + 1,
                stored_at=datetime.now(UTC),
            )
            events.append(stored)
        return events

    @pytest.mark.asyncio
    async def test_sampling_reduces_verification(
        self,
        tenant_id: UUID,
        source_store: AsyncMock,
        target_store: AsyncMock,
    ) -> None:
        """Test that sampling verifies only a subset of events."""
        agg_id = uuid4()
        events = self._create_stored_events(100, tenant_id, agg_id)

        async def generator(options):
            for event in events:
                yield event

        source_store.read_all = generator
        target_store.read_all = generator

        verifier = ConsistencyVerifier(
            source_store=source_store,
            target_store=target_store,
            enable_tracing=False,
        )

        report = await verifier.verify_tenant_consistency(
            tenant_id,
            level=VerificationLevel.HASH,
            sample_percentage=10.0,
        )

        assert report.sample_percentage == 10.0
        # Should still be consistent (same events)
        assert report.is_consistent is True


class TestConsistencyVerifierChecksums:
    """Tests for verify_event_checksums() method."""

    @pytest.fixture
    def tenant_id(self) -> UUID:
        """Create a tenant ID for testing."""
        return uuid4()

    @pytest.fixture
    def source_store(self) -> AsyncMock:
        """Create a mock source store."""
        return AsyncMock()

    @pytest.fixture
    def target_store(self) -> AsyncMock:
        """Create a mock target store."""
        return AsyncMock()

    @pytest.mark.asyncio
    async def test_verify_checksums_match(
        self,
        tenant_id: UUID,
        source_store: AsyncMock,
        target_store: AsyncMock,
    ) -> None:
        """Test verify_event_checksums when all match."""
        agg_id = uuid4()
        event = TestEvent(
            aggregate_id=agg_id,
            tenant_id=tenant_id,
            value="test",
        )
        stored = StoredEvent(
            event=event,
            stream_id=f"{agg_id}:TestAggregate",
            stream_position=1,
            global_position=1,
            stored_at=datetime.now(UTC),
        )

        async def generator(options):
            yield stored

        source_store.read_all = generator
        target_store.read_all = generator

        verifier = ConsistencyVerifier(
            source_store=source_store,
            target_store=target_store,
            enable_tracing=False,
        )

        match, violations = await verifier.verify_event_checksums(tenant_id)

        assert match is True
        assert len(violations) == 0


class TestConsistencyVerifierAggregateVersions:
    """Tests for verify_aggregate_versions() method."""

    @pytest.fixture
    def tenant_id(self) -> UUID:
        """Create a tenant ID for testing."""
        return uuid4()

    @pytest.fixture
    def source_store(self) -> AsyncMock:
        """Create a mock source store."""
        return AsyncMock()

    @pytest.fixture
    def target_store(self) -> AsyncMock:
        """Create a mock target store."""
        return AsyncMock()

    def _create_stored_events(
        self,
        count: int,
        tenant_id: UUID,
        aggregate_id: UUID,
    ) -> list[StoredEvent]:
        """Create a list of stored events."""
        events = []
        for i in range(count):
            event = TestEvent(
                aggregate_id=aggregate_id,
                tenant_id=tenant_id,
                value=f"test_{i}",
            )
            stored = StoredEvent(
                event=event,
                stream_id=f"{aggregate_id}:TestAggregate",
                stream_position=i + 1,
                global_position=i + 1,
                stored_at=datetime.now(UTC),
            )
            events.append(stored)
        return events

    @pytest.mark.asyncio
    async def test_verify_aggregate_versions_match(
        self,
        tenant_id: UUID,
        source_store: AsyncMock,
        target_store: AsyncMock,
    ) -> None:
        """Test verify_aggregate_versions when all match."""
        agg_id = uuid4()
        events = self._create_stored_events(5, tenant_id, agg_id)

        async def generator(options):
            for event in events:
                yield event

        source_store.read_all = generator
        target_store.read_all = generator

        verifier = ConsistencyVerifier(
            source_store=source_store,
            target_store=target_store,
            enable_tracing=False,
        )

        match, violations = await verifier.verify_aggregate_versions(tenant_id)

        assert match is True
        assert len(violations) == 0

    @pytest.mark.asyncio
    async def test_verify_aggregate_versions_mismatch(
        self,
        tenant_id: UUID,
        source_store: AsyncMock,
        target_store: AsyncMock,
    ) -> None:
        """Test verify_aggregate_versions with version mismatch."""
        agg_id = uuid4()
        source_events = self._create_stored_events(10, tenant_id, agg_id)
        target_events = self._create_stored_events(8, tenant_id, agg_id)

        async def source_generator(options):
            for event in source_events:
                yield event

        async def target_generator(options):
            for event in target_events:
                yield event

        source_store.read_all = source_generator
        target_store.read_all = target_generator

        verifier = ConsistencyVerifier(
            source_store=source_store,
            target_store=target_store,
            enable_tracing=False,
        )

        match, violations = await verifier.verify_aggregate_versions(tenant_id)

        assert match is False
        assert len(violations) > 0


class TestConsistencyVerifierHelperMethods:
    """Tests for ConsistencyVerifier helper methods."""

    def test_compute_event_hash(self) -> None:
        """Test _compute_event_hash produces consistent hashes."""
        tenant_id = uuid4()
        agg_id = uuid4()
        event = TestEvent(
            aggregate_id=agg_id,
            tenant_id=tenant_id,
            value="test",
        )
        stored = StoredEvent(
            event=event,
            stream_id=f"{agg_id}:TestAggregate",
            stream_position=1,
            global_position=1,
            stored_at=datetime.now(UTC),
        )

        verifier = ConsistencyVerifier(
            source_store=MagicMock(),
            target_store=MagicMock(),
            enable_tracing=False,
        )

        hash1 = verifier._compute_event_hash(stored)
        hash2 = verifier._compute_event_hash(stored)

        # Same event should produce same hash
        assert hash1 == hash2
        # Hash should be 64 characters (SHA-256 hex)
        assert len(hash1) == 64

    def test_group_events_by_stream(self) -> None:
        """Test _group_events_by_stream groups correctly."""
        tenant_id = uuid4()
        agg1_id = uuid4()
        agg2_id = uuid4()

        events = []
        for agg_id in [agg1_id, agg2_id]:
            for i in range(3):
                event = TestEvent(
                    aggregate_id=agg_id,
                    tenant_id=tenant_id,
                    value=f"test_{i}",
                )
                stored = StoredEvent(
                    event=event,
                    stream_id=f"{agg_id}:TestAggregate",
                    stream_position=i + 1,
                    global_position=i + 1,
                    stored_at=datetime.now(UTC),
                )
                events.append(stored)

        verifier = ConsistencyVerifier(
            source_store=MagicMock(),
            target_store=MagicMock(),
            enable_tracing=False,
        )

        grouped = verifier._group_events_by_stream(events)

        assert len(grouped) == 2
        assert f"{agg1_id}:TestAggregate" in grouped
        assert f"{agg2_id}:TestAggregate" in grouped
        assert len(grouped[f"{agg1_id}:TestAggregate"]) == 3
        assert len(grouped[f"{agg2_id}:TestAggregate"]) == 3

    def test_sample_events_full_sample(self) -> None:
        """Test _sample_events with 100% sampling."""
        verifier = ConsistencyVerifier(
            source_store=MagicMock(),
            target_store=MagicMock(),
            enable_tracing=False,
        )

        source = [MagicMock() for _ in range(10)]
        target = [MagicMock() for _ in range(10)]

        sampled = verifier._sample_events(source, target, 100.0)

        assert len(sampled) == 10

    def test_sample_events_partial_sample(self) -> None:
        """Test _sample_events with partial sampling."""
        verifier = ConsistencyVerifier(
            source_store=MagicMock(),
            target_store=MagicMock(),
            enable_tracing=False,
        )

        source = [MagicMock() for _ in range(100)]
        target = [MagicMock() for _ in range(100)]

        sampled = verifier._sample_events(source, target, 10.0)

        # Should have approximately 10 events (10%)
        assert len(sampled) >= 1
        assert len(sampled) <= 15  # Allow some variance

    def test_sample_events_mismatched_counts_returns_empty(self) -> None:
        """Test _sample_events returns empty when counts mismatch."""
        verifier = ConsistencyVerifier(
            source_store=MagicMock(),
            target_store=MagicMock(),
            enable_tracing=False,
        )

        source = [MagicMock() for _ in range(10)]
        target = [MagicMock() for _ in range(8)]

        sampled = verifier._sample_events(source, target, 100.0)

        assert len(sampled) == 0


class TestConsistencyVerifierErrorHandling:
    """Tests for error handling."""

    @pytest.fixture
    def tenant_id(self) -> UUID:
        """Create a tenant ID for testing."""
        return uuid4()

    @pytest.mark.asyncio
    async def test_verify_handles_store_error(
        self,
        tenant_id: UUID,
    ) -> None:
        """Test that store errors are wrapped in ConsistencyError."""
        source_store = AsyncMock()
        target_store = AsyncMock()

        async def error_generator(options):
            raise Exception("Store connection failed")
            yield

        source_store.read_all = error_generator
        target_store.read_all = error_generator

        verifier = ConsistencyVerifier(
            source_store=source_store,
            target_store=target_store,
            enable_tracing=False,
        )

        with pytest.raises(ConsistencyError) as exc_info:
            await verifier.verify_tenant_consistency(tenant_id)

        assert "Store connection failed" in str(exc_info.value.details)
