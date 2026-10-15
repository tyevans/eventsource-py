"""Unit tests for ConsistencyVerifier sampling, aggregate versions, helpers, and error handling."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest

from eventsource.application.migration.consistency import (
    ConsistencyVerifier,
    VerificationLevel,
)
from eventsource.application.migration.exceptions import ConsistencyError
from eventsource.domain import StreamId
from eventsource.ports import EventEnvelope
from tests.unit.application.migration.test_consistency_verifier._fixtures import (
    TestEvent,
)


@pytest.fixture
def tenant_id() -> UUID:
    return uuid4()


@pytest.fixture
def source_store() -> AsyncMock:
    return AsyncMock()


@pytest.fixture
def target_store() -> AsyncMock:
    return AsyncMock()


def _create_events(count: int, tenant_id: UUID, aggregate_id: UUID) -> list[EventEnvelope]:
    """Create a list of EventEnvelope instances."""
    stream_id = StreamId(aggregate_id=aggregate_id, category="TestAggregate")
    return [
        EventEnvelope(
            event=TestEvent(aggregate_id=aggregate_id, tenant_id=tenant_id, value=f"test_{i}"),
            stream_id=stream_id,
            stream_version=i + 1,
            position=None,
            stored_at=datetime.now(UTC),
        )
        for i in range(count)
    ]


class TestConsistencyVerifierSampling:
    """Tests for sampling functionality."""

    @pytest.mark.asyncio
    async def test_sampling_reduces_verification(
        self,
        tenant_id: UUID,
        source_store: AsyncMock,
        target_store: AsyncMock,
    ) -> None:
        """Test that sampling verifies only a subset of events."""
        events = _create_events(100, tenant_id, uuid4())

        async def generator(from_position, options):
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
        assert report.is_consistent is True


class TestConsistencyVerifierAggregateVersions:
    """Tests for verify_aggregate_versions() method."""

    @pytest.mark.asyncio
    async def test_verify_aggregate_versions_match(
        self,
        tenant_id: UUID,
        source_store: AsyncMock,
        target_store: AsyncMock,
    ) -> None:
        """Test verify_aggregate_versions when all match."""
        events = _create_events(5, tenant_id, uuid4())

        async def generator(from_position, options):
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
        source_events = _create_events(10, tenant_id, agg_id)
        target_events = _create_events(8, tenant_id, agg_id)

        async def source_generator(from_position, options):
            for event in source_events:
                yield event

        async def target_generator(from_position, options):
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
        agg_id = uuid4()
        envelope = EventEnvelope(
            event=TestEvent(aggregate_id=agg_id, tenant_id=uuid4(), value="test"),
            stream_id=StreamId(aggregate_id=agg_id, category="TestAggregate"),
            stream_version=1,
            position=None,
            stored_at=datetime.now(UTC),
        )
        verifier = ConsistencyVerifier(
            source_store=MagicMock(),
            target_store=MagicMock(),
            enable_tracing=False,
        )

        hash1 = verifier._compute_event_hash(envelope)
        hash2 = verifier._compute_event_hash(envelope)

        assert hash1 == hash2
        assert len(hash1) == 64

    def test_group_events_by_stream(self) -> None:
        """Test _group_events_by_stream groups correctly by stream_id.render()."""
        tenant_id = uuid4()
        agg1_id, agg2_id = uuid4(), uuid4()
        events = _create_events(3, tenant_id, agg1_id) + _create_events(3, tenant_id, agg2_id)

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
        assert len(sampled) >= 1
        assert len(sampled) <= 15

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

    @pytest.mark.asyncio
    async def test_verify_handles_store_error(self, tenant_id: UUID) -> None:
        """Test that store errors are wrapped in ConsistencyError."""
        source_store = AsyncMock()
        target_store = AsyncMock()

        async def error_generator(from_position, options):
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
