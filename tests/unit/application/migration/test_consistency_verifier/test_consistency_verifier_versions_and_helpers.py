"""Unit tests for ConsistencyVerifier sampling, aggregate versions, helpers, and error handling."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest

from eventsource.application.migration.consistency import ConsistencyVerifier, VerificationLevel
from eventsource.application.migration.exceptions import ConsistencyError
from eventsource.domain import StreamId
from eventsource.ports import EventEnvelope
from tests.unit.application.migration.test_consistency_verifier._fixtures import TestEvent


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


def _gen(events: list[EventEnvelope]):
    async def generator(from_position, options):
        for event in events:
            yield event

    return generator


class TestConsistencyVerifierSampling:
    @pytest.mark.asyncio
    async def test_sampling_reduces_verification(
        self, tenant_id: UUID, source_store: AsyncMock, target_store: AsyncMock
    ) -> None:
        events = _create_events(100, tenant_id, uuid4())
        source_store.read_all = _gen(events)
        target_store.read_all = _gen(events)
        verifier = ConsistencyVerifier(
            source_store=source_store, target_store=target_store, enable_tracing=False
        )
        report = await verifier.verify_tenant_consistency(
            tenant_id, level=VerificationLevel.HASH, sample_percentage=10.0
        )
        assert report.sample_percentage == 10.0 and report.is_consistent is True


class TestConsistencyVerifierAggregateVersions:
    @pytest.mark.asyncio
    async def test_verify_aggregate_versions_match(
        self, tenant_id: UUID, source_store: AsyncMock, target_store: AsyncMock
    ) -> None:
        events = _create_events(5, tenant_id, uuid4())
        source_store.read_all = _gen(events)
        target_store.read_all = _gen(events)
        verifier = ConsistencyVerifier(
            source_store=source_store, target_store=target_store, enable_tracing=False
        )
        match, violations = await verifier.verify_aggregate_versions(tenant_id)
        assert match is True and len(violations) == 0

    @pytest.mark.asyncio
    async def test_verify_aggregate_versions_mismatch(
        self, tenant_id: UUID, source_store: AsyncMock, target_store: AsyncMock
    ) -> None:
        agg_id = uuid4()
        source_store.read_all = _gen(_create_events(10, tenant_id, agg_id))
        target_store.read_all = _gen(_create_events(8, tenant_id, agg_id))
        verifier = ConsistencyVerifier(
            source_store=source_store, target_store=target_store, enable_tracing=False
        )
        match, violations = await verifier.verify_aggregate_versions(tenant_id)
        assert match is False and len(violations) > 0


class TestConsistencyVerifierHelperMethods:
    def _verifier(self) -> ConsistencyVerifier:
        return ConsistencyVerifier(
            source_store=MagicMock(), target_store=MagicMock(), enable_tracing=False
        )

    def test_compute_event_hash(self) -> None:
        agg_id = uuid4()
        envelope = EventEnvelope(
            event=TestEvent(aggregate_id=agg_id, tenant_id=uuid4(), value="test"),
            stream_id=StreamId(aggregate_id=agg_id, category="TestAggregate"),
            stream_version=1,
            position=None,
            stored_at=datetime.now(UTC),
        )
        verifier = self._verifier()
        hash1 = verifier._compute_event_hash(envelope)
        assert hash1 == verifier._compute_event_hash(envelope) and len(hash1) == 64

    def test_group_events_by_stream(self) -> None:
        tenant_id, agg1_id, agg2_id = uuid4(), uuid4(), uuid4()
        events = _create_events(3, tenant_id, agg1_id) + _create_events(3, tenant_id, agg2_id)
        grouped = self._verifier()._group_events_by_stream(events)
        assert len(grouped) == 2
        assert len(grouped[f"{agg1_id}:TestAggregate"]) == 3
        assert len(grouped[f"{agg2_id}:TestAggregate"]) == 3

    def test_sample_events_full_sample(self) -> None:
        source = [MagicMock() for _ in range(10)]
        target = [MagicMock() for _ in range(10)]
        assert len(self._verifier()._sample_events(source, target, 100.0)) == 10

    def test_sample_events_partial_sample(self) -> None:
        source = [MagicMock() for _ in range(100)]
        target = [MagicMock() for _ in range(100)]
        sampled = self._verifier()._sample_events(source, target, 10.0)
        assert 1 <= len(sampled) <= 15

    def test_sample_events_mismatched_counts_returns_empty(self) -> None:
        source = [MagicMock() for _ in range(10)]
        target = [MagicMock() for _ in range(8)]
        assert len(self._verifier()._sample_events(source, target, 100.0)) == 0


class TestConsistencyVerifierErrorHandling:
    @pytest.mark.asyncio
    async def test_verify_handles_store_error(self, tenant_id: UUID) -> None:
        source_store, target_store = AsyncMock(), AsyncMock()

        async def error_generator(from_position, options):
            raise Exception("Store connection failed")
            yield

        source_store.read_all = error_generator
        target_store.read_all = error_generator
        verifier = ConsistencyVerifier(
            source_store=source_store, target_store=target_store, enable_tracing=False
        )
        with pytest.raises(ConsistencyError) as exc_info:
            await verifier.verify_tenant_consistency(tenant_id)
        assert "Store connection failed" in str(exc_info.value.details)
