"""Unit tests for ConsistencyVerifier initialization and tenant verification."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest

from eventsource.application.migration.consistency import (
    ConsistencyVerifier,
    VerificationLevel,
)
from eventsource.domain import StreamId
from eventsource.ports import EventEnvelope
from tests.unit.application.migration.test_consistency_verifier._fixtures import (
    TestEvent,
)


class TestConsistencyVerifierInit:
    def test_init_with_stores(self) -> None:
        source_store = MagicMock()
        target_store = MagicMock()
        verifier = ConsistencyVerifier(
            source_store=source_store,
            target_store=target_store,
        )
        assert verifier._source == source_store
        assert verifier._target == target_store

    def test_init_with_tracing_disabled(self) -> None:
        source_store = MagicMock()
        target_store = MagicMock()
        verifier = ConsistencyVerifier(
            source_store=source_store,
            target_store=target_store,
            enable_tracing=False,
        )
        assert verifier._enable_tracing is False


class TestConsistencyVerifierVerifyTenant:
    @pytest.fixture
    def tenant_id(self) -> UUID:
        return uuid4()

    @pytest.fixture
    def source_store(self) -> AsyncMock:
        return AsyncMock()

    @pytest.fixture
    def target_store(self) -> AsyncMock:
        return AsyncMock()

    def _create_events(
        self,
        count: int,
        tenant_id: UUID,
        aggregate_id: UUID | None = None,
        value_prefix: str = "test",
    ) -> list[EventEnvelope]:
        events = []
        agg_id = aggregate_id or uuid4()
        stream_id = StreamId(aggregate_id=agg_id, category="TestAggregate")
        for i in range(count):
            event = TestEvent(
                aggregate_id=agg_id,
                tenant_id=tenant_id,
                value=f"{value_prefix}_{i}",
            )
            envelope = EventEnvelope(
                event=event,
                stream_id=stream_id,
                stream_version=i + 1,
                position=None,
                stored_at=datetime.now(UTC),
            )
            events.append(envelope)
        return events

    @pytest.mark.asyncio
    async def test_verify_consistent_tenant(
        self,
        tenant_id: UUID,
        source_store: AsyncMock,
        target_store: AsyncMock,
    ) -> None:
        agg_id = uuid4()
        events = self._create_events(5, tenant_id, agg_id)

        async def source_generator(from_position, options):
            for event in events:
                yield event

        async def target_generator(from_position, options):
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
        agg_id = uuid4()
        source_events = self._create_events(10, tenant_id, agg_id)
        target_events = self._create_events(8, tenant_id, agg_id)

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
        async def empty_generator(from_position, options):
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
        agg_id = uuid4()
        source_events = self._create_events(5, tenant_id, agg_id)

        async def source_generator(from_position, options):
            for event in source_events:
                yield event

        async def target_generator(from_position, options):
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
        agg1_id = uuid4()
        agg2_id = uuid4()
        source_events1 = self._create_events(3, tenant_id, agg1_id)
        source_events2 = self._create_events(4, tenant_id, agg2_id)
        all_source = source_events1 + source_events2

        async def source_generator(from_position, options):
            for event in all_source:
                yield event

        async def target_generator(from_position, options):
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
