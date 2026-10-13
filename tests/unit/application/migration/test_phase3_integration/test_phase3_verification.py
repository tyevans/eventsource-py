"""Consistency verification failure and report tests for Phase 3 integration."""

from __future__ import annotations

from uuid import UUID

import pytest

from eventsource.adapters.memory import InMemoryEventStore
from eventsource.application.migration.consistency import (
    ConsistencyVerifier,
    VerificationLevel,
)
from eventsource.domain import StreamId
from eventsource.ports.positions import ExpectedVersion
from tests.unit.application.migration.test_phase3_integration._helpers import (
    create_test_events,
)


class TestVerificationFailures:
    """Tests for consistency verification failure scenarios."""

    @pytest.mark.asyncio
    async def test_verification_detects_missing_events(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        tenant_id: UUID,
    ) -> None:
        """Test verification detects missing events in target."""
        # Create events in source only
        await create_test_events(source_store, tenant_id, count=5)

        # Target is empty

        verifier = ConsistencyVerifier(source_store, target_store, enable_tracing=False)

        report = await verifier.verify_tenant_consistency(
            tenant_id=tenant_id,
            level=VerificationLevel.COUNT,
        )

        assert report.is_consistent is False
        assert report.source_event_count == 5
        assert report.target_event_count == 0
        assert len(report.violations) > 0

    @pytest.mark.asyncio
    async def test_verification_detects_extra_events_in_target(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        tenant_id: UUID,
    ) -> None:
        """Test verification detects extra events in target."""
        # Create more events in target than source
        await create_test_events(target_store, tenant_id, count=5)
        # Source has no events

        verifier = ConsistencyVerifier(source_store, target_store, enable_tracing=False)

        report = await verifier.verify_tenant_consistency(
            tenant_id=tenant_id,
            level=VerificationLevel.COUNT,
        )

        assert report.is_consistent is False
        assert report.source_event_count == 0
        assert report.target_event_count == 5

    @pytest.mark.asyncio
    async def test_verification_passes_with_empty_stores(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        tenant_id: UUID,
    ) -> None:
        """Test verification passes when both stores are empty."""
        verifier = ConsistencyVerifier(source_store, target_store, enable_tracing=False)

        report = await verifier.verify_tenant_consistency(
            tenant_id=tenant_id,
            level=VerificationLevel.HASH,
        )

        assert report.is_consistent is True
        assert report.source_event_count == 0
        assert report.target_event_count == 0


class TestVerificationReport:
    """Tests for VerificationReport functionality."""

    @pytest.mark.asyncio
    async def test_report_to_dict_serialization(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        tenant_id: UUID,
    ) -> None:
        """Test report can be serialized to dictionary."""
        await create_test_events(source_store, tenant_id, count=3)
        async for envelope in source_store.read_all():
            if envelope.event.tenant_id == tenant_id:
                event = envelope.event
                await target_store.append(
                    StreamId(aggregate_id=event.aggregate_id, category=event.aggregate_type),
                    [event],
                    ExpectedVersion.any_(),
                )

        verifier = ConsistencyVerifier(source_store, target_store, enable_tracing=False)

        report = await verifier.verify_tenant_consistency(
            tenant_id=tenant_id,
            level=VerificationLevel.HASH,
        )

        report_dict = report.to_dict()

        assert "tenant_id" in report_dict
        assert "is_consistent" in report_dict
        assert "source_event_count" in report_dict
        assert "target_event_count" in report_dict
        assert "verification_level" in report_dict
        assert report_dict["verification_level"] == "hash"

    @pytest.mark.asyncio
    async def test_report_consistency_percentage(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        tenant_id: UUID,
    ) -> None:
        """Test consistency percentage calculation."""
        # Create events that will pass verification
        await create_test_events(source_store, tenant_id, count=3)
        async for envelope in source_store.read_all():
            if envelope.event.tenant_id == tenant_id:
                event = envelope.event
                await target_store.append(
                    StreamId(aggregate_id=event.aggregate_id, category=event.aggregate_type),
                    [event],
                    ExpectedVersion.any_(),
                )

        verifier = ConsistencyVerifier(source_store, target_store, enable_tracing=False)

        report = await verifier.verify_tenant_consistency(
            tenant_id=tenant_id,
            level=VerificationLevel.COUNT,
        )

        assert report.consistency_percentage == 100.0
