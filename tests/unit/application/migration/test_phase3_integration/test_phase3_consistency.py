"""Consistency verification levels and sampling tests for Phase 3 integration."""

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


class TestConsistencyVerificationLevels:
    """Tests for consistency verification at different levels."""

    @pytest.mark.asyncio
    async def test_count_level_verification_passes(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        tenant_id: UUID,
    ) -> None:
        """Test COUNT level verification passes when counts match."""
        # Create same events in both stores
        await create_test_events(source_store, tenant_id, count=5)

        # Copy to target (same events)
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

        assert report.is_consistent is True
        assert report.source_event_count == 5
        assert report.target_event_count == 5
        assert report.streams_verified == 5

    @pytest.mark.asyncio
    async def test_count_level_verification_detects_mismatch(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        tenant_id: UUID,
    ) -> None:
        """Test COUNT level verification detects count mismatch."""
        # Create events in source
        await create_test_events(source_store, tenant_id, count=5)

        # Create fewer events in target (only 3)
        copied = 0
        async for envelope in source_store.read_all():
            if envelope.event.tenant_id == tenant_id and copied < 3:
                event = envelope.event
                await target_store.append(
                    StreamId(aggregate_id=event.aggregate_id, category=event.aggregate_type),
                    [event],
                    ExpectedVersion.any_(),
                )
                copied += 1

        verifier = ConsistencyVerifier(source_store, target_store, enable_tracing=False)

        report = await verifier.verify_tenant_consistency(
            tenant_id=tenant_id,
            level=VerificationLevel.COUNT,
        )

        assert report.is_consistent is False
        assert report.source_event_count == 5
        assert report.target_event_count == 3

    @pytest.mark.asyncio
    async def test_hash_level_verification_passes(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        tenant_id: UUID,
    ) -> None:
        """Test HASH level verification passes with identical data."""
        # Create same events in both stores
        await create_test_events(source_store, tenant_id, count=5)

        # Copy to target
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

        assert report.is_consistent is True
        assert report.verification_level == VerificationLevel.HASH

    @pytest.mark.asyncio
    async def test_full_level_verification_passes(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        tenant_id: UUID,
    ) -> None:
        """Test FULL level verification passes with identical data."""
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
            level=VerificationLevel.FULL,
        )

        assert report.is_consistent is True
        assert report.verification_level == VerificationLevel.FULL


class TestConsistencyVerificationWithSampling:
    """Tests for consistency verification with sampling."""

    @pytest.mark.asyncio
    async def test_verification_with_50_percent_sampling(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        tenant_id: UUID,
    ) -> None:
        """Test verification with 50% sampling."""
        # Create events in both stores
        await create_test_events(source_store, tenant_id, count=10)

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
            sample_percentage=50.0,
        )

        assert report.is_consistent is True
        assert report.sample_percentage == 50.0

    @pytest.mark.asyncio
    async def test_invalid_sample_percentage_raises(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        tenant_id: UUID,
    ) -> None:
        """Test that invalid sample percentage raises ValueError."""
        verifier = ConsistencyVerifier(source_store, target_store, enable_tracing=False)

        with pytest.raises(ValueError):
            await verifier.verify_tenant_consistency(
                tenant_id=tenant_id,
                level=VerificationLevel.COUNT,
                sample_percentage=0.0,  # Invalid
            )

        with pytest.raises(ValueError):
            await verifier.verify_tenant_consistency(
                tenant_id=tenant_id,
                level=VerificationLevel.COUNT,
                sample_percentage=150.0,  # Invalid
            )
