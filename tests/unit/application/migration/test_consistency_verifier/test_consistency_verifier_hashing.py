"""Unit tests for ConsistencyVerifier hash and full verification, and checksums."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock
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


def _create_event(
    tenant_id: UUID,
    aggregate_id: UUID,
    stream_version: int,
    value: str = "test",
) -> EventEnvelope:
    event = TestEvent(aggregate_id=aggregate_id, tenant_id=tenant_id, value=value)
    return EventEnvelope(
        event=event,
        stream_id=StreamId(aggregate_id=aggregate_id, category="TestAggregate"),
        stream_version=stream_version,
        position=None,
        stored_at=datetime.now(UTC),
    )


class TestConsistencyVerifierHashVerification:
    """Tests for hash-based verification."""

    @pytest.fixture
    def tenant_id(self) -> UUID:
        return uuid4()

    @pytest.fixture
    def source_store(self) -> AsyncMock:
        return AsyncMock()

    @pytest.fixture
    def target_store(self) -> AsyncMock:
        return AsyncMock()

    @pytest.mark.asyncio
    async def test_verify_hash_mismatch(
        self,
        tenant_id: UUID,
        source_store: AsyncMock,
        target_store: AsyncMock,
    ) -> None:
        """Test detecting hash mismatch when event content differs."""
        agg_id = uuid4()
        source_event = _create_event(tenant_id, agg_id, 1, "source_value")
        target_event = _create_event(tenant_id, agg_id, 1, "different_value")

        async def source_generator(from_position, options):
            yield source_event

        async def target_generator(from_position, options):
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
        return uuid4()

    @pytest.fixture
    def source_store(self) -> AsyncMock:
        return AsyncMock()

    @pytest.fixture
    def target_store(self) -> AsyncMock:
        return AsyncMock()

    @pytest.mark.asyncio
    async def test_full_verification_matches(
        self,
        tenant_id: UUID,
        source_store: AsyncMock,
        target_store: AsyncMock,
    ) -> None:
        """Test full verification with matching events."""
        agg_id = uuid4()
        envelope = _create_event(tenant_id, agg_id, 1, "value")

        async def generator(from_position, options):
            yield envelope

        source_store.read_all = generator
        target_store.read_all = generator

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


class TestConsistencyVerifierChecksums:
    """Tests for verify_event_checksums() method."""

    @pytest.fixture
    def tenant_id(self) -> UUID:
        return uuid4()

    @pytest.fixture
    def source_store(self) -> AsyncMock:
        return AsyncMock()

    @pytest.fixture
    def target_store(self) -> AsyncMock:
        return AsyncMock()

    @pytest.mark.asyncio
    async def test_verify_checksums_match(
        self,
        tenant_id: UUID,
        source_store: AsyncMock,
        target_store: AsyncMock,
    ) -> None:
        """Test verify_event_checksums when all match."""
        envelope = _create_event(tenant_id, uuid4(), 1, "test")

        async def generator(from_position, options):
            yield envelope

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
