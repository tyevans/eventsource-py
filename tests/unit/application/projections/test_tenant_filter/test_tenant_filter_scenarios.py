"""Scenario, edge-case, and compatibility tests for tenant filtering."""

import logging
from uuid import UUID, uuid4

import pytest

from eventsource.application.projections import DeclarativeProjection
from eventsource.domain.decorators import handles
from eventsource.domain.event import DomainEvent

from ._fixtures import SampleProjection, TenantEvent


class TestCallableTenantFilter:
    @pytest.mark.asyncio
    async def test_filter_called_per_event(
        self,
        tenant_id_1: UUID,
        tenant_event_1: TenantEvent,
        tenant_event_2: TenantEvent,
    ) -> None:
        filter_calls: list[int] = []

        def dynamic_filter() -> UUID:
            filter_calls.append(1)
            return tenant_id_1

        projection = SampleProjection(tenant_filter=dynamic_filter)
        event1 = TenantEvent(aggregate_id=uuid4(), tenant_id=tenant_id_1)
        event2 = TenantEvent(aggregate_id=uuid4(), tenant_id=tenant_id_1)

        await projection.handle(event1)
        await projection.handle(event2)

        assert len(filter_calls) == 2
        assert len(projection.processed_events) == 2

    @pytest.mark.asyncio
    async def test_filter_returns_none_processes_all(
        self,
        tenant_event_1: TenantEvent,
        tenant_event_2: TenantEvent,
    ) -> None:
        def no_filter() -> None:
            return None

        projection = SampleProjection(tenant_filter=no_filter)
        await projection.handle(tenant_event_1)
        await projection.handle(tenant_event_2)

        assert len(projection.processed_events) == 2

    @pytest.mark.asyncio
    async def test_dynamic_filter_changes_between_events(
        self,
        tenant_id_1: UUID,
        tenant_id_2: UUID,
    ) -> None:
        current_tenant: list[UUID | None] = [tenant_id_1]

        def dynamic_filter() -> UUID | None:
            return current_tenant[0]

        projection = SampleProjection(tenant_filter=dynamic_filter)
        event1 = TenantEvent(aggregate_id=uuid4(), tenant_id=tenant_id_1)
        event2 = TenantEvent(aggregate_id=uuid4(), tenant_id=tenant_id_2)

        await projection.handle(event1)
        assert event1 in projection.processed_events

        current_tenant[0] = tenant_id_2
        await projection.handle(event2)
        assert event2 in projection.processed_events

    @pytest.mark.asyncio
    async def test_callable_with_context_based_filtering(
        self,
        tenant_id_1: UUID,
        tenant_event_1: TenantEvent,
        tenant_event_2: TenantEvent,
    ) -> None:
        context: dict[str, UUID | None] = {"tenant": tenant_id_1}

        def get_current_tenant() -> UUID | None:
            return context["tenant"]

        projection = SampleProjection(tenant_filter=get_current_tenant)
        await projection.handle(tenant_event_1)
        await projection.handle(tenant_event_2)

        assert projection.processed_events == [tenant_event_1]


class TestTenantFilterLogging:
    @pytest.mark.asyncio
    async def test_logs_when_event_skipped(
        self,
        tenant_id_1: UUID,
        tenant_event_2: TenantEvent,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        projection = SampleProjection(tenant_filter=tenant_id_1)

        with caplog.at_level(logging.DEBUG):
            await projection.handle(tenant_event_2)

        assert any("Skipping event" in record.message for record in caplog.records)
        assert any("tenant" in record.message.lower() for record in caplog.records)


class TestEdgeCases:
    @pytest.mark.asyncio
    async def test_filter_uuid_equality(self, tenant_id_1: UUID) -> None:
        same_uuid = UUID(str(tenant_id_1))
        projection = SampleProjection(tenant_filter=tenant_id_1)
        event = TenantEvent(aggregate_id=uuid4(), tenant_id=same_uuid)
        await projection.handle(event)
        assert event in projection.processed_events

    @pytest.mark.asyncio
    async def test_multiple_projections_different_filters(
        self,
        tenant_id_1: UUID,
        tenant_id_2: UUID,
        tenant_event_1: TenantEvent,
        tenant_event_2: TenantEvent,
    ) -> None:
        projection1 = SampleProjection(tenant_filter=tenant_id_1)
        projection2 = SampleProjection(tenant_filter=tenant_id_2)

        await projection1.handle(tenant_event_1)
        await projection1.handle(tenant_event_2)
        await projection2.handle(tenant_event_1)
        await projection2.handle(tenant_event_2)

        assert projection1.processed_events == [tenant_event_1]
        assert projection2.processed_events == [tenant_event_2]

    def test_tenant_filter_is_accessible(self, tenant_id_1: UUID) -> None:
        projection = SampleProjection(tenant_filter=tenant_id_1)
        assert projection._tenant_filter == tenant_id_1


class TestBackwardCompatibility:
    @pytest.mark.asyncio
    async def test_existing_projections_work_without_tenant_filter(
        self,
        tenant_event_1: TenantEvent,
        tenant_event_2: TenantEvent,
    ) -> None:
        class LegacyProjection(DeclarativeProjection):
            def __init__(self) -> None:
                super().__init__()
                self.events: list[DomainEvent] = []

            @handles(TenantEvent)
            async def _handle(self, event: TenantEvent) -> None:
                self.events.append(event)

        projection = LegacyProjection()
        await projection.handle(tenant_event_1)
        await projection.handle(tenant_event_2)
        assert len(projection.events) == 2

    def test_projection_init_signature_backward_compatible(self) -> None:
        projection = SampleProjection()
        assert projection._tenant_filter is None
