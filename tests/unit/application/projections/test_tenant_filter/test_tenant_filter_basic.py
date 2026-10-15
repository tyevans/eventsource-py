"""Basic unit tests for projection tenant filtering functionality."""

from uuid import UUID

import pytest

from eventsource.application.projections import TenantFilter

from ._fixtures import NonTenantEvent, OrderCreated, SampleProjection, TenantEvent


class TestStaticTenantFilter:
    """Tests for static UUID tenant filter."""

    @pytest.fixture
    def projection(self, tenant_id_1: UUID) -> SampleProjection:
        """Create projection with static tenant filter."""
        return SampleProjection(tenant_filter=tenant_id_1)

    @pytest.mark.asyncio
    async def test_processes_matching_tenant(
        self, projection: SampleProjection, tenant_event_1: TenantEvent
    ) -> None:
        """Events with matching tenant_id are processed."""
        await projection.handle(tenant_event_1)
        assert tenant_event_1 in projection.processed_events

    @pytest.mark.asyncio
    async def test_skips_non_matching_tenant(
        self, projection: SampleProjection, tenant_event_2: TenantEvent
    ) -> None:
        """Events with different tenant_id are skipped."""
        await projection.handle(tenant_event_2)
        assert tenant_event_2 not in projection.processed_events
        assert len(projection.processed_events) == 0

    @pytest.mark.asyncio
    async def test_processes_events_without_tenant_id(
        self, projection: SampleProjection, non_tenant_event: NonTenantEvent
    ) -> None:
        """Events without tenant_id field are processed (legacy events)."""
        await projection.handle(non_tenant_event)
        assert non_tenant_event in projection.processed_events

    @pytest.mark.asyncio
    async def test_multiple_events_filtering(
        self,
        projection: SampleProjection,
        tenant_event_1: TenantEvent,
        tenant_event_2: TenantEvent,
        non_tenant_event: NonTenantEvent,
    ) -> None:
        """Multiple events are correctly filtered."""
        await projection.handle(tenant_event_1)
        await projection.handle(tenant_event_2)
        await projection.handle(non_tenant_event)

        assert len(projection.processed_events) == 2
        assert tenant_event_1 in projection.processed_events
        assert tenant_event_2 not in projection.processed_events
        assert non_tenant_event in projection.processed_events


class TestNoFilter:
    """Tests for no tenant filter (None)."""

    @pytest.fixture
    def projection(self) -> SampleProjection:
        """Create projection with no tenant filter."""
        return SampleProjection(tenant_filter=None)

    @pytest.mark.asyncio
    async def test_all_events_processed(
        self,
        projection: SampleProjection,
        tenant_event_1: TenantEvent,
        tenant_event_2: TenantEvent,
        non_tenant_event: NonTenantEvent,
    ) -> None:
        """Without filter, all events are processed."""
        await projection.handle(tenant_event_1)
        await projection.handle(tenant_event_2)
        await projection.handle(non_tenant_event)
        assert len(projection.processed_events) == 3

    @pytest.mark.asyncio
    async def test_default_is_no_filter(
        self, tenant_event_1: TenantEvent, tenant_event_2: TenantEvent
    ) -> None:
        """Default projection has no filter (backward compatible)."""
        projection = SampleProjection()
        await projection.handle(tenant_event_1)
        await projection.handle(tenant_event_2)
        assert len(projection.processed_events) == 2


class TestShouldProcessEvent:
    """Tests for _should_process_event method."""

    def test_returns_true_when_no_filter(self, tenant_event_1: TenantEvent) -> None:
        """Returns True when tenant_filter is None."""
        projection = SampleProjection(tenant_filter=None)
        assert projection._should_process_event(tenant_event_1) is True

    def test_returns_true_when_matching(
        self, tenant_id_1: UUID, tenant_event_1: TenantEvent
    ) -> None:
        """Returns True when tenant matches filter."""
        projection = SampleProjection(tenant_filter=tenant_id_1)
        assert projection._should_process_event(tenant_event_1) is True

    def test_returns_false_when_not_matching(
        self, tenant_id_1: UUID, tenant_event_2: TenantEvent
    ) -> None:
        """Returns False when tenant doesn't match filter."""
        projection = SampleProjection(tenant_filter=tenant_id_1)
        assert projection._should_process_event(tenant_event_2) is False

    def test_returns_true_for_event_without_tenant_id(
        self, tenant_id_1: UUID, non_tenant_event: NonTenantEvent
    ) -> None:
        """Returns True for events without tenant_id field."""
        projection = SampleProjection(tenant_filter=tenant_id_1)
        assert projection._should_process_event(non_tenant_event) is True

    def test_returns_true_for_event_with_none_tenant_id(
        self, tenant_id_1: UUID, order_created_no_tenant: OrderCreated
    ) -> None:
        """Returns True for events where tenant_id is None."""
        projection = SampleProjection(tenant_filter=tenant_id_1)
        assert projection._should_process_event(order_created_no_tenant) is True


class TestGetTenantFilterValue:
    """Tests for _get_tenant_filter_value method."""

    def test_returns_none_when_no_filter(self) -> None:
        """Returns None when tenant_filter is None."""
        projection = SampleProjection(tenant_filter=None)
        assert projection._get_tenant_filter_value() is None

    def test_returns_uuid_for_static_filter(self, tenant_id_1: UUID) -> None:
        """Returns UUID for static filter."""
        projection = SampleProjection(tenant_filter=tenant_id_1)
        assert projection._get_tenant_filter_value() == tenant_id_1

    def test_calls_callable_filter(self, tenant_id_1: UUID) -> None:
        """Calls callable filter and returns result."""
        call_count = [0]

        def dynamic_filter() -> UUID:
            call_count[0] += 1
            return tenant_id_1

        projection = SampleProjection(tenant_filter=dynamic_filter)
        assert projection._get_tenant_filter_value() == tenant_id_1
        assert call_count[0] == 1

    def test_callable_returns_none(self) -> None:
        """Callable filter can return None."""

        def no_tenant() -> None:
            return None

        projection = SampleProjection(tenant_filter=no_tenant)
        assert projection._get_tenant_filter_value() is None


class TestTenantFilterTypeAlias:
    """Tests for TenantFilter type alias export."""

    def test_tenant_filter_is_exported(self) -> None:
        """TenantFilter type is exported from projections module."""
        from eventsource.application.projections import TenantFilter

        assert TenantFilter is not None

    def test_tenant_filter_accepts_uuid(self, tenant_id_1: UUID) -> None:
        """TenantFilter accepts UUID values."""
        filter_value: TenantFilter = tenant_id_1
        projection = SampleProjection(tenant_filter=filter_value)
        assert projection._tenant_filter == tenant_id_1

    def test_tenant_filter_accepts_callable(self) -> None:
        """TenantFilter accepts callable values."""

        def get_tenant() -> UUID | None:
            return None

        filter_value: TenantFilter = get_tenant
        projection = SampleProjection(tenant_filter=filter_value)
        assert projection._tenant_filter == get_tenant

    def test_tenant_filter_accepts_none(self) -> None:
        """TenantFilter accepts None."""
        filter_value: TenantFilter = None
        projection = SampleProjection(tenant_filter=filter_value)
        assert projection._tenant_filter is None
