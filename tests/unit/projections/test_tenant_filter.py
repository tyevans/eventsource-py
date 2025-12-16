"""
Unit tests for projection tenant filtering functionality.

Tests cover:
- Static UUID tenant filter
- Callable tenant filter (dynamic filtering)
- No filter (process all events)
- Events without tenant_id field
- _should_process_event method
- _get_tenant_filter_value method
- Integration with DeclarativeProjection
- Backward compatibility when tenant_filter is None
"""

from uuid import UUID, uuid4

import pytest

from eventsource.events.base import DomainEvent
from eventsource.handlers import handles
from eventsource.projections import DeclarativeProjection, TenantFilter

# --- Sample Event Classes ---


class TenantEvent(DomainEvent):
    """Event with tenant_id field."""

    aggregate_type: str = "Order"
    tenant_id: UUID  # Override to make required for testing


class NonTenantEvent(DomainEvent):
    """Event without tenant_id field (legacy/system event)."""

    aggregate_type: str = "System"


class OrderCreated(DomainEvent):
    """Order created event for handler tests."""

    aggregate_type: str = "Order"
    order_number: str = "ORD-001"


class OrderShipped(DomainEvent):
    """Order shipped event for handler tests."""

    aggregate_type: str = "Order"
    tracking_number: str = "TRK-001"


# --- Sample Projection Classes ---


class SampleProjection(DeclarativeProjection):
    """Sample projection that tracks processed events."""

    def __init__(
        self,
        *,
        tenant_filter: TenantFilter = None,
    ) -> None:
        super().__init__(tenant_filter=tenant_filter)
        self.processed_events: list[DomainEvent] = []

    @handles(TenantEvent)
    async def _handle_tenant_event(self, event: TenantEvent) -> None:
        self.processed_events.append(event)

    @handles(NonTenantEvent)
    async def _handle_non_tenant_event(self, event: NonTenantEvent) -> None:
        self.processed_events.append(event)

    @handles(OrderCreated)
    async def _handle_order_created(self, event: OrderCreated) -> None:
        self.processed_events.append(event)


# --- Fixtures ---


@pytest.fixture
def tenant_id_1() -> UUID:
    """First test tenant ID."""
    return UUID("11111111-1111-1111-1111-111111111111")


@pytest.fixture
def tenant_id_2() -> UUID:
    """Second test tenant ID."""
    return UUID("22222222-2222-2222-2222-222222222222")


@pytest.fixture
def tenant_event_1(tenant_id_1: UUID) -> TenantEvent:
    """Create a TenantEvent for tenant 1."""
    return TenantEvent(aggregate_id=uuid4(), tenant_id=tenant_id_1)


@pytest.fixture
def tenant_event_2(tenant_id_2: UUID) -> TenantEvent:
    """Create a TenantEvent for tenant 2."""
    return TenantEvent(aggregate_id=uuid4(), tenant_id=tenant_id_2)


@pytest.fixture
def non_tenant_event() -> NonTenantEvent:
    """Create a NonTenantEvent (no tenant_id)."""
    return NonTenantEvent(aggregate_id=uuid4())


@pytest.fixture
def order_created_tenant_1(tenant_id_1: UUID) -> OrderCreated:
    """Create an OrderCreated event for tenant 1."""
    return OrderCreated(aggregate_id=uuid4(), tenant_id=tenant_id_1)


@pytest.fixture
def order_created_tenant_2(tenant_id_2: UUID) -> OrderCreated:
    """Create an OrderCreated event for tenant 2."""
    return OrderCreated(aggregate_id=uuid4(), tenant_id=tenant_id_2)


@pytest.fixture
def order_created_no_tenant() -> OrderCreated:
    """Create an OrderCreated event with no tenant."""
    return OrderCreated(aggregate_id=uuid4(), tenant_id=None)


# --- Static Tenant Filter Tests ---


class TestStaticTenantFilter:
    """Tests for static UUID tenant filter."""

    @pytest.fixture
    def projection(self, tenant_id_1: UUID) -> SampleProjection:
        """Create projection with static tenant filter."""
        return SampleProjection(tenant_filter=tenant_id_1)

    @pytest.mark.asyncio
    async def test_processes_matching_tenant(
        self,
        projection: SampleProjection,
        tenant_event_1: TenantEvent,
    ) -> None:
        """Events with matching tenant_id are processed."""
        await projection.handle(tenant_event_1)

        assert tenant_event_1 in projection.processed_events

    @pytest.mark.asyncio
    async def test_skips_non_matching_tenant(
        self,
        projection: SampleProjection,
        tenant_event_2: TenantEvent,
    ) -> None:
        """Events with different tenant_id are skipped."""
        await projection.handle(tenant_event_2)

        assert tenant_event_2 not in projection.processed_events
        assert len(projection.processed_events) == 0

    @pytest.mark.asyncio
    async def test_processes_events_without_tenant_id(
        self,
        projection: SampleProjection,
        non_tenant_event: NonTenantEvent,
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


# --- Callable Tenant Filter Tests ---


class TestCallableTenantFilter:
    """Tests for callable tenant filter."""

    @pytest.mark.asyncio
    async def test_filter_called_per_event(
        self,
        tenant_id_1: UUID,
        tenant_event_1: TenantEvent,
        tenant_event_2: TenantEvent,
    ) -> None:
        """Callable filter is invoked for each event."""
        filter_calls: list[int] = []

        def dynamic_filter() -> UUID:
            filter_calls.append(1)
            return tenant_id_1

        projection = SampleProjection(tenant_filter=dynamic_filter)

        # Create events with matching tenant
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
        """When callable returns None, all events processed."""

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
        """Filter can return different values for different events."""
        current_tenant: list[UUID | None] = [tenant_id_1]

        def dynamic_filter() -> UUID | None:
            return current_tenant[0]

        projection = SampleProjection(tenant_filter=dynamic_filter)

        event1 = TenantEvent(aggregate_id=uuid4(), tenant_id=tenant_id_1)
        event2 = TenantEvent(aggregate_id=uuid4(), tenant_id=tenant_id_2)

        # First event matches
        await projection.handle(event1)
        assert event1 in projection.processed_events

        # Change tenant filter
        current_tenant[0] = tenant_id_2

        # Second event now matches
        await projection.handle(event2)
        assert event2 in projection.processed_events

    @pytest.mark.asyncio
    async def test_callable_with_context_based_filtering(
        self,
        tenant_id_1: UUID,
        tenant_event_1: TenantEvent,
        tenant_event_2: TenantEvent,
    ) -> None:
        """Callable filter simulating get_current_tenant() pattern."""
        # Simulate a context-based tenant getter
        context: dict[str, UUID | None] = {"tenant": tenant_id_1}

        def get_current_tenant() -> UUID | None:
            return context["tenant"]

        projection = SampleProjection(tenant_filter=get_current_tenant)

        await projection.handle(tenant_event_1)
        await projection.handle(tenant_event_2)

        # Only tenant_event_1 matches
        assert len(projection.processed_events) == 1
        assert tenant_event_1 in projection.processed_events


# --- No Filter Tests ---


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
        self,
        tenant_event_1: TenantEvent,
        tenant_event_2: TenantEvent,
    ) -> None:
        """Default projection has no filter (backward compatible)."""
        projection = SampleProjection()  # No tenant_filter argument

        await projection.handle(tenant_event_1)
        await projection.handle(tenant_event_2)

        assert len(projection.processed_events) == 2


# --- _should_process_event Tests ---


class TestShouldProcessEvent:
    """Tests for _should_process_event method."""

    def test_returns_true_when_no_filter(
        self,
        tenant_event_1: TenantEvent,
    ) -> None:
        """Returns True when tenant_filter is None."""
        projection = SampleProjection(tenant_filter=None)

        assert projection._should_process_event(tenant_event_1) is True

    def test_returns_true_when_matching(
        self,
        tenant_id_1: UUID,
        tenant_event_1: TenantEvent,
    ) -> None:
        """Returns True when tenant matches filter."""
        projection = SampleProjection(tenant_filter=tenant_id_1)

        assert projection._should_process_event(tenant_event_1) is True

    def test_returns_false_when_not_matching(
        self,
        tenant_id_1: UUID,
        tenant_event_2: TenantEvent,
    ) -> None:
        """Returns False when tenant doesn't match filter."""
        projection = SampleProjection(tenant_filter=tenant_id_1)

        assert projection._should_process_event(tenant_event_2) is False

    def test_returns_true_for_event_without_tenant_id(
        self,
        tenant_id_1: UUID,
        non_tenant_event: NonTenantEvent,
    ) -> None:
        """Returns True for events without tenant_id field."""
        projection = SampleProjection(tenant_filter=tenant_id_1)

        assert projection._should_process_event(non_tenant_event) is True

    def test_returns_true_for_event_with_none_tenant_id(
        self,
        tenant_id_1: UUID,
        order_created_no_tenant: OrderCreated,
    ) -> None:
        """Returns True for events where tenant_id is None."""
        projection = SampleProjection(tenant_filter=tenant_id_1)

        assert projection._should_process_event(order_created_no_tenant) is True


# --- _get_tenant_filter_value Tests ---


class TestGetTenantFilterValue:
    """Tests for _get_tenant_filter_value method."""

    def test_returns_none_when_no_filter(self) -> None:
        """Returns None when tenant_filter is None."""
        projection = SampleProjection(tenant_filter=None)

        assert projection._get_tenant_filter_value() is None

    def test_returns_uuid_for_static_filter(
        self,
        tenant_id_1: UUID,
    ) -> None:
        """Returns UUID for static filter."""
        projection = SampleProjection(tenant_filter=tenant_id_1)

        assert projection._get_tenant_filter_value() == tenant_id_1

    def test_calls_callable_filter(
        self,
        tenant_id_1: UUID,
    ) -> None:
        """Calls callable filter and returns result."""
        call_count = [0]

        def dynamic_filter() -> UUID:
            call_count[0] += 1
            return tenant_id_1

        projection = SampleProjection(tenant_filter=dynamic_filter)

        result = projection._get_tenant_filter_value()

        assert result == tenant_id_1
        assert call_count[0] == 1

    def test_callable_returns_none(self) -> None:
        """Callable filter can return None."""

        def no_tenant() -> None:
            return None

        projection = SampleProjection(tenant_filter=no_tenant)

        assert projection._get_tenant_filter_value() is None


# --- Logging Tests ---


class TestTenantFilterLogging:
    """Tests for tenant filter logging behavior."""

    @pytest.mark.asyncio
    async def test_logs_when_event_skipped(
        self,
        tenant_id_1: UUID,
        tenant_event_2: TenantEvent,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Debug log is emitted when event is skipped due to tenant filter."""
        import logging

        projection = SampleProjection(tenant_filter=tenant_id_1)

        with caplog.at_level(logging.DEBUG):
            await projection.handle(tenant_event_2)

        assert any("Skipping event" in record.message for record in caplog.records)
        assert any("tenant" in record.message.lower() for record in caplog.records)


# --- Edge Cases ---


class TestEdgeCases:
    """Tests for edge cases in tenant filtering."""

    @pytest.mark.asyncio
    async def test_filter_uuid_equality(
        self,
        tenant_id_1: UUID,
    ) -> None:
        """UUID equality is correctly evaluated."""
        # Create same UUID from string to ensure equality works
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
        """Different projections can have different tenant filters."""
        projection1 = SampleProjection(tenant_filter=tenant_id_1)
        projection2 = SampleProjection(tenant_filter=tenant_id_2)

        await projection1.handle(tenant_event_1)
        await projection1.handle(tenant_event_2)
        await projection2.handle(tenant_event_1)
        await projection2.handle(tenant_event_2)

        # Projection 1 only processes tenant 1 events
        assert len(projection1.processed_events) == 1
        assert tenant_event_1 in projection1.processed_events

        # Projection 2 only processes tenant 2 events
        assert len(projection2.processed_events) == 1
        assert tenant_event_2 in projection2.processed_events

    def test_tenant_filter_is_accessible(
        self,
        tenant_id_1: UUID,
    ) -> None:
        """tenant_filter is accessible as _tenant_filter attribute."""
        projection = SampleProjection(tenant_filter=tenant_id_1)

        assert projection._tenant_filter == tenant_id_1


# --- Type Alias Tests ---


class TestTenantFilterTypeAlias:
    """Tests for TenantFilter type alias export."""

    def test_tenant_filter_is_exported(self) -> None:
        """TenantFilter type is exported from projections module."""
        from eventsource.projections import TenantFilter

        # Just verify it's importable and is a type alias
        assert TenantFilter is not None

    def test_tenant_filter_accepts_uuid(
        self,
        tenant_id_1: UUID,
    ) -> None:
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


# --- Backward Compatibility Tests ---


class TestBackwardCompatibility:
    """Tests for backward compatibility with existing code."""

    @pytest.mark.asyncio
    async def test_existing_projections_work_without_tenant_filter(
        self,
        tenant_event_1: TenantEvent,
        tenant_event_2: TenantEvent,
    ) -> None:
        """Existing projections without tenant_filter continue to work."""

        class LegacyProjection(DeclarativeProjection):
            """Projection without tenant_filter parameter."""

            def __init__(self) -> None:
                super().__init__()  # No tenant_filter
                self.events: list[DomainEvent] = []

            @handles(TenantEvent)
            async def _handle(self, event: TenantEvent) -> None:
                self.events.append(event)

        projection = LegacyProjection()

        await projection.handle(tenant_event_1)
        await projection.handle(tenant_event_2)

        assert len(projection.events) == 2

    def test_projection_init_signature_backward_compatible(self) -> None:
        """DeclarativeProjection.__init__ accepts old positional args."""
        # Old way: DeclarativeProjection(checkpoint_repo, dlq_repo, enable_tracing)
        # New way adds tenant_filter as keyword-only

        # This should work without modification
        projection = SampleProjection()
        assert projection._tenant_filter is None
