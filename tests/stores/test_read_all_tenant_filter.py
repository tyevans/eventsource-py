"""
Unit tests for tenant_id filtering in read_all() method.

These tests verify the PREREQ-001 implementation that adds tenant_id
filtering capability to the EventStore's read_all() method.

Tests cover:
- Filtering events by tenant_id returns only that tenant's events
- No tenant filter returns all events (backward compatibility)
- Combination of tenant filter with other options (limit, from_position)
- Empty result when tenant has no events
- Tenant filter works correctly with multiple tenants
"""

from uuid import UUID, uuid4

import pytest

from eventsource.events.base import DomainEvent
from eventsource.stores.in_memory import InMemoryEventStore
from eventsource.stores.interface import ReadDirection, ReadOptions

# --- Test Event Classes ---


class TenantTestEvent(DomainEvent):
    """Test event with tenant support."""

    event_type: str = "TenantTestEvent"
    aggregate_type: str = "TenantAggregate"
    data: str = "test"


# --- Fixtures ---


@pytest.fixture
def store() -> InMemoryEventStore:
    """Create a fresh InMemoryEventStore for each test."""
    return InMemoryEventStore()


@pytest.fixture
def tenant_a() -> UUID:
    """Create tenant A UUID."""
    return uuid4()


@pytest.fixture
def tenant_b() -> UUID:
    """Create tenant B UUID."""
    return uuid4()


@pytest.fixture
def aggregate_id() -> UUID:
    """Create a random aggregate ID."""
    return uuid4()


# --- Basic Tenant Filtering Tests ---


class TestReadAllTenantFilter:
    """Tests for tenant_id filtering in read_all()."""

    @pytest.mark.asyncio
    async def test_filter_by_tenant_returns_only_tenant_events(
        self,
        store: InMemoryEventStore,
        tenant_a: UUID,
        tenant_b: UUID,
    ) -> None:
        """Test that filtering by tenant_id returns only that tenant's events."""
        # Create events for tenant A
        agg_a = uuid4()
        events_a = [
            TenantTestEvent(aggregate_id=agg_a, data="tenant_a_1", tenant_id=tenant_a),
            TenantTestEvent(aggregate_id=agg_a, data="tenant_a_2", tenant_id=tenant_a),
        ]
        await store.append_events(
            aggregate_id=agg_a,
            aggregate_type="TenantAggregate",
            events=events_a,
            expected_version=0,
        )

        # Create events for tenant B
        agg_b = uuid4()
        events_b = [
            TenantTestEvent(aggregate_id=agg_b, data="tenant_b_1", tenant_id=tenant_b),
            TenantTestEvent(aggregate_id=agg_b, data="tenant_b_2", tenant_id=tenant_b),
            TenantTestEvent(aggregate_id=agg_b, data="tenant_b_3", tenant_id=tenant_b),
        ]
        await store.append_events(
            aggregate_id=agg_b,
            aggregate_type="TenantAggregate",
            events=events_b,
            expected_version=0,
        )

        # Filter by tenant A
        options = ReadOptions(tenant_id=tenant_a)
        stored_events = []
        async for stored in store.read_all(options):
            stored_events.append(stored)

        assert len(stored_events) == 2
        for stored in stored_events:
            assert stored.event.tenant_id == tenant_a
            assert stored.event.data.startswith("tenant_a")

    @pytest.mark.asyncio
    async def test_no_tenant_filter_returns_all_events(
        self,
        store: InMemoryEventStore,
        tenant_a: UUID,
        tenant_b: UUID,
    ) -> None:
        """Test that no tenant filter returns all events (backward compatibility)."""
        # Create events for tenant A
        agg_a = uuid4()
        event_a = TenantTestEvent(aggregate_id=agg_a, data="tenant_a", tenant_id=tenant_a)
        await store.append_events(
            aggregate_id=agg_a,
            aggregate_type="TenantAggregate",
            events=[event_a],
            expected_version=0,
        )

        # Create events for tenant B
        agg_b = uuid4()
        event_b = TenantTestEvent(aggregate_id=agg_b, data="tenant_b", tenant_id=tenant_b)
        await store.append_events(
            aggregate_id=agg_b,
            aggregate_type="TenantAggregate",
            events=[event_b],
            expected_version=0,
        )

        # Create events without tenant
        agg_c = uuid4()
        event_c = TenantTestEvent(aggregate_id=agg_c, data="no_tenant")
        await store.append_events(
            aggregate_id=agg_c,
            aggregate_type="TenantAggregate",
            events=[event_c],
            expected_version=0,
        )

        # No tenant filter
        stored_events = []
        async for stored in store.read_all():
            stored_events.append(stored)

        assert len(stored_events) == 3

    @pytest.mark.asyncio
    async def test_tenant_filter_with_no_matching_events(
        self,
        store: InMemoryEventStore,
        tenant_a: UUID,
    ) -> None:
        """Test that filtering by tenant with no events returns empty result."""
        # Create events for tenant A
        agg = uuid4()
        event = TenantTestEvent(aggregate_id=agg, data="tenant_a", tenant_id=tenant_a)
        await store.append_events(
            aggregate_id=agg,
            aggregate_type="TenantAggregate",
            events=[event],
            expected_version=0,
        )

        # Filter by a different tenant
        other_tenant = uuid4()
        options = ReadOptions(tenant_id=other_tenant)
        stored_events = []
        async for stored in store.read_all(options):
            stored_events.append(stored)

        assert len(stored_events) == 0

    @pytest.mark.asyncio
    async def test_tenant_filter_with_events_without_tenant(
        self,
        store: InMemoryEventStore,
        tenant_a: UUID,
    ) -> None:
        """Test that events without tenant_id are not returned when filtering."""
        # Create event with tenant
        agg_a = uuid4()
        event_with_tenant = TenantTestEvent(
            aggregate_id=agg_a, data="with_tenant", tenant_id=tenant_a
        )
        await store.append_events(
            aggregate_id=agg_a,
            aggregate_type="TenantAggregate",
            events=[event_with_tenant],
            expected_version=0,
        )

        # Create event without tenant (None)
        agg_b = uuid4()
        event_without_tenant = TenantTestEvent(
            aggregate_id=agg_b, data="without_tenant"
        )  # tenant_id is None by default
        await store.append_events(
            aggregate_id=agg_b,
            aggregate_type="TenantAggregate",
            events=[event_without_tenant],
            expected_version=0,
        )

        # Filter by tenant A - should only get event_with_tenant
        options = ReadOptions(tenant_id=tenant_a)
        stored_events = []
        async for stored in store.read_all(options):
            stored_events.append(stored)

        assert len(stored_events) == 1
        assert stored_events[0].event.data == "with_tenant"


# --- Combination Filter Tests ---


class TestReadAllTenantFilterCombinations:
    """Tests for tenant filter combined with other options."""

    @pytest.mark.asyncio
    async def test_tenant_filter_with_limit(
        self,
        store: InMemoryEventStore,
        tenant_a: UUID,
    ) -> None:
        """Test tenant filter combined with limit."""
        # Create 5 events for tenant A
        agg = uuid4()
        events = [
            TenantTestEvent(aggregate_id=agg, data=f"event_{i}", tenant_id=tenant_a)
            for i in range(5)
        ]
        await store.append_events(
            aggregate_id=agg,
            aggregate_type="TenantAggregate",
            events=events,
            expected_version=0,
        )

        # Filter by tenant with limit 3
        options = ReadOptions(tenant_id=tenant_a, limit=3)
        stored_events = []
        async for stored in store.read_all(options):
            stored_events.append(stored)

        assert len(stored_events) == 3
        assert stored_events[0].event.data == "event_0"
        assert stored_events[1].event.data == "event_1"
        assert stored_events[2].event.data == "event_2"

    @pytest.mark.asyncio
    async def test_tenant_filter_with_from_position(
        self,
        store: InMemoryEventStore,
        tenant_a: UUID,
        tenant_b: UUID,
    ) -> None:
        """Test tenant filter combined with from_position."""
        # Create events for tenant A (global positions 1, 2)
        agg_a = uuid4()
        events_a = [
            TenantTestEvent(aggregate_id=agg_a, data="a_1", tenant_id=tenant_a),
            TenantTestEvent(aggregate_id=agg_a, data="a_2", tenant_id=tenant_a),
        ]
        await store.append_events(
            aggregate_id=agg_a,
            aggregate_type="TenantAggregate",
            events=events_a,
            expected_version=0,
        )

        # Create events for tenant B (global positions 3, 4)
        agg_b = uuid4()
        events_b = [
            TenantTestEvent(aggregate_id=agg_b, data="b_1", tenant_id=tenant_b),
            TenantTestEvent(aggregate_id=agg_b, data="b_2", tenant_id=tenant_b),
        ]
        await store.append_events(
            aggregate_id=agg_b,
            aggregate_type="TenantAggregate",
            events=events_b,
            expected_version=0,
        )

        # More events for tenant A (global positions 5, 6)
        more_events_a = [
            TenantTestEvent(aggregate_id=agg_a, data="a_3", tenant_id=tenant_a),
            TenantTestEvent(aggregate_id=agg_a, data="a_4", tenant_id=tenant_a),
        ]
        await store.append_events(
            aggregate_id=agg_a,
            aggregate_type="TenantAggregate",
            events=more_events_a,
            expected_version=2,
        )

        # Filter by tenant A, starting from position 3
        # Should skip a_1 (pos 1), a_2 (pos 2), and get a_3 (pos 5), a_4 (pos 6)
        options = ReadOptions(tenant_id=tenant_a, from_position=3)
        stored_events = []
        async for stored in store.read_all(options):
            stored_events.append(stored)

        assert len(stored_events) == 2
        assert stored_events[0].event.data == "a_3"
        assert stored_events[1].event.data == "a_4"

    @pytest.mark.asyncio
    async def test_tenant_filter_with_backward_direction(
        self,
        store: InMemoryEventStore,
        tenant_a: UUID,
    ) -> None:
        """Test tenant filter with backward reading direction."""
        # Create events for tenant A
        agg = uuid4()
        events = [
            TenantTestEvent(aggregate_id=agg, data=f"event_{i}", tenant_id=tenant_a)
            for i in range(3)
        ]
        await store.append_events(
            aggregate_id=agg,
            aggregate_type="TenantAggregate",
            events=events,
            expected_version=0,
        )

        # Filter by tenant with backward direction
        options = ReadOptions(tenant_id=tenant_a, direction=ReadDirection.BACKWARD)
        stored_events = []
        async for stored in store.read_all(options):
            stored_events.append(stored)

        assert len(stored_events) == 3
        # Events should be in reverse order
        assert stored_events[0].event.data == "event_2"
        assert stored_events[1].event.data == "event_1"
        assert stored_events[2].event.data == "event_0"

    @pytest.mark.asyncio
    async def test_tenant_filter_with_all_options(
        self,
        store: InMemoryEventStore,
        tenant_a: UUID,
        tenant_b: UUID,
    ) -> None:
        """Test tenant filter combined with limit, from_position, and direction."""
        # Create interleaved events for both tenants
        agg_a = uuid4()
        agg_b = uuid4()

        # Tenant A events at positions 1, 3, 5, 7
        for i in range(4):
            event = TenantTestEvent(aggregate_id=agg_a, data=f"a_{i}", tenant_id=tenant_a)
            await store.append_events(
                aggregate_id=agg_a,
                aggregate_type="TenantAggregate",
                events=[event],
                expected_version=i,
            )
            # Interleave with tenant B
            event_b = TenantTestEvent(aggregate_id=agg_b, data=f"b_{i}", tenant_id=tenant_b)
            await store.append_events(
                aggregate_id=agg_b,
                aggregate_type="TenantAggregate",
                events=[event_b],
                expected_version=i,
            )

        # Filter: tenant A, from position 2, limit 2, backward
        options = ReadOptions(
            tenant_id=tenant_a,
            from_position=2,  # Skip global positions 1-2
            limit=2,
            direction=ReadDirection.BACKWARD,
        )
        stored_events = []
        async for stored in store.read_all(options):
            stored_events.append(stored)

        # Should get tenant A events after position 2, in reverse, limited to 2
        # Tenant A positions: 1 (a_0), 3 (a_1), 5 (a_2), 7 (a_3)
        # After position 2: 3, 5, 7 -> reversed: 7, 5, 3 -> limited: 7, 5 (a_3, a_2)
        assert len(stored_events) == 2
        assert stored_events[0].event.data == "a_3"
        assert stored_events[1].event.data == "a_2"


# --- ReadOptions Validation Tests ---


class TestReadOptionsTenantField:
    """Tests for the tenant_id field in ReadOptions."""

    def test_read_options_default_tenant_is_none(self) -> None:
        """Test that default tenant_id is None."""
        options = ReadOptions()
        assert options.tenant_id is None

    def test_read_options_with_tenant_id(self) -> None:
        """Test creating ReadOptions with tenant_id."""
        tenant = uuid4()
        options = ReadOptions(tenant_id=tenant)
        assert options.tenant_id == tenant

    def test_read_options_tenant_id_with_other_fields(self) -> None:
        """Test creating ReadOptions with tenant_id and other fields."""
        tenant = uuid4()
        options = ReadOptions(
            direction=ReadDirection.BACKWARD,
            from_position=10,
            limit=50,
            tenant_id=tenant,
        )
        assert options.tenant_id == tenant
        assert options.direction == ReadDirection.BACKWARD
        assert options.from_position == 10
        assert options.limit == 50

    def test_read_options_is_frozen(self) -> None:
        """Test that ReadOptions is immutable (frozen dataclass)."""
        tenant = uuid4()
        options = ReadOptions(tenant_id=tenant)

        with pytest.raises(AttributeError):
            options.tenant_id = uuid4()  # type: ignore
