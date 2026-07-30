"""Unit and property tests for SubscriptionRegistry."""

from typing import Any

from hypothesis import given
from hypothesis import strategies as st

from eventsource.bus.registry import SubscriptionRegistry
from eventsource.events.base import DomainEvent


class RegistryEventA(DomainEvent):
    event_type: str = "RegistryEventA"
    aggregate_type: str = "Registry"


class RegistryEventB(DomainEvent):
    event_type: str = "RegistryEventB"
    aggregate_type: str = "Registry"


def _handler(event: DomainEvent) -> None:
    """A no-op handler used purely as an identity token."""


def test_add_then_handlers_for_returns_the_handler() -> None:
    registry = SubscriptionRegistry()
    registry.add(RegistryEventA, _handler)

    handlers = registry.handlers_for(RegistryEventA)

    assert len(handlers) == 1
    assert handlers[0].original is _handler


def test_handlers_for_unknown_type_is_empty() -> None:
    registry = SubscriptionRegistry()

    assert registry.handlers_for(RegistryEventA) == ()


def test_remove_returns_true_when_present_false_when_absent() -> None:
    registry = SubscriptionRegistry()
    registry.add(RegistryEventA, _handler)

    assert registry.remove(RegistryEventA, _handler) is True
    assert registry.remove(RegistryEventA, _handler) is False


def test_specific_handlers_precede_wildcard_handlers() -> None:
    registry = SubscriptionRegistry()

    def wildcard(event: DomainEvent) -> None: ...

    registry.add_wildcard(wildcard)
    registry.add(RegistryEventA, _handler)

    handlers = registry.handlers_for(RegistryEventA)

    assert [h.original for h in handlers] == [_handler, wildcard]


def test_wildcard_reaches_every_event_type() -> None:
    registry = SubscriptionRegistry()
    registry.add_wildcard(_handler)

    assert len(registry.handlers_for(RegistryEventA)) == 1
    assert len(registry.handlers_for(RegistryEventB)) == 1


def test_handlers_for_is_stable_across_calls_without_mutation() -> None:
    """The combined tuple is cached, so dispatch allocates nothing per event."""
    registry = SubscriptionRegistry()
    registry.add(RegistryEventA, _handler)

    first = registry.handlers_for(RegistryEventA)
    second = registry.handlers_for(RegistryEventA)

    assert first is second


def test_handlers_for_cache_is_keyed_by_event_type_and_reused() -> None:
    """Regression test: a naive cache keyed by the wrong value, or one that
    stores ``None`` instead of the combined tuple, would still pass an
    identity check when concatenating a non-empty tuple with an *empty*
    wildcard tuple -- CPython's tuple ``+`` returns the left operand
    unchanged in that case. Registering a wildcard handler too forces a real
    concatenation each time, so this only passes if the cache genuinely
    short-circuits recomputation.
    """
    registry = SubscriptionRegistry()
    registry.add(RegistryEventA, _handler)

    def wildcard(event: DomainEvent) -> None: ...

    registry.add_wildcard(wildcard)

    first = registry.handlers_for(RegistryEventA)
    second = registry.handlers_for(RegistryEventA)

    assert first is second
    assert len(first) == 2

    # A second, distinct event type must get its own cache entry rather than
    # sharing (or clobbering) RegistryEventA's.
    third = registry.handlers_for(RegistryEventB)
    assert third is not first
    assert len(third) == 1


def test_cache_is_invalidated_on_mutation() -> None:
    registry = SubscriptionRegistry()
    registry.add(RegistryEventA, _handler)
    before = registry.handlers_for(RegistryEventA)

    def other(event: DomainEvent) -> None: ...

    registry.add(RegistryEventA, other)
    after = registry.handlers_for(RegistryEventA)

    assert before is not after
    assert len(after) == 2


def test_wildcard_mutation_invalidates_specific_type_cache() -> None:
    registry = SubscriptionRegistry()
    registry.add(RegistryEventA, _handler)
    before = registry.handlers_for(RegistryEventA)

    def wildcard(event: DomainEvent) -> None: ...

    registry.add_wildcard(wildcard)
    after = registry.handlers_for(RegistryEventA)

    assert before is not after
    assert len(after) == 2


def test_count_and_wildcard_count() -> None:
    registry = SubscriptionRegistry()

    def other(event: DomainEvent) -> None: ...

    registry.add(RegistryEventA, _handler)
    registry.add(RegistryEventA, other)
    registry.add(RegistryEventB, _handler)
    registry.add_wildcard(_handler)

    assert registry.count() == 3
    assert registry.count(RegistryEventA) == 2
    assert registry.count(RegistryEventB) == 1
    assert registry.wildcard_count() == 1


def test_clear_removes_everything() -> None:
    registry = SubscriptionRegistry()
    registry.add(RegistryEventA, _handler)
    registry.add_wildcard(_handler)

    registry.clear()

    assert registry.count() == 0
    assert registry.wildcard_count() == 0


def test_add_subscriber_registers_every_declared_type() -> None:
    class Subscriber:
        def subscribed_to(self) -> list[type[DomainEvent]]:
            return [RegistryEventA, RegistryEventB]

        async def handle(self, event: DomainEvent) -> None: ...

    registry = SubscriptionRegistry()
    registry.add_subscriber(Subscriber())

    assert registry.count(RegistryEventA) == 1
    assert registry.count(RegistryEventB) == 1


def test_the_same_handler_may_subscribe_twice() -> None:
    """Duplicate registration is allowed; each remove strips one."""
    registry = SubscriptionRegistry()
    registry.add(RegistryEventA, _handler)
    registry.add(RegistryEventA, _handler)

    assert registry.count(RegistryEventA) == 2
    assert registry.remove(RegistryEventA, _handler) is True
    assert registry.count(RegistryEventA) == 1


# =============================================================================
# Property tests
# =============================================================================

# Each op is (kind, handler_index). kind: 0=add, 1=remove, 2=add_wc, 3=remove_wc
_OPS = st.lists(
    st.tuples(st.integers(min_value=0, max_value=3), st.integers(min_value=0, max_value=4)),
    max_size=40,
)


@given(ops=_OPS)
def test_counts_track_net_operations(ops: list[tuple[int, int]]) -> None:
    """count() and wildcard_count() always equal the net number of live handlers."""
    handlers: list[Any] = []
    for i in range(5):

        def make(idx: int) -> Any:
            def h(event: DomainEvent) -> None: ...

            h.__name__ = f"handler_{idx}"
            return h

        handlers.append(make(i))

    registry = SubscriptionRegistry()
    specific: list[Any] = []
    wildcard: list[Any] = []

    for kind, idx in ops:
        handler = handlers[idx]
        if kind == 0:
            registry.add(RegistryEventA, handler)
            specific.append(handler)
        elif kind == 1:
            expected = handler in specific
            assert registry.remove(RegistryEventA, handler) is expected
            if expected:
                specific.remove(handler)
        elif kind == 2:
            registry.add_wildcard(handler)
            wildcard.append(handler)
        else:
            expected = handler in wildcard
            assert registry.remove_wildcard(handler) is expected
            if expected:
                wildcard.remove(handler)

        assert registry.count(RegistryEventA) == len(specific)
        assert registry.wildcard_count() == len(wildcard)
        assert len(registry.handlers_for(RegistryEventA)) == len(specific) + len(wildcard)


@given(ops=_OPS)
def test_handlers_for_always_orders_specific_before_wildcard(
    ops: list[tuple[int, int]],
) -> None:
    handlers: list[Any] = []
    for i in range(5):

        def make(idx: int) -> Any:
            def h(event: DomainEvent) -> None: ...

            h.__name__ = f"handler_{idx}"
            return h

        handlers.append(make(i))

    registry = SubscriptionRegistry()
    specific_count = 0

    for kind, idx in ops:
        handler = handlers[idx]
        if kind == 0:
            registry.add(RegistryEventA, handler)
            specific_count += 1
        elif kind == 1:
            if registry.remove(RegistryEventA, handler):
                specific_count -= 1
        elif kind == 2:
            registry.add_wildcard(handler)
        else:
            registry.remove_wildcard(handler)

    combined = registry.handlers_for(RegistryEventA)
    assert len(combined) == registry.count(RegistryEventA) + registry.wildcard_count()
    # The first `specific_count` entries are the type-specific ones; the rest
    # are wildcards. Verified through the public API only.
    assert len(combined[:specific_count]) == specific_count
    assert len(combined[specific_count:]) == registry.wildcard_count()
