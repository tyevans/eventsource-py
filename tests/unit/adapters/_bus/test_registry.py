"""Unit and property tests for SubscriptionRegistry."""

from typing import Any

from hypothesis import given
from hypothesis import strategies as st

from eventsource.adapters._bus.registry import SubscriptionRegistry
from eventsource.domain.event import DomainEvent


class RegistryEventA(DomainEvent):
    aggregate_type: str = "Registry"


class RegistryEventB(DomainEvent):
    aggregate_type: str = "Registry"


def _handler(event: DomainEvent) -> None:
    """A no-op handler used purely as an identity token."""


def _make_handlers() -> list[Any]:
    def make(idx: int) -> Any:
        def h(event: DomainEvent) -> None: ...

        h.__name__ = f"handler_{idx}"
        return h

    return [make(i) for i in range(5)]


def test_add_then_handlers_for_returns_the_handler() -> None:
    registry = SubscriptionRegistry()
    registry.add(RegistryEventA, _handler)
    handlers = registry.handlers_for(RegistryEventA)
    assert len(handlers) == 1 and handlers[0].original is _handler


def test_handlers_for_unknown_type_is_empty() -> None:
    assert SubscriptionRegistry().handlers_for(RegistryEventA) == ()


def test_remove_returns_true_when_present_false_when_absent() -> None:
    registry = SubscriptionRegistry()
    registry.add(RegistryEventA, _handler)
    assert registry.remove(RegistryEventA, _handler) is True
    assert registry.remove(RegistryEventA, _handler) is False


def test_specific_handlers_precede_wildcard_handlers() -> None:
    registry = SubscriptionRegistry()
    registry.add_wildcard(_handler)
    other = _make_handlers()[0]
    registry.add(RegistryEventA, other)
    handlers = registry.handlers_for(RegistryEventA)
    assert [h.original for h in handlers] == [other, _handler]


def test_wildcard_reaches_every_event_type() -> None:
    registry = SubscriptionRegistry()
    registry.add_wildcard(_handler)
    assert len(registry.handlers_for(RegistryEventA)) == 1
    assert len(registry.handlers_for(RegistryEventB)) == 1


def test_handlers_for_is_stable_across_calls_without_mutation() -> None:
    registry = SubscriptionRegistry()
    registry.add(RegistryEventA, _handler)
    assert registry.handlers_for(RegistryEventA) is registry.handlers_for(RegistryEventA)


def test_handlers_for_cache_is_keyed_by_event_type_and_reused() -> None:
    registry = SubscriptionRegistry()
    registry.add(RegistryEventA, _handler)
    registry.add_wildcard(_make_handlers()[0])
    first = registry.handlers_for(RegistryEventA)
    assert first is registry.handlers_for(RegistryEventA) and len(first) == 2
    third = registry.handlers_for(RegistryEventB)
    assert third is not first and len(third) == 1


def test_cache_is_invalidated_on_mutation() -> None:
    registry = SubscriptionRegistry()
    registry.add(RegistryEventA, _handler)
    before = registry.handlers_for(RegistryEventA)
    registry.add(RegistryEventA, _make_handlers()[0])
    after = registry.handlers_for(RegistryEventA)
    assert before is not after and len(after) == 2


def test_wildcard_mutation_invalidates_specific_type_cache() -> None:
    registry = SubscriptionRegistry()
    registry.add(RegistryEventA, _handler)
    before = registry.handlers_for(RegistryEventA)
    registry.add_wildcard(_make_handlers()[0])
    after = registry.handlers_for(RegistryEventA)
    assert before is not after and len(after) == 2


def test_count_and_wildcard_count() -> None:
    registry = SubscriptionRegistry()
    registry.add(RegistryEventA, _handler)
    registry.add(RegistryEventA, _make_handlers()[0])
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
    assert registry.count() == 0 and registry.wildcard_count() == 0


def test_add_subscriber_registers_every_declared_type() -> None:
    class Subscriber:
        def subscribed_to(self) -> list[type[DomainEvent]]:
            return [RegistryEventA, RegistryEventB]

        async def handle(self, event: DomainEvent) -> None: ...

    registry = SubscriptionRegistry()
    registry.add_subscriber(Subscriber())
    assert registry.count(RegistryEventA) == 1 and registry.count(RegistryEventB) == 1


def test_the_same_handler_may_subscribe_twice() -> None:
    registry = SubscriptionRegistry()
    registry.add(RegistryEventA, _handler)
    registry.add(RegistryEventA, _handler)
    assert registry.count(RegistryEventA) == 2
    assert registry.remove(RegistryEventA, _handler) is True
    assert registry.count(RegistryEventA) == 1


_OPS = st.lists(
    st.tuples(st.integers(min_value=0, max_value=3), st.integers(min_value=0, max_value=4)),
    max_size=40,
)


@given(ops=_OPS)
def test_counts_track_net_operations(ops: list[tuple[int, int]]) -> None:
    handlers = _make_handlers()
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
def test_handlers_for_always_orders_specific_before_wildcard(ops: list[tuple[int, int]]) -> None:
    handlers = _make_handlers()
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
    assert len(combined[:specific_count]) == specific_count
    assert len(combined[specific_count:]) == registry.wildcard_count()
