"""Handler error isolation, as a property over arbitrary failure subsets."""

import asyncio
from collections.abc import Awaitable, Callable
from uuid import uuid4

from hypothesis import given, settings
from hypothesis import strategies as st

from eventsource.adapters.memory.bus import InMemoryEventBus
from eventsource.events.base import DomainEvent


class IsolationEvent(DomainEvent):
    event_type: str = "IsolationEvent"
    aggregate_type: str = "Isolation"


async def _run_case(failing: list[bool]) -> tuple[list[int], dict[str, int]]:
    """Subscribe one handler per flag, publish once, report what happened."""
    bus = InMemoryEventBus()
    succeeded: list[int] = []

    def make(idx: int, fails: bool) -> Callable[[DomainEvent], Awaitable[None]]:
        async def handler(event: DomainEvent) -> None:
            if fails:
                raise ValueError(f"handler {idx} failed")
            succeeded.append(idx)

        return handler

    for index, should_fail in enumerate(failing):
        bus.subscribe(IsolationEvent, make(index, should_fail))

    # Must not raise, regardless of how many handlers fail.
    await bus.publish([IsolationEvent(aggregate_id=uuid4())])

    return succeeded, bus.get_stats()


# NOTE: this test is deliberately SYNC and drives the async body with
# asyncio.run. Hypothesis has no native async support -- applying @given to an
# `async def` makes Hypothesis call it, receive a coroutine it never awaits,
# and pass without executing a single assertion. Do not "simplify" this by
# making the test async.
@given(failing=st.lists(st.booleans(), min_size=1, max_size=12))
@settings(deadline=None)
def test_failing_handlers_never_starve_the_others(failing: list[bool]) -> None:
    """For any subset of handlers that raise, the rest still receive the event
    and the bus records exactly one error per failing handler."""
    succeeded, stats = asyncio.run(_run_case(failing))

    expected_ok = [i for i, fails in enumerate(failing) if not fails]
    assert sorted(succeeded) == expected_ok
    assert stats["handler_errors"] == sum(failing)
    assert stats["handlers_invoked"] == len(expected_ok)
