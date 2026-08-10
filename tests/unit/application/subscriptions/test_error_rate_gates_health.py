"""
Unit tests for `ErrorStats.error_rate_per_minute` and the health gate it feeds.

Covers task #19. The field was declared on `ErrorStats`, serialized by
`to_dict()`, and read at four sites that gate health -- `health.py`'s
`_check_errors` (DEGRADED when the rate exceeds
`HealthCheckConfig.max_error_rate_per_minute`), two sites in
`health_provider.py`, and `error_handling.py`'s own threshold check. It had
**no write site**: `ErrorStats.record_error()` updated every other counter and
left this one at its `0.0` default forever.

So the comparison `error_rate > config.max_error_rate_per_minute` was
`0.0 > 10.0` on every call, and a subscription failing every event reported
healthy on the rate axis. The `events_failed` count could still trip the
count-based thresholds, which is why this was invisible: the *other* half of
the same indicator worked.

These tests follow `.claude/rules/definition-of-done.md`'s new-feature item 3:
they drive the **real caller** -- `SubscriptionErrorHandler.handle_error()`,
the only thing in the tree that records an error -- and then inspect the
mechanism. A test that set `stats.error_rate_per_minute` directly would pass
against the broken code, which is exactly how this survived.
"""

import asyncio
from uuid import uuid4

from eventsource.application.subscriptions.config import SubscriptionConfig
from eventsource.application.subscriptions.error_handling import (
    ErrorStats,
    SubscriptionErrorHandler,
)
from eventsource.application.subscriptions.health import (
    HealthCheckConfig,
    HealthStatus,
    SubscriptionHealthChecker,
)
from eventsource.application.subscriptions.subscription import Subscription
from eventsource.domain.event import DomainEvent
from eventsource.domain.event_registry import EventRegistry, register_event

_REGISTRY = EventRegistry()


@register_event(registry=_REGISTRY)
class RateTestEvent(DomainEvent):
    """Simple test event for error-rate tests."""

    aggregate_type: str = "RateAggregate"
    data: str = "test"


class NoopSubscriber:
    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [RateTestEvent]

    async def handle(self, event: DomainEvent) -> None:
        return None


def make_event() -> RateTestEvent:
    return RateTestEvent(aggregate_id=uuid4(), data="x")


async def record_failures(handler: SubscriptionErrorHandler, count: int) -> None:
    """Drive the real error-recording path `count` times."""
    for i in range(count):
        await handler.handle_error(ValueError(f"boom {i}"), None, make_event())


class TestErrorRateIsWritten:
    async def test_rate_is_zero_before_any_error(self) -> None:
        """The default is still 0.0 -- the fix must not invent a rate."""
        handler = SubscriptionErrorHandler(subscription_name="rate-zero")
        assert handler.stats.error_rate_per_minute == 0.0

    async def test_recording_errors_raises_the_rate(self) -> None:
        """Driving the real caller moves the rate off zero. This is the
        assertion that did not exist: ~60 tests covered these stats and none
        asserted this field after actually recording an error."""
        handler = SubscriptionErrorHandler(subscription_name="rate-rises")

        await record_failures(handler, 5)

        assert handler.stats.error_rate_per_minute == 5.0

    async def test_rate_counts_only_the_last_minute(self) -> None:
        """It is a *rate*, not a running total. A stats object whose errors are
        all older than the window reports zero, so an incident an hour ago does
        not keep a recovered subscription permanently DEGRADED."""
        stats = ErrorStats()
        # Reach past the public surface deliberately: the alternative is
        # sleeping 60 seconds in a unit test.
        stats._rate_bucket_stamps = [s - 120 for s in stats._rate_bucket_stamps]

        assert stats.error_rate_per_minute == 0.0

    async def test_rate_appears_in_to_dict(self) -> None:
        """`to_dict()` has always emitted this key; it must now emit a real
        number rather than a constant zero."""
        handler = SubscriptionErrorHandler(subscription_name="rate-dict")

        await record_failures(handler, 3)

        assert handler.stats.to_dict()["error_rate_per_minute"] == 3.0


class TestErrorRateGatesHealth:
    """The reason the field exists: it is supposed to degrade health."""

    def _checker(
        self, handler: SubscriptionErrorHandler, max_rate: float
    ) -> SubscriptionHealthChecker:
        subscription = Subscription(
            name="rate-health",
            config=SubscriptionConfig(),
            subscriber=NoopSubscriber(),
        )
        return SubscriptionHealthChecker(
            subscription=subscription,
            # Push the count-based thresholds out of reach so this test can
            # only pass via the *rate* axis -- otherwise events_failed would
            # trip DEGRADED on its own and prove nothing.
            config=HealthCheckConfig(
                max_errors_warning=10_000,
                max_errors_critical=100_000,
                max_error_rate_per_minute=max_rate,
            ),
            error_handler=handler,
        )

    async def test_high_error_rate_degrades_health(self) -> None:
        """A subscription erroring above the configured rate reports DEGRADED.
        Before the fix this reported HEALTHY no matter how many errors were
        recorded, because the comparison was always `0.0 > threshold`."""
        handler = SubscriptionErrorHandler(subscription_name="rate-degrades")
        await record_failures(handler, 12)

        indicator = self._checker(handler, max_rate=10.0)._check_errors()

        assert indicator.status == HealthStatus.DEGRADED
        assert indicator.details["error_rate_per_minute"] == 12.0

    async def test_rate_below_threshold_stays_healthy(self) -> None:
        """The control -- the gate must not fire below its threshold."""
        handler = SubscriptionErrorHandler(subscription_name="rate-ok")
        await record_failures(handler, 3)

        indicator = self._checker(handler, max_rate=10.0)._check_errors()

        assert indicator.status == HealthStatus.HEALTHY
        assert indicator.details["error_rate_per_minute"] == 3.0


class TestConcurrentRecording:
    async def test_concurrent_errors_are_all_counted(self) -> None:
        """`handle_error` takes a lock around stat recording; the rate must not
        lose increments under concurrency."""
        handler = SubscriptionErrorHandler(subscription_name="rate-concurrent")

        await asyncio.gather(
            *(handler.handle_error(ValueError("x"), None, make_event()) for _ in range(20))
        )

        assert handler.stats.error_rate_per_minute == 20.0
