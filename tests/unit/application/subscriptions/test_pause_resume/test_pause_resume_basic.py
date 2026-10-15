"""Unit tests for subscription pause/resume basic functionality and properties."""

import asyncio
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from eventsource.application.subscriptions import (
    PauseReason,
    Subscription,
    SubscriptionState,
)
from tests.unit.application.subscriptions.test_pause_resume._fixtures import pos


class TestPauseReasonEnum:
    def test_manual_value(self):
        assert PauseReason.MANUAL.value == "manual"

    def test_backpressure_value(self):
        assert PauseReason.BACKPRESSURE.value == "backpressure"

    def test_maintenance_value(self):
        assert PauseReason.MAINTENANCE.value == "maintenance"

    def test_all_reasons_exist(self):
        reasons = list(PauseReason)
        assert len(reasons) == 3
        assert (
            PauseReason.MANUAL in reasons
            and PauseReason.BACKPRESSURE in reasons
            and PauseReason.MAINTENANCE in reasons
        )


class TestSubscriptionPause:
    @pytest.mark.asyncio
    async def test_pause_from_catching_up(self, subscription: Subscription):
        await subscription.transition_to(SubscriptionState.CATCHING_UP)
        await subscription.pause()
        assert subscription.state == SubscriptionState.PAUSED

    @pytest.mark.asyncio
    async def test_pause_from_live(self, subscription: Subscription):
        await subscription.transition_to(SubscriptionState.LIVE)
        await subscription.pause()
        assert subscription.state == SubscriptionState.PAUSED

    @pytest.mark.asyncio
    async def test_pause_stores_previous_state_catching_up(self, subscription: Subscription):
        await subscription.transition_to(SubscriptionState.CATCHING_UP)
        await subscription.pause()
        assert subscription.state_before_pause == SubscriptionState.CATCHING_UP

    @pytest.mark.asyncio
    async def test_pause_stores_previous_state_live(self, subscription: Subscription):
        await subscription.transition_to(SubscriptionState.LIVE)
        await subscription.pause()
        assert subscription.state_before_pause == SubscriptionState.LIVE

    @pytest.mark.asyncio
    async def test_pause_with_manual_reason(self, subscription: Subscription):
        await subscription.transition_to(SubscriptionState.LIVE)
        await subscription.pause(reason=PauseReason.MANUAL)
        assert subscription.pause_reason == PauseReason.MANUAL

    @pytest.mark.asyncio
    async def test_pause_with_backpressure_reason(self, subscription: Subscription):
        await subscription.transition_to(SubscriptionState.LIVE)
        await subscription.pause(reason=PauseReason.BACKPRESSURE)
        assert subscription.pause_reason == PauseReason.BACKPRESSURE

    @pytest.mark.asyncio
    async def test_pause_with_maintenance_reason(self, subscription: Subscription):
        await subscription.transition_to(SubscriptionState.LIVE)
        await subscription.pause(reason=PauseReason.MAINTENANCE)
        assert subscription.pause_reason == PauseReason.MAINTENANCE

    @pytest.mark.asyncio
    async def test_pause_default_reason_is_manual(self, subscription: Subscription):
        await subscription.transition_to(SubscriptionState.LIVE)
        await subscription.pause()
        assert subscription.pause_reason == PauseReason.MANUAL

    @pytest.mark.asyncio
    async def test_pause_sets_paused_at_timestamp(self, subscription: Subscription):
        await subscription.transition_to(SubscriptionState.LIVE)
        before = datetime.now(UTC)
        await subscription.pause()
        after = datetime.now(UTC)
        assert subscription.paused_at is not None and before <= subscription.paused_at <= after

    @pytest.mark.asyncio
    async def test_pause_clears_pause_event(self, subscription: Subscription):
        await subscription.transition_to(SubscriptionState.LIVE)
        assert subscription._pause_event.is_set()
        await subscription.pause()
        assert not subscription._pause_event.is_set()

    @pytest.mark.asyncio
    async def test_pause_maintains_position(self, subscription: Subscription):
        await subscription.transition_to(SubscriptionState.LIVE)
        await subscription.record_event_processed(
            position=pos(42), event_id=uuid4(), event_type="TestEvent"
        )
        await subscription.pause()
        assert subscription.last_processed_position == pos(42)


class TestSubscriptionResume:
    @pytest.mark.asyncio
    async def test_resume_from_paused_to_catching_up(self, subscription: Subscription):
        await subscription.transition_to(SubscriptionState.CATCHING_UP)
        await subscription.pause()
        await subscription.resume()
        assert subscription.state == SubscriptionState.CATCHING_UP

    @pytest.mark.asyncio
    async def test_resume_from_paused_to_live(self, subscription: Subscription):
        await subscription.transition_to(SubscriptionState.LIVE)
        await subscription.pause()
        await subscription.resume()
        assert subscription.state == SubscriptionState.LIVE

    @pytest.mark.asyncio
    async def test_resume_clears_pause_reason(self, subscription: Subscription):
        await subscription.transition_to(SubscriptionState.LIVE)
        await subscription.pause(reason=PauseReason.MAINTENANCE)
        assert subscription.pause_reason == PauseReason.MAINTENANCE
        await subscription.resume()
        assert subscription.pause_reason is None

    @pytest.mark.asyncio
    async def test_resume_clears_paused_at(self, subscription: Subscription):
        await subscription.transition_to(SubscriptionState.LIVE)
        await subscription.pause()
        assert subscription.paused_at is not None
        await subscription.resume()
        assert subscription.paused_at is None

    @pytest.mark.asyncio
    async def test_resume_sets_pause_event(self, subscription: Subscription):
        await subscription.transition_to(SubscriptionState.LIVE)
        await subscription.pause()
        assert not subscription._pause_event.is_set()
        await subscription.resume()
        assert subscription._pause_event.is_set()

    @pytest.mark.asyncio
    async def test_resume_maintains_position(self, subscription: Subscription):
        await subscription.transition_to(SubscriptionState.LIVE)
        await subscription.record_event_processed(
            position=pos(42), event_id=uuid4(), event_type="TestEvent"
        )
        await subscription.pause()
        await subscription.resume()
        assert subscription.last_processed_position == pos(42)


class TestPauseProperties:
    @pytest.mark.asyncio
    async def test_is_paused_true_when_paused(self, subscription: Subscription):
        await subscription.transition_to(SubscriptionState.LIVE)
        await subscription.pause()
        assert subscription.is_paused is True

    @pytest.mark.asyncio
    async def test_is_paused_false_when_not_paused(self, subscription: Subscription):
        await subscription.transition_to(SubscriptionState.LIVE)
        assert subscription.is_paused is False

    @pytest.mark.asyncio
    async def test_pause_duration_seconds_during_pause(self, subscription: Subscription):
        await subscription.transition_to(SubscriptionState.LIVE)
        await subscription.pause()
        await asyncio.sleep(0.1)
        duration = subscription.pause_duration_seconds
        assert duration is not None and duration >= 0.1

    @pytest.mark.asyncio
    async def test_pause_duration_seconds_none_when_not_paused(self, subscription: Subscription):
        await subscription.transition_to(SubscriptionState.LIVE)
        assert subscription.pause_duration_seconds is None

    @pytest.mark.asyncio
    async def test_state_before_pause_none_when_not_paused(self, subscription: Subscription):
        assert subscription.state_before_pause is None

    @pytest.mark.asyncio
    async def test_pause_reason_none_when_not_paused(self, subscription: Subscription):
        await subscription.transition_to(SubscriptionState.LIVE)
        assert subscription.pause_reason is None


class TestPauseResumeModuleImports:
    def test_import_pause_reason_from_subscriptions(self):
        from eventsource.application.subscriptions import PauseReason

        assert PauseReason is not None
        assert hasattr(PauseReason, "MANUAL")
        assert hasattr(PauseReason, "BACKPRESSURE")
        assert hasattr(PauseReason, "MAINTENANCE")

    def test_import_pause_reason_from_subscription_module(self):
        from eventsource.application.subscriptions.subscription import PauseReason

        assert PauseReason is not None
