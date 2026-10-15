"""Fixtures and sample event models for test harness tests."""

from __future__ import annotations

from pydantic import BaseModel

from eventsource.domain.aggregate import DeclarativeAggregate
from eventsource.domain.decorators import handles
from eventsource.domain.event import DomainEvent
from eventsource.domain.event_registry import register_event


@register_event
class HarnessSampleEvent(DomainEvent):
    """Sample event for testing."""

    aggregate_type: str = "Sample"


@register_event
class HarnessOtherEvent(DomainEvent):
    """Another event type for testing filtering."""

    aggregate_type: str = "Other"


class SampleOrderState(BaseModel):
    """Sample state for testing create_repository."""

    step_count: int = 0


class SampleOrderAggregate(DeclarativeAggregate[SampleOrderState]):
    """Sample aggregate for testing harness.create_repository."""

    aggregate_type = "Sample"

    def _get_initial_state(self) -> SampleOrderState:
        return SampleOrderState()

    def record_step(self) -> None:
        self.apply_event(
            HarnessSampleEvent(
                aggregate_id=self.aggregate_id,
                aggregate_type=self.aggregate_type,
                aggregate_version=self.get_next_version(),
            )
        )

    @handles(HarnessSampleEvent)
    def _on_sample(self, event: HarnessSampleEvent) -> None:
        state = self._state or self._get_initial_state()
        self._state = state.model_copy(update={"step_count": state.step_count + 1})
