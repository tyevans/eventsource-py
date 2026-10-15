"""Property-based Hypothesis tests for DeciderAggregate._stamp identity and provenance invariants."""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

import pytest
from hypothesis import assume, given
from hypothesis import strategies as st
from pydantic import BaseModel

from eventsource.domain.command import DomainCommand
from eventsource.domain.decider import DeciderAggregate
from eventsource.domain.event import DomainEvent
from eventsource.domain.exceptions import (
    AggregateIdMismatchError,
    AggregateTypeMismatchError,
)
from eventsource.domain.stream_id import CATEGORY_PATTERN

_VALID_CATEGORY = st.from_regex(CATEGORY_PATTERN, fullmatch=True).filter(
    lambda s: s != "Unknown" and len(s) <= 32
)


class StampState(BaseModel):
    count: int = 0


@dataclass(frozen=True)
class EmitToTarget:
    target_id: UUID


class StampDomainCommand(DomainCommand):
    target_id: UUID


def _make_decider_class(
    agg_type: str,
    event_cls: type[DomainEvent],
) -> type[DeciderAggregate[StampState, object]]:
    class _DynamicDecider(DeciderAggregate[StampState, object]):
        aggregate_type = agg_type

        @staticmethod
        def initial_state() -> StampState:
            return StampState()

        @staticmethod
        def decide(command: object, state: StampState) -> list[DomainEvent]:
            if isinstance(command, EmitToTarget | StampDomainCommand):
                return [event_cls(aggregate_id=command.target_id)]
            return []

        @staticmethod
        def evolve(state: StampState, event: DomainEvent) -> StampState:
            return state.model_copy(update={"count": state.count + 1})

    return _DynamicDecider


@given(
    aggregate_id=st.uuids(),
    agg_type=_VALID_CATEGORY,
)
def test_stamp_matching_identity_stamps_type_and_increments_version(
    aggregate_id: UUID,
    agg_type: str,
) -> None:
    """Events with matching aggregate_id and matching class aggregate_type are stamped cleanly."""

    class MatchingEvent(DomainEvent):
        aggregate_type: str = agg_type

    decider_cls = _make_decider_class(agg_type, MatchingEvent)
    agg = decider_cls(aggregate_id)

    (stamped,) = agg.execute(EmitToTarget(target_id=aggregate_id))

    assert stamped.aggregate_id == aggregate_id
    assert stamped.aggregate_type == agg_type
    assert stamped.aggregate_version == 1
    assert agg.version == 1
    assert agg.state.count == 1


@given(
    aggregate_id=st.uuids(),
    foreign_id=st.uuids(),
    agg_type=_VALID_CATEGORY,
)
def test_stamp_rejects_foreign_aggregate_id(
    aggregate_id: UUID,
    foreign_id: UUID,
    agg_type: str,
) -> None:
    """Events naming a foreign aggregate_id are rejected with AggregateIdMismatchError."""
    assume(aggregate_id != foreign_id)

    class MatchingEvent(DomainEvent):
        aggregate_type: str = agg_type

    decider_cls = _make_decider_class(agg_type, MatchingEvent)
    agg = decider_cls(aggregate_id)

    with pytest.raises(AggregateIdMismatchError) as exc_info:
        agg.execute(EmitToTarget(target_id=foreign_id))

    assert exc_info.value.aggregate_id == aggregate_id
    assert exc_info.value.event_aggregate_id == foreign_id
    assert agg.version == 0
    assert agg.uncommitted_events == []


@given(
    aggregate_id=st.uuids(),
    agg_type=_VALID_CATEGORY,
    event_agg_type=_VALID_CATEGORY,
)
def test_stamp_rejects_divergent_event_class_aggregate_type(
    aggregate_id: UUID,
    agg_type: str,
    event_agg_type: str,
) -> None:
    """Events whose class declares a different aggregate_type raise AggregateTypeMismatchError."""
    assume(agg_type != event_agg_type)

    class DivergentEvent(DomainEvent):
        aggregate_type: str = event_agg_type

    decider_cls = _make_decider_class(agg_type, DivergentEvent)
    agg = decider_cls(aggregate_id)

    with pytest.raises(AggregateTypeMismatchError) as exc_info:
        agg.execute(EmitToTarget(target_id=aggregate_id))

    assert exc_info.value.aggregate_type == agg_type
    assert exc_info.value.event_aggregate_type == event_agg_type
    assert agg.version == 0
    assert agg.uncommitted_events == []


@given(
    aggregate_id=st.uuids(),
    agg_type=_VALID_CATEGORY,
    command_id=st.uuids(),
    correlation_id=st.uuids(),
    tenant_id=st.uuids(),
    actor_id=st.text(min_size=1, max_size=24),
)
def test_stamp_propagates_domain_command_provenance(
    aggregate_id: UUID,
    agg_type: str,
    command_id: UUID,
    correlation_id: UUID,
    tenant_id: UUID,
    actor_id: str,
) -> None:
    """DomainCommand provenance fields are stamped onto events when not explicitly set."""

    class MatchingEvent(DomainEvent):
        aggregate_type: str = agg_type

    decider_cls = _make_decider_class(agg_type, MatchingEvent)
    agg = decider_cls(aggregate_id)
    cmd = StampDomainCommand(
        target_id=aggregate_id,
        command_id=command_id,
        correlation_id=correlation_id,
        tenant_id=tenant_id,
        actor_id=actor_id,
    )

    (stamped,) = agg.execute(cmd)

    assert stamped.causation_id == command_id
    assert stamped.correlation_id == correlation_id
    assert stamped.tenant_id == tenant_id
    assert stamped.actor_id == actor_id
