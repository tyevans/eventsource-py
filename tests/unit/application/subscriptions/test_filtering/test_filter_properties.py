"""Property-based and fast-path tests for EventFilter and FilterStats."""

from __future__ import annotations

import fnmatch
from datetime import UTC, datetime
from uuid import uuid4

from hypothesis import given
from hypothesis import strategies as st

from eventsource.application.subscriptions.filtering import (
    EventFilter,
    FilterStats,
    matches_pattern,
)
from eventsource.domain import StreamId
from eventsource.domain.event import DomainEvent
from eventsource.ports.envelopes import EventEnvelope
from eventsource.ports.positions import Position
from tests.unit.application.subscriptions.test_filtering._fixtures import (
    OrderCancelled,
    OrderCreated,
    OrderShipped,
    PaymentReceived,
    UserRegistered,
    UserUpdatedEvent,
)

_EVENT_CLASSES = (
    OrderCreated,
    OrderShipped,
    OrderCancelled,
    PaymentReceived,
    UserRegistered,
    UserUpdatedEvent,
)

_PATTERN_ALPHABET = "OrderShippedCancelledPaymentReceivedUserRegisteredEvent*?[]"


def _make_envelope(event_cls: type[DomainEvent]) -> EventEnvelope:
    event = event_cls(aggregate_id=uuid4())
    return EventEnvelope(
        event=event,
        stream_id=StreamId(aggregate_id=event.aggregate_id, category=event.aggregate_type),
        position=Position(store_id="memory", key=(1,)),
        stream_version=1,
        stored_at=datetime.now(UTC),
    )


class TestEventFilterFastPaths:
    """Tests for EventFilter pre-compiled pattern and pass-through fast-paths."""

    def test_separates_exact_and_wildcard_patterns(self) -> None:
        """Exact patterns go to _exact_patterns; wildcards go to _compiled_patterns."""
        event_filter = EventFilter(
            event_type_patterns=("OrderCreated", "Order*", "Payment?eceived", "User[RU]*")
        )

        assert event_filter._exact_patterns == frozenset({"OrderCreated"})
        assert len(event_filter._compiled_patterns) == 3
        assert event_filter._has_any_filter is True

    def test_exact_pattern_only_leaves_compiled_patterns_empty(self) -> None:
        """When only exact string patterns are configured, no regexes are compiled."""
        event_filter = EventFilter(event_type_patterns=("OrderCreated", "PaymentReceived"))

        assert event_filter._exact_patterns == frozenset({"OrderCreated", "PaymentReceived"})
        assert event_filter._compiled_patterns == ()
        assert event_filter.matches(OrderCreated(aggregate_id=uuid4())) is True
        assert event_filter.matches(OrderShipped(aggregate_id=uuid4())) is False

    def test_matches_envelope_fast_path_and_filtered(self) -> None:
        """matches_envelope fast-paths unconfigured filters and delegates when configured."""
        pass_through = EventFilter()
        envelope_order = _make_envelope(OrderCreated)
        envelope_payment = _make_envelope(PaymentReceived)

        assert pass_through._has_any_filter is False
        assert pass_through.matches_envelope(envelope_order) is True
        assert pass_through.matches_envelope(envelope_payment) is True
        assert pass_through.stats.events_evaluated == 2
        assert pass_through.stats.events_matched == 2

        filtered = EventFilter(event_types=(OrderCreated,))
        assert filtered._has_any_filter is True
        assert filtered.matches_envelope(envelope_order) is True
        assert filtered.matches_envelope(envelope_payment) is False
        assert filtered.stats.events_evaluated == 2
        assert filtered.stats.events_matched == 1
        assert filtered.stats.events_skipped == 1


class TestEventFilterProperties:
    """Hypothesis property-based tests for EventFilter invariants."""

    @given(
        patterns=st.lists(
            st.sampled_from(
                [
                    "OrderCreated",
                    "OrderShipped",
                    "Order*",
                    "*Created",
                    "*Event",
                    "Payment?eceived",
                    "*",
                    "NonExistent",
                ]
            ),
            min_size=0,
            max_size=5,
        ),
        event_indices=st.lists(st.integers(min_value=0, max_value=len(_EVENT_CLASSES) - 1)),
    )
    def test_precompiled_pattern_matching_matches_fnmatch_oracle(
        self,
        patterns: list[str],
        event_indices: list[int],
    ) -> None:
        """Pre-compiled exact + regex pattern matching is equivalent to fnmatch."""
        pattern_tuple = tuple(patterns)
        event_filter = EventFilter(event_type_patterns=pattern_tuple)

        expected_matches = 0
        expected_skips = 0

        for idx in event_indices:
            event_cls = _EVENT_CLASSES[idx]
            event = event_cls(aggregate_id=uuid4())
            oracle = any(fnmatch.fnmatch(event.event_type, p) for p in pattern_tuple)
            actual = event_filter.matches(event)
            assert actual is oracle
            if oracle:
                expected_matches += 1
            else:
                expected_skips += 1

        assert event_filter.stats.events_evaluated == len(event_indices)
        assert event_filter.stats.events_matched == expected_matches
        assert event_filter.stats.events_skipped == expected_skips

    @given(
        outcomes=st.lists(st.booleans(), min_size=0, max_size=100),
    )
    def test_filter_stats_invariants(self, outcomes: list[bool]) -> None:
        """FilterStats maintains accounting invariants across arbitrary match/skip sequences."""
        stats = FilterStats()
        for matched in outcomes:
            if matched:
                stats.record_match()
            else:
                stats.record_skip()

        assert stats.events_evaluated == len(outcomes)
        assert stats.events_matched + stats.events_skipped == stats.events_evaluated
        assert 0.0 <= stats.match_rate <= 1.0
        if not outcomes:
            assert stats.match_rate == 1.0

    @given(
        name=st.text(alphabet=_PATTERN_ALPHABET, min_size=1, max_size=24),
        pattern=st.sampled_from(["OrderCreated", "Order*", "*Event", "?", "*", "PaymentReceived"]),
    )
    def test_matches_pattern_helper_equivalence(self, name: str, pattern: str) -> None:
        """matches_pattern helper agrees with fnmatch.fnmatch across exact and wildcard inputs."""
        assert matches_pattern(name, pattern) == fnmatch.fnmatch(name, pattern)
