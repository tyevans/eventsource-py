"""Unit + property tests for the pure death-header functions."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

from hypothesis import given
from hypothesis import strategies as st

from eventsource.adapters.rabbitmq import death_headers


def _message(headers: dict[str, Any] | None) -> MagicMock:
    """Build a mock AbstractIncomingMessage-like object with the given headers."""
    message = MagicMock()
    message.headers = headers
    return message


def _headers_with_death(
    count: int,
    queue: str = "q1",
    reason: str = "rejected",
    exchange: str = "ex",
    routing_keys: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "x-death": [
            {
                "count": count,
                "queue": queue,
                "reason": reason,
                "exchange": exchange,
                "routing-keys": routing_keys or ["events.order.created"],
            }
        ],
        "x-first-death-queue": queue,
        "x-first-death-reason": reason,
        "x-first-death-exchange": exchange,
    }


# ---------------------------------------------------------------------------
# get_death_count
# ---------------------------------------------------------------------------


def test_death_count_zero_when_no_header() -> None:
    assert death_headers.get_death_count(_message({})) == 0


def test_death_count_zero_when_headers_none() -> None:
    assert death_headers.get_death_count(_message(None)) == 0


def test_death_count_reads_first_entry() -> None:
    assert death_headers.get_death_count(_message(_headers_with_death(3))) == 3


def test_death_count_sums_multiple_death_records() -> None:
    headers = {
        "x-death": [
            {"count": 3},
            {"count": 2},
        ]
    }
    assert death_headers.get_death_count(_message(headers)) == 5


def test_death_count_ignores_invalid_count_type() -> None:
    headers = {"x-death": [{"count": "not-a-number"}, {"count": 5}]}
    assert death_headers.get_death_count(_message(headers)) == 5


def test_death_count_handles_non_list_x_death() -> None:
    assert death_headers.get_death_count(_message({"x-death": "not-a-list"})) == 0


def test_death_count_handles_non_dict_entries() -> None:
    headers = {"x-death": ["not-a-dict", {"count": 2}, None]}
    assert death_headers.get_death_count(_message(headers)) == 2


@given(count=st.integers(min_value=0, max_value=10_000))
def test_death_count_roundtrips_any_count(count: int) -> None:
    assert death_headers.get_death_count(_message(_headers_with_death(count))) == count


# ---------------------------------------------------------------------------
# get_first_death_queue
# ---------------------------------------------------------------------------


def test_first_death_queue_none_when_no_header() -> None:
    assert death_headers.get_first_death_queue(_message({})) is None


def test_first_death_queue_returns_recorded_queue() -> None:
    headers = _headers_with_death(1, queue="events.default")
    assert death_headers.get_first_death_queue(_message(headers)) == "events.default"


@given(queue=st.text(min_size=1, max_size=50))
def test_first_death_queue_returns_recorded_queue_property(queue: str) -> None:
    assert (
        death_headers.get_first_death_queue(_message(_headers_with_death(1, queue=queue))) == queue
    )


# ---------------------------------------------------------------------------
# get_first_death_reason
# ---------------------------------------------------------------------------


def test_first_death_reason_none_when_no_header() -> None:
    assert death_headers.get_first_death_reason(_message({})) is None


def test_first_death_reason_returns_recorded_reason() -> None:
    headers = _headers_with_death(1, reason="expired")
    assert death_headers.get_first_death_reason(_message(headers)) == "expired"


@given(reason=st.text(min_size=1, max_size=50))
def test_first_death_reason_returns_recorded_reason_property(reason: str) -> None:
    assert (
        death_headers.get_first_death_reason(_message(_headers_with_death(1, reason=reason)))
        == reason
    )


# ---------------------------------------------------------------------------
# get_first_death_exchange
# ---------------------------------------------------------------------------


def test_first_death_exchange_none_when_no_header() -> None:
    assert death_headers.get_first_death_exchange(_message({})) is None


def test_first_death_exchange_returns_recorded_exchange() -> None:
    headers = _headers_with_death(1, exchange="events")
    assert death_headers.get_first_death_exchange(_message(headers)) == "events"


@given(exchange=st.text(min_size=1, max_size=50))
def test_first_death_exchange_returns_recorded_exchange_property(exchange: str) -> None:
    assert (
        death_headers.get_first_death_exchange(_message(_headers_with_death(1, exchange=exchange)))
        == exchange
    )


# ---------------------------------------------------------------------------
# get_original_routing_key
# ---------------------------------------------------------------------------


def test_original_routing_key_none_when_no_header() -> None:
    assert death_headers.get_original_routing_key(_message({})) is None


def test_original_routing_key_returns_first_routing_key() -> None:
    headers = _headers_with_death(1, routing_keys=["orders.created", "orders.updated"])
    assert death_headers.get_original_routing_key(_message(headers)) == "orders.created"


def test_original_routing_key_none_when_empty_routing_keys() -> None:
    headers = {"x-death": [{"routing-keys": []}]}
    assert death_headers.get_original_routing_key(_message(headers)) is None


def test_original_routing_key_none_when_missing_routing_keys_field() -> None:
    headers = {"x-death": [{"count": 1, "reason": "rejected"}]}
    assert death_headers.get_original_routing_key(_message(headers)) is None


@given(routing_key=st.text(min_size=1, max_size=50))
def test_original_routing_key_roundtrips_property(routing_key: str) -> None:
    headers = _headers_with_death(1, routing_keys=[routing_key])
    assert death_headers.get_original_routing_key(_message(headers)) == routing_key


# ---------------------------------------------------------------------------
# is_from_dlq
# ---------------------------------------------------------------------------


def test_is_from_dlq_false_without_header() -> None:
    assert death_headers.is_from_dlq(_message({})) is False


def test_is_from_dlq_false_when_headers_none() -> None:
    assert death_headers.is_from_dlq(_message(None)) is False


def test_is_from_dlq_false_when_empty_x_death_list() -> None:
    assert death_headers.is_from_dlq(_message({"x-death": []})) is False


def test_is_from_dlq_true_with_death_record() -> None:
    assert death_headers.is_from_dlq(_message(_headers_with_death(1))) is True


# ---------------------------------------------------------------------------
# get_death_info
# ---------------------------------------------------------------------------


def test_death_info_defaults_when_no_death() -> None:
    info = death_headers.get_death_info(_message({}))

    assert info["is_dead_lettered"] is False
    assert info["death_count"] == 0
    assert info["first_death_queue"] is None
    assert info["first_death_reason"] is None
    assert info["first_death_exchange"] is None
    assert info["original_routing_key"] is None
    assert info["x_death"] is None


def test_death_info_populated_when_dead_lettered() -> None:
    headers = _headers_with_death(2, queue="events.default", reason="rejected", exchange="events")
    info = death_headers.get_death_info(_message(headers))

    assert info["is_dead_lettered"] is True
    assert info["death_count"] == 2
    assert info["first_death_queue"] == "events.default"
    assert info["first_death_reason"] == "rejected"
    assert info["first_death_exchange"] == "events"
    assert info["original_routing_key"] == "events.order.created"
    assert info["x_death"] == headers["x-death"]


@given(count=st.integers(min_value=0, max_value=10_000))
def test_death_info_death_count_matches_get_death_count_property(count: int) -> None:
    headers = _headers_with_death(count)
    info = death_headers.get_death_info(_message(headers))
    assert info["death_count"] == death_headers.get_death_count(_message(headers))
