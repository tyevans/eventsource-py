from dataclasses import FrozenInstanceError
from uuid import uuid4

import pytest
from hypothesis import given
from hypothesis import strategies as st

from eventsource.domain import StreamId

CATEGORY_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_.-"


class TestStreamId:
    def test_render_matches_legacy_wire_format(self) -> None:
        aid = uuid4()
        assert StreamId(aggregate_id=aid, category="Order").render() == f"{aid}:Order"

    def test_parse_round_trip(self) -> None:
        sid = StreamId(aggregate_id=uuid4(), category="Order.v2")
        assert StreamId.parse(sid.render()) == sid

    def test_category_with_colon_rejected(self) -> None:
        with pytest.raises(ValueError):
            StreamId(aggregate_id=uuid4(), category="Order:evil")

    def test_empty_category_rejected(self) -> None:
        with pytest.raises(ValueError):
            StreamId(aggregate_id=uuid4(), category="")

    def test_category_with_trailing_newline_rejected(self) -> None:
        """`$` matches just before a trailing newline; the pattern must use `\\Z`."""
        with pytest.raises(ValueError):
            StreamId(aggregate_id=uuid4(), category="Order\n")

    def test_frozen(self) -> None:
        sid = StreamId(aggregate_id=uuid4(), category="Order")
        with pytest.raises(FrozenInstanceError):
            sid.category = "Other"  # type: ignore[misc]

    @given(category=st.text(alphabet=CATEGORY_ALPHABET, min_size=1, max_size=64))
    def test_valid_categories_round_trip(self, category: str) -> None:
        sid = StreamId(aggregate_id=uuid4(), category=category)
        assert StreamId.parse(sid.render()) == sid

    @given(category=st.text(min_size=1, max_size=64))
    def test_fuzzed_categories_never_corrupt_wire_format(self, category: str) -> None:
        try:
            sid = StreamId(aggregate_id=uuid4(), category=category)
        except ValueError:
            return  # rejected is fine; corrupted is not
        assert StreamId.parse(sid.render()) == sid
