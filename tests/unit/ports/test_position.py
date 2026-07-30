import pytest
from hypothesis import given
from hypothesis import strategies as st

from eventsource.exceptions import PositionDecodeError, PositionForeignError
from eventsource.ports import Position

keys = st.lists(
    st.one_of(st.integers(min_value=0, max_value=2**62), st.text(max_size=32)),
    min_size=1,
    max_size=3,
).map(tuple)

int_keys = st.lists(st.integers(min_value=0, max_value=2**62), min_size=1, max_size=3).map(tuple)
str_keys = st.lists(st.text(max_size=32), min_size=1, max_size=3).map(tuple)


class TestPositionOrdering:
    def test_same_store_orders_by_key(self) -> None:
        assert Position("pg:a", (1,)) < Position("pg:a", (2,))

    def test_foreign_store_ordering_raises(self) -> None:
        with pytest.raises(PositionForeignError):
            _ = Position("pg:a", (1,)) < Position("pg:b", (1,))

    def test_foreign_store_equality_is_false_not_error(self) -> None:
        assert Position("pg:a", (1,)) != Position("pg:b", (1,))

    def test_equality_with_none_and_other_types(self) -> None:
        assert Position("pg:a", (1,)) != None  # noqa: E711
        assert Position("pg:a", (1,)) != 1
        assert Position("pg:a", (1,)) in {Position("pg:a", (1,))}

    @given(data=st.data())
    def test_ordering_laws_within_store(self, data: st.DataObject) -> None:
        key_strategy = data.draw(st.sampled_from([int_keys, str_keys]))
        pa, pb, pc = (Position("s", data.draw(key_strategy)) for _ in range(3))
        assert (pa < pb) == (not (pb < pa or pa == pb))  # trichotomy-ish
        if pa < pb and pb < pc:
            assert pa < pc  # transitivity


class TestPositionSerialization:
    @given(store_id=st.text(min_size=1, max_size=32), key=keys)
    def test_round_trip(self, store_id: str, key) -> None:
        p = Position(store_id, key)
        assert Position.from_str(p.to_str()) == p

    @given(garbage=st.text(max_size=64))
    def test_from_str_garbage_raises_decode_error(self, garbage: str) -> None:
        try:
            p = Position.from_str(garbage)
        except PositionDecodeError:
            return
        # If it decoded, it must round-trip (accidentally-valid JSON is ok)
        assert Position.from_str(p.to_str()) == p

    def test_bare_int_is_not_valid_here(self) -> None:
        # Legacy bare-int checkpoints are decoded by the SQL codec (Task 6),
        # never by Position.from_str itself (no store_id to attach).
        with pytest.raises(PositionDecodeError):
            Position.from_str("12345")

    def test_bool_key_elements_rejected(self) -> None:
        # bool is an int subclass; a key of [true] must not decode as (True,)
        with pytest.raises(PositionDecodeError):
            Position.from_str('{"s":"x","k":[true]}')
