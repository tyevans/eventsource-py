from dataclasses import FrozenInstanceError

import pytest

from eventsource.ports import ExpectedVersion


class TestExpectedVersion:
    def test_four_modes(self) -> None:
        assert ExpectedVersion.any_().kind == "any"
        assert ExpectedVersion.no_stream().kind == "no_stream"
        assert ExpectedVersion.stream_exists().kind == "stream_exists"
        ev = ExpectedVersion.exact(3)
        assert (ev.kind, ev.version) == ("exact", 3)

    def test_exact_negative_rejected(self) -> None:
        with pytest.raises(ValueError):
            ExpectedVersion.exact(-1)

    def test_exact_zero_is_legal(self) -> None:
        ev = ExpectedVersion.exact(0)
        assert (ev.kind, ev.version) == ("exact", 0)

    def test_equality(self) -> None:
        assert ExpectedVersion.exact(2) == ExpectedVersion.exact(2)
        assert ExpectedVersion.any_() != ExpectedVersion.no_stream()

    def test_frozen(self) -> None:
        with pytest.raises(FrozenInstanceError):
            ExpectedVersion.any_().kind = "exact"  # type: ignore[misc]
