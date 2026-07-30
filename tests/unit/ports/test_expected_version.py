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

    def test_unknown_kind_rejected(self) -> None:
        with pytest.raises(ValueError):
            ExpectedVersion(kind="bogus")

    def test_exact_without_version_rejected(self) -> None:
        with pytest.raises(ValueError):
            ExpectedVersion(kind="exact")

    def test_exact_with_negative_version_rejected(self) -> None:
        with pytest.raises(ValueError):
            ExpectedVersion(kind="exact", version=-1)

    @pytest.mark.parametrize("kind", ["any", "no_stream", "stream_exists"])
    def test_non_exact_kind_with_version_rejected(self, kind: str) -> None:
        with pytest.raises(ValueError):
            ExpectedVersion(kind=kind, version=1)

    def test_direct_construction_of_four_constructors_still_works(self) -> None:
        assert ExpectedVersion(kind="any").kind == "any"
        assert ExpectedVersion(kind="no_stream").kind == "no_stream"
        assert ExpectedVersion(kind="stream_exists").kind == "stream_exists"
        assert ExpectedVersion(kind="exact", version=0) == ExpectedVersion.exact(0)
