import pytest

from eventsource.adapters._sql.positions import IntPositionCodec
from eventsource.exceptions import PositionDecodeError, PositionForeignError
from eventsource.ports.positions import Position


class TestIntPositionCodec:
    def test_encode_decode_round_trip(self) -> None:
        codec = IntPositionCodec(store_id="pg:main")
        pos = codec.encode(42)
        assert codec.decode(pos.to_str()) == pos
        assert codec.value_of(pos) == 42

    def test_legacy_bare_int_checkpoint_decodes(self) -> None:
        codec = IntPositionCodec(store_id="pg:main")
        assert codec.decode("12345") == Position("pg:main", (12345,))

    def test_foreign_store_decode_raises(self) -> None:
        codec = IntPositionCodec(store_id="pg:main")
        foreign = Position("sqlite:other", (1,)).to_str()
        with pytest.raises(PositionForeignError):
            codec.decode(foreign)

    def test_garbage_raises_decode_error(self) -> None:
        with pytest.raises(PositionDecodeError):
            IntPositionCodec(store_id="pg:main").decode("not-a-position")

    def test_dialect_reexport_intact(self) -> None:
        from eventsource.adapters._sql.dialect import Dialect as NewDialect
        from eventsource.repositories._dialect import Dialect  # old path

        assert Dialect is NewDialect
