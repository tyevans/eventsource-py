"""Int-backed position codec shared by the SQL-family adapters."""

from dataclasses import dataclass

from eventsource.ports.exceptions import PositionDecodeError, PositionForeignError
from eventsource.ports.positions import Position


@dataclass(frozen=True, slots=True)
class IntPositionCodec:
    store_id: str

    def encode(self, value: int) -> Position:
        return Position(store_id=self.store_id, key=(value,))

    def decode(self, raw: str) -> Position:
        if raw.isdigit():  # legacy bare-int checkpoint value
            return self.encode(int(raw))
        position = Position.from_str(raw)
        if position.store_id != self.store_id:
            raise PositionForeignError(
                f"position belongs to {position.store_id!r}, this store is {self.store_id!r}"
            )
        return position

    def value_of(self, position: Position) -> int:
        if position.store_id != self.store_id:
            raise PositionForeignError(
                f"position belongs to {position.store_id!r}, this store is {self.store_id!r}"
            )
        if len(position.key) != 1 or not isinstance(position.key[0], int):
            raise PositionDecodeError(f"not an int-backed position: {position!r}")
        return position.key[0]
