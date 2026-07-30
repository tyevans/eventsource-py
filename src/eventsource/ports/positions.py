"""Position and ExpectedVersion value objects for the store ports."""

import json
from dataclasses import dataclass
from typing import Any

from eventsource.exceptions import PositionDecodeError, PositionForeignError


@dataclass(frozen=True, slots=True)
class Position:
    """Opaque, ordered, serializable global-feed position token.

    Totally ordered within one store; ordering across stores raises
    PositionForeignError; equality across stores is False. Consumers
    compare and persist — never arithmetic. Key element types are uniform
    within one store (the adapter defines the shape); ordering between
    differently-shaped keys is undefined.
    """

    store_id: str
    key: tuple[int | str, ...]

    def _check_comparable(self, other: "Position") -> None:
        if self.store_id != other.store_id:
            raise PositionForeignError(
                f"cannot order positions from {self.store_id!r} and {other.store_id!r}"
            )

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, Position):
            return NotImplemented
        self._check_comparable(other)
        return self.key < other.key

    def __le__(self, other: object) -> bool:
        if not isinstance(other, Position):
            return NotImplemented
        self._check_comparable(other)
        return self.key <= other.key

    def __gt__(self, other: object) -> bool:
        if not isinstance(other, Position):
            return NotImplemented
        self._check_comparable(other)
        return self.key > other.key

    def __ge__(self, other: object) -> bool:
        if not isinstance(other, Position):
            return NotImplemented
        self._check_comparable(other)
        return self.key >= other.key

    def to_str(self) -> str:
        return json.dumps({"s": self.store_id, "k": list(self.key)}, separators=(",", ":"))

    @classmethod
    def from_str(cls, raw: str) -> "Position":
        try:
            data: Any = json.loads(raw)
            store_id = data["s"]
            key = data["k"]
            if not isinstance(store_id, str) or not isinstance(key, list):
                raise TypeError
            if not all(isinstance(e, (int, str)) and not isinstance(e, bool) for e in key):
                raise TypeError
        except (json.JSONDecodeError, TypeError, KeyError) as exc:
            raise PositionDecodeError(f"not a position: {raw!r}") from exc
        return cls(store_id=store_id, key=tuple(key))
