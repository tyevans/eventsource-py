"""Stream identity value object."""

import re
from dataclasses import dataclass
from uuid import UUID

CATEGORY_PATTERN = re.compile(r"^[A-Za-z0-9_.-]+$")


@dataclass(frozen=True, slots=True)
class StreamId:
    """Identity of an event stream: aggregate id + category.

    ``category`` is today's aggregate type. The rendered wire format is
    ``"{aggregate_id}:{category}"`` — the delimiter is why ``:`` is banned
    from categories.
    """

    aggregate_id: UUID
    category: str

    def __post_init__(self) -> None:
        if not CATEGORY_PATTERN.match(self.category):
            raise ValueError(
                f"invalid stream category {self.category!r}: must match [A-Za-z0-9_.-]+"
            )

    def render(self) -> str:
        return f"{self.aggregate_id}:{self.category}"

    @classmethod
    def parse(cls, raw: str) -> "StreamId":
        aggregate_id, sep, category = raw.partition(":")
        if not sep:
            raise ValueError(f"not a stream id: {raw!r}")
        return cls(aggregate_id=UUID(aggregate_id), category=category)
