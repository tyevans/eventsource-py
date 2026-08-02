"""Stream identity value object."""

import re
from dataclasses import dataclass
from uuid import UUID

# `\Z` (not `$`) as the end anchor: `$` matches just before a trailing
# newline, so `"Order\n"` would otherwise validate as a legal category.
CATEGORY_PATTERN = re.compile(r"^[A-Za-z0-9_.-]+\Z")


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
        # Validate the id first. The two positional arguments read in the
        # opposite order from most of the library's category-first APIs, and
        # from the rendered `"{aggregate_id}:{category}"` form, so
        # `StreamId("Order", some_uuid)` is an easy transposition. Left to
        # `CATEGORY_PATTERN.match`, it surfaces as a bare
        # `TypeError: expected string or bytes-like object` naming neither
        # argument.
        if not isinstance(self.aggregate_id, UUID):
            raise TypeError(
                f"StreamId.aggregate_id must be a UUID, got "
                f"{type(self.aggregate_id).__name__} ({self.aggregate_id!r}). "
                f"The signature is StreamId(aggregate_id, category) -- note "
                f"the id comes first, unlike the category-first read APIs."
            )
        if not isinstance(self.category, str):
            raise TypeError(
                f"StreamId.category must be a str, got "
                f"{type(self.category).__name__} ({self.category!r}). "
                f"The signature is StreamId(aggregate_id, category)."
            )
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
