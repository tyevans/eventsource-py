"""Helper functions and row deserializer for position mapping repository.

Governed by:
- ADR-0002 (<500 lines per module)
- ADR-0007 (Hexagonal Architecture)
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from eventsource.ports.exceptions import PositionDecodeError
from eventsource.ports.migration.models import PositionMapping
from eventsource.ports.positions import Position


def _token(position: Position) -> str:
    """Render a Position as its persisted wire token."""
    return position.to_str()


def row_to_mapping(row: Sequence[Any]) -> PositionMapping:
    """Convert database row to PositionMapping instance.

    The row order matches the SELECT queries:
    (id, migration_id, source_position_token, target_position_token,
    event_id, mapped_at)

    Args:
        row: Database row tuple from SELECT query

    Returns:
        PositionMapping instance

    Raises:
        PositionDecodeError: If either token column is missing.
    """
    source_token, target_token = row[2], row[3]
    if source_token is None or target_token is None:
        raise PositionDecodeError(
            f"position mapping row {row[0]!r} has no position token "
            "(legacy int-only row is not decodable)"
        )
    return PositionMapping(
        migration_id=row[1],
        source_position=Position.from_str(source_token),
        target_position=Position.from_str(target_token),
        event_id=row[4],
        mapped_at=row[5],
    )


__all__ = ["_token", "row_to_mapping"]
