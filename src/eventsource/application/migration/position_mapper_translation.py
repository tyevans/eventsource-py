"""
Translation result dataclasses for PositionMapper.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from eventsource.ports.positions import Position


@dataclass(frozen=True)
class TranslationResult:
    """
    Result of a position translation operation.

    Contains the translated position along with metadata about
    how the translation was performed.

    Attributes:
        source_position: The original source position.
        target_position: The translated target position.
        is_exact: Whether the translation was an exact match.
        nearest_source_position: The source position used for nearest match.
        interpolated: Whether interpolation was used.
    """

    source_position: Position
    target_position: Position
    is_exact: bool
    nearest_source_position: Position | None = None
    interpolated: bool = False


@dataclass(frozen=True)
class ReverseTranslationResult:
    """
    Result of a reverse position translation operation (target to source).

    Contains the translated position along with metadata about
    how the translation was performed.

    Attributes:
        target_position: The original target position.
        source_position: The translated source position.
        is_exact: Whether the translation was an exact match.
        nearest_target_position: The target position used for nearest match.
    """

    target_position: Position
    source_position: Position
    is_exact: bool
    nearest_target_position: Position | None = None


__all__ = ["ReverseTranslationResult", "TranslationResult"]
