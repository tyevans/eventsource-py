"""Watermark and sync lag anchor calculation mixin for DualWriteInterceptor."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from eventsource.ports import Position


class DualWriteWatermarksMixin:
    """Mixin providing watermarks, safe lag anchor calculation, and copy pass attestation."""

    _dual_write_success_count: int
    _first_seen_source_position: Position | None
    _last_synced_source_position: Position | None
    _unabsorbed_failure_positions: list[Position]
    _failure_positions_saturated: bool
    _coverage_complete: bool

    @property
    def dual_write_success_count(self) -> int:
        """Events successfully mirrored to the target since construction (statistics only)."""
        return self._dual_write_success_count

    @property
    def first_seen_source_position(self) -> Position | None:
        """Source position of the first append this interceptor handled."""
        return self._first_seen_source_position

    @property
    def last_synced_source_position(self) -> Position | None:
        """Source position of the most recent successful mirror (conservative watermark)."""
        return self._last_synced_source_position

    @property
    def first_failed_source_position(self) -> Position | None:
        """Earliest source position of an unabsorbed mirror failure."""
        if not self._unabsorbed_failure_positions:
            return None
        return min(self._unabsorbed_failure_positions)

    def safe_lag_anchor(self, checkpoint: Position | None) -> Position | None:
        """Return the furthest source position provably present in the target."""
        candidate = self._last_synced_source_position
        if candidate is None or self._failure_positions_saturated:
            return checkpoint

        # Clamp 1: checkpoint must reach coverage unless a covered copy pass completed.
        if not self._coverage_complete:
            first_seen = self._first_seen_source_position
            if first_seen is None:
                return checkpoint
            if checkpoint is None:
                return None
            assert checkpoint.store_id == first_seen.store_id
            if checkpoint < first_seen:
                return checkpoint

        # Clamp 2: successes after a failure prove nothing about the hole.
        first_failed = self.first_failed_source_position
        if first_failed is not None:
            assert candidate.store_id == first_failed.store_id
            if candidate >= first_failed:
                return checkpoint

        if checkpoint is not None:
            assert candidate.store_id == checkpoint.store_id
            if candidate <= checkpoint:
                return checkpoint

        return candidate

    def mark_copy_pass_complete(self, checkpoint: Position | None) -> int:
        """Record that a covered bulk-copy pass completed at `checkpoint`."""
        self._coverage_complete = True
        if checkpoint is not None:
            self._unabsorbed_failure_positions = [
                p for p in self._unabsorbed_failure_positions if p > checkpoint
            ]
        remaining = len(self._unabsorbed_failure_positions)
        return remaining + 1 if self._failure_positions_saturated else remaining


__all__ = ["DualWriteWatermarksMixin"]
