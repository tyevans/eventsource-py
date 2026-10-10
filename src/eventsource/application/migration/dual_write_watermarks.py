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
        """Events successfully mirrored to the target since construction.

        Counts EVENTS, not append calls. STATISTICS ONLY -- it must never
        be subtracted from a lag count. A bare success count cannot tell
        "five mirrored" from "five mirrored after three were dropped", so
        subtracting it can report zero lag over a hole. Use
        `safe_lag_anchor` instead, which stops at the first failure.
        """
        return self._dual_write_success_count

    @property
    def first_seen_source_position(self) -> Position | None:
        """Source position of the FIRST append this interceptor handled.

        Set once, on the first append, whether or not the mirror
        succeeded -- it marks where this interceptor's coverage starts,
        not where it worked. Writes that landed before it were mirrored
        by nobody.
        """
        return self._first_seen_source_position

    @property
    def last_synced_source_position(self) -> Position | None:
        """Source position of the most recent successful mirror.

        The first-of-batch position, which is a CONSERVATIVE (never
        optimistic) watermark -- not a monotone one: concurrent mirrors
        can complete out of order, so this can move backward. That is
        safe because every clamp errs toward counting more lag, so a
        watermark that lags reality only ever refuses a cutover that
        would have been allowed, never the reverse. For a multi-event
        append the batch's remaining events sit after this position and
        keep counting as lag until the next successful mirror moves past
        them.
        """
        return self._last_synced_source_position

    @property
    def first_failed_source_position(self) -> Position | None:
        """Earliest source position of an unabsorbed mirror failure.

        A later mirror SUCCESS never clears or advances this -- it does
        not retroactively deliver the event that was dropped. The only
        release is `mark_copy_pass_complete`: a completed bulk-copy pass
        whose checkpoint reaches the failure proves the copier
        re-delivered the event, which absorbs it. This is what stops
        `safe_lag_anchor` from advancing over a hole.
        """
        if not self._unabsorbed_failure_positions:
            return None
        return min(self._unabsorbed_failure_positions)

    def safe_lag_anchor(self, checkpoint: Position | None) -> Position | None:
        """The furthest source position provably present in the target.

        Counting lag from here is safe: every source event at or before
        the returned position is known to be in the target.

        Starts from `checkpoint` (the migration's `last_source_position`,
        i.e. what the bulk copy proved) and advances to the synced
        watermark only when BOTH clamps pass. The anchor never moves
        backward.

        Clamp 1 -- the install window. The copier stops at `checkpoint`
        and this interceptor's coverage starts at
        `first_seen_source_position`; anything in between was mirrored by
        nobody. The anchor may advance only when the checkpoint has
        reached the start of coverage, which is the only way the
        interceptor ALONE can know no such gap exists. The clamp is
        released for good by `mark_copy_pass_complete`, the coordinator's
        attestation that a copy pass beginning after installation
        completed -- installed-before-copy means the window is empty by
        construction. Fail-closed: without that attestation, when the two
        watermarks cannot be shown to meet the anchor stays put even if
        there happened to be no events in the window.

        Clamp 2 -- the failure. If mirroring has failed and no completed
        copy pass has absorbed the failure (see
        `mark_copy_pass_complete`), the anchor advances only when the
        whole synced run precedes that first unabsorbed failure.

        CONSEQUENCE: events stranded behind an unabsorbed failure block
        cutover until a copy pass absorbs them, advancing the checkpoint
        past them. Re-copying is safe -- the copier treats an event
        already in the target as already-copied and continues -- so the
        accepted failure mode here is stuck-until-recopied, never a
        cutover over missing data.

        Args:
            checkpoint: Last source position the bulk copy proved copied.

        Returns:
            The anchor to pass as `SyncLagTracker.calculate_lag(since=...)`.
        """
        candidate = self._last_synced_source_position
        if candidate is None:
            return checkpoint

        # Fail-closed under saturation: failure positions were dropped,
        # so no advancement can ever be shown safe again.
        if self._failure_positions_saturated:
            return checkpoint

        # Clamp 1: the checkpoint must have reached this interceptor's
        # coverage, or events between them were mirrored by nobody.
        # A completed covered copy pass proves the window empty instead.
        if not self._coverage_complete:
            first_seen = self._first_seen_source_position
            if first_seen is None:
                return checkpoint
            if checkpoint is None:
                # Nothing was copied, so everything before coverage is a gap.
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
        """Record that a covered bulk-copy pass completed at `checkpoint`.

        MUST only be called by the migration coordinator, and only for a
        copy pass that BEGAN AFTER this interceptor was installed and ran
        to completion -- both are ordering facts the interceptor cannot
        observe on its own. Two proofs follow from them:

        - The install window is empty. The pass's feed snapshot contains
          every event predating this interceptor's coverage, so no event
          was mirrored by nobody: the install-window clamp in
          `safe_lag_anchor` is released permanently.
        - Failures at or before `checkpoint` are absorbed. The copier
          verified every feed event through its checkpoint (appended, or
          confirmed already present), so a mirror that dropped one of
          those events was re-delivered by the copy. Later failures stay:
          the checkpoint proves nothing about them.

        Args:
            checkpoint: The completed pass's final source checkpoint
                (`Migration.last_source_position`); None when the tenant
                had no events to copy, which still proves the window
                empty but absorbs nothing.

        Returns:
            The number of unabsorbed mirror failures remaining. Non-zero
            means `safe_lag_anchor` stays clamped at the checkpoint until
            another completed pass absorbs them.
        """
        self._coverage_complete = True
        if checkpoint is not None:
            self._unabsorbed_failure_positions = [
                p for p in self._unabsorbed_failure_positions if p > checkpoint
            ]
        remaining = len(self._unabsorbed_failure_positions)
        if self._failure_positions_saturated:
            return remaining + 1
        return remaining


__all__ = [
    "DualWriteWatermarksMixin",
]
