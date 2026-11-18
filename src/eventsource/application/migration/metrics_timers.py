"""Phase and cutover timer context managers for MigrationMetrics."""

from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager

from eventsource.application.migration.metrics_types import _CutoverTimer, _PhaseTimer


class MigrationMetricsTimerMixin:
    """Mixin providing ``time_phase`` and ``time_cutover`` context managers."""

    def record_phase_duration(self, phase: str, duration_seconds: float) -> None:
        raise NotImplementedError  # fmt: skip

    def record_cutover_duration(self, duration_ms: float, success: bool = True) -> None:
        raise NotImplementedError  # fmt: skip

    @contextmanager
    def time_phase(self, phase: str) -> Generator[_PhaseTimer]:
        timer = _PhaseTimer()
        timer.start()
        try:
            yield timer
        finally:
            timer.stop()
            self.record_phase_duration(phase, timer.duration_seconds)

    @contextmanager
    def time_cutover(self) -> Generator[_CutoverTimer]:
        timer = _CutoverTimer()
        timer.start()
        try:
            yield timer
        finally:
            timer.stop()
            self.record_cutover_duration(timer.duration_ms, timer.success)


__all__ = ["MigrationMetricsTimerMixin"]
