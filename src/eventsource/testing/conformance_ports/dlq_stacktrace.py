"""Stored stack-trace cases for the `DLQRepository` port (TASK-0470).

Mixed into `DLQRepositoryConformance`, so every binding runs them. A stored trace
keeps the frames of the handled exception and drops its message text, because
exception messages can echo event payload.
"""

from uuid import uuid4

from eventsource.ports.dlq import DLQRepository

_MARKER = "PAYLOAD-MARKER-7f3a"


def _raise_chained(marker: str) -> None:
    """Raise a RuntimeError whose cause also carries the marker in its message."""
    try:
        raise KeyError(marker)
    except KeyError as inner:
        raise RuntimeError(f"validation failed: {marker}") from inner


class DLQStackTraceConformance:
    """Stack-trace cases for `DLQRepository`. Bindings use the base suite's `store` fixture."""

    async def test_stored_stacktrace_keeps_frames_and_drops_exception_text(
        self, store: DLQRepository
    ) -> None:
        """Stored traces name the raising frame and never echo exception messages."""
        try:
            _raise_chained(_MARKER)
        except RuntimeError as exc:
            await store.add_failed_event(
                event_id=uuid4(),
                projection_name="P",
                event_type="Created",
                event_data={},
                error=exc,
            )
        (entry,) = await store.get_failed_events()
        assert entry.error_stacktrace is not None
        assert _MARKER not in entry.error_stacktrace
        assert "_raise_chained" in entry.error_stacktrace
        assert "RuntimeError" in entry.error_stacktrace

    async def test_stored_stacktrace_is_empty_outside_an_except_block(
        self, store: DLQRepository
    ) -> None:
        """With no active exception, no trace is stored."""
        await store.add_failed_event(
            event_id=uuid4(),
            projection_name="P",
            event_type="Created",
            event_data={},
            error=RuntimeError("boom"),
        )
        (entry,) = await store.get_failed_events()
        assert entry.error_stacktrace == ""
