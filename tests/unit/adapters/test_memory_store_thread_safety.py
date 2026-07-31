"""Regression test: MemoryEventStore must be safe under cross-loop contention.

`SyncEventStoreAdapter` runs each call in a fresh event loop (`asyncio.run`).
`MemoryEventStore` used to guard its append critical section with an
`asyncio.Lock`, which is bound to whichever loop first acquires it -- a lock
acquired from one `asyncio.run()` call raises `RuntimeError` when awaited from
a different loop on another thread. The append critical section never awaits
anything, so a plain `threading.Lock` is sufficient and loop-agnostic.
"""

from __future__ import annotations

import sys
import threading
from uuid import uuid4

import pytest

from eventsource.adapters.memory.store import MemoryEventStore
from eventsource.domain import StreamId
from eventsource.events.base import DomainEvent
from eventsource.exceptions import OptimisticLockError
from eventsource.ports import ExpectedVersion
from eventsource.sync import SyncEventStoreAdapter


class ThingHappened(DomainEvent):
    aggregate_type: str = "Thing"
    event_type: str = "ThingHappened"


class TestCrossLoopLockSafety:
    """Multiple threads, each running the sync adapter's own fresh event
    loop, contending on a single stream must never see the lock's internal
    machinery leak across loops as RuntimeError/TimeoutError -- only the
    expected OptimisticLockError from losing the race."""

    def test_concurrent_sync_appends_never_raise_loop_errors(self) -> None:
        store = MemoryEventStore()
        sync_store = SyncEventStoreAdapter(store, timeout=5.0)
        agg_id = uuid4()
        stream = StreamId(aggregate_id=agg_id, category="Thing")

        original_interval = sys.getswitchinterval()
        sys.setswitchinterval(1e-6)
        try:
            unexpected_errors: list[BaseException] = []
            errors_lock = threading.Lock()
            trials_per_thread = 15
            thread_count = 6

            def worker() -> None:
                for _ in range(trials_per_thread):
                    try:
                        version = sync_store.get_stream_version(stream)
                        event = ThingHappened(aggregate_id=agg_id)
                        sync_store.append(stream, [event], ExpectedVersion.exact(version))
                    except OptimisticLockError:
                        pass
                    except BaseException as exc:  # noqa: BLE001 -- must catch everything to assert on it
                        with errors_lock:
                            unexpected_errors.append(exc)

            threads = [threading.Thread(target=worker) for _ in range(thread_count)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

            assert not unexpected_errors, (
                "expected only OptimisticLockError from losing the race, got: "
                f"{[(type(e).__name__, str(e)) for e in unexpected_errors]}"
            )
        finally:
            sys.setswitchinterval(original_interval)

    @pytest.mark.parametrize("run", range(5))
    def test_stable_across_repeated_runs(self, run: int) -> None:
        """Same scenario, run multiple times to guard against flakiness."""
        self.test_concurrent_sync_appends_never_raise_loop_errors()
