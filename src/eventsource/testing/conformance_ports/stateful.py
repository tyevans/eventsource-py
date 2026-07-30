"""Hypothesis stateful conformance machine for `FullEventStore`-shaped adapters.

`StoreStateMachine` drives an adapter (via `SyncStoreFacade`) through
randomized sequences of appends and checks two invariants after every
step: each stream's stored events match an in-memory model, and the
global feed's event order matches the flattened append order with
strictly increasing positions.

Concrete backends subclass `StoreStateMachine` and implement
`make_store()`. See `tests/unit/adapters/test_memory_stateful.py` for
the canonical usage.

Kept sqlalchemy-free: only `ports`, `domain`, `events`, `exceptions`,
this package's `_fixtures`, stdlib, and hypothesis are imported here.
"""

from abc import abstractmethod
from uuid import UUID, uuid4

from hypothesis import strategies as st
from hypothesis.stateful import RuleBasedStateMachine, invariant, precondition, rule

from eventsource.domain import StreamId
from eventsource.exceptions import OptimisticLockError
from eventsource.ports import ExpectedVersion
from eventsource.testing.conformance_ports._fixtures import make_event
from eventsource.testing.sync_facade import SyncStoreFacade

MAX_STREAMS = 8
MAX_EVENTS_PER_APPEND = 5


class StoreStateMachine(RuleBasedStateMachine):
    """Rule-based stateful conformance machine for event store ports.

    Subclasses provide `make_store()` returning a `SyncStoreFacade`
    wrapping the backend under test.
    """

    def __init__(self) -> None:
        super().__init__()
        self.facade: SyncStoreFacade = self.make_store()
        # model: stream -> list of event ids, in append order
        self.model: dict[StreamId, list[UUID]] = {}
        # flattened order of all event ids ever appended, in append order
        self.appended: list[UUID] = []

    @abstractmethod
    def make_store(self) -> SyncStoreFacade:
        """Return a fresh `SyncStoreFacade` wrapping the backend under test."""
        raise NotImplementedError

    def _known_streams(self) -> list[StreamId]:
        return list(self.model.keys())

    @rule(n_events=st.integers(min_value=1, max_value=MAX_EVENTS_PER_APPEND))
    def append_new_stream(self, n_events: int) -> None:
        """Append to a brand-new stream, bounded to MAX_STREAMS total streams."""
        if len(self.model) >= MAX_STREAMS:
            return
        stream = StreamId(aggregate_id=uuid4(), category="Conformance")
        events = [make_event(stream.aggregate_id) for _ in range(n_events)]
        self.facade.append(stream, events, ExpectedVersion.no_stream())
        event_ids = [e.event_id for e in events]
        self.model[stream] = event_ids
        self.appended.extend(event_ids)

    @precondition(lambda self: bool(self.model))
    @rule(
        data=st.data(),
        n_events=st.integers(min_value=1, max_value=MAX_EVENTS_PER_APPEND),
    )
    def append_existing(self, data: st.DataObject, n_events: int) -> None:
        """Append more events to an existing stream, with the correct exact version."""
        stream = data.draw(st.sampled_from(self._known_streams()), label="stream")
        current_version = len(self.model[stream])
        events = [make_event(stream.aggregate_id) for _ in range(n_events)]
        self.facade.append(stream, events, ExpectedVersion.exact(current_version))
        event_ids = [e.event_id for e in events]
        self.model[stream].extend(event_ids)
        self.appended.extend(event_ids)

    @precondition(lambda self: any(len(v) >= 1 for v in self.model.values()))
    @rule(data=st.data())
    def append_stale(self, data: st.DataObject) -> None:
        """Append with a deliberately stale expected version; must raise and not mutate."""
        candidates = [s for s, v in self.model.items() if len(v) >= 1]
        stream = data.draw(st.sampled_from(candidates), label="stream")
        current_version = len(self.model[stream])
        event = make_event(stream.aggregate_id)
        try:
            self.facade.append(stream, [event], ExpectedVersion.exact(current_version - 1))
        except OptimisticLockError:
            pass
        else:
            raise AssertionError(f"expected OptimisticLockError for stale append on {stream!r}")
        # model unchanged

    @precondition(lambda self: bool(self.model))
    @rule(data=st.data())
    def check_stream(self, data: st.DataObject) -> None:
        """Invariant: a single stream's stored events match the model."""
        stream = data.draw(st.sampled_from(self._known_streams()), label="stream")
        envelopes = self.facade.read_stream(stream)
        actual_ids = [e.event.event_id for e in envelopes]
        assert actual_ids == self.model[stream]
        assert self.facade.get_stream_version(stream) == len(self.model[stream])

    @invariant()
    def check_feed(self) -> None:
        """Invariant: the global feed matches flattened append order, positions increase."""
        envelopes = self.facade.read_all()
        actual_ids = [e.event.event_id for e in envelopes]
        assert actual_ids == self.appended

        positions = [e.position for e in envelopes if e.position is not None]
        for prev, curr in zip(positions, positions[1:], strict=False):
            assert prev < curr

    def teardown(self) -> None:
        self.facade.close()


__all__ = ["StoreStateMachine"]
