from hypothesis import settings

from eventsource.adapters.memory import MemoryEventStore
from eventsource.testing.conformance_ports.stateful import StoreStateMachine
from eventsource.testing.sync_facade import SyncStoreFacade


class MemoryStateMachine(StoreStateMachine):
    def make_store(self) -> SyncStoreFacade:
        return SyncStoreFacade(MemoryEventStore())


TestMemoryStateful = MemoryStateMachine.TestCase
# derandomize=True: pytest-randomly reseeds hypothesis's global random source
# per test, which otherwise makes this state machine's example generation
# nondeterministic across runs (project-known gotcha).
TestMemoryStateful.settings = settings(max_examples=25, deadline=None, derandomize=True)
