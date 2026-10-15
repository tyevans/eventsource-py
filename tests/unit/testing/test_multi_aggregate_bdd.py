"""Unit tests for multi-aggregate support in BDD test helpers."""

from __future__ import annotations

from uuid import uuid4

import pytest
from pydantic import BaseModel

from eventsource.domain import StreamId
from eventsource.domain.aggregate import AggregateRoot
from eventsource.domain.event import DomainEvent
from eventsource.domain.event_registry import register_event
from eventsource.ports import ExpectedVersion
from eventsource.testing import InMemoryTestHarness
from eventsource.testing.bdd import given_events, then_event_published, when_command


@register_event
class AccountCreated(DomainEvent):
    aggregate_type: str = "Account"
    owner: str


@register_event
class MoneyDeposited(DomainEvent):
    aggregate_type: str = "Account"
    amount: float


class AccountState(BaseModel):
    owner: str = ""
    balance: float = 0.0


class AccountAggregate(AggregateRoot[AccountState]):
    aggregate_type: str = "Account"

    def _get_initial_state(self) -> AccountState:
        return AccountState()

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, AccountCreated):
            self._state = AccountState(owner=event.owner, balance=0.0)
        elif isinstance(event, MoneyDeposited) and self._state:
            self._state = self._state.model_copy(
                update={"balance": self._state.balance + event.amount}
            )

    def create(self, owner: str) -> None:
        self._raise_event(
            AccountCreated(
                aggregate_id=self.aggregate_id,
                aggregate_type=self.aggregate_type,
                aggregate_version=self.get_next_version(),
                owner=owner,
            )
        )

    def deposit(self, amount: float) -> None:
        self._raise_event(
            MoneyDeposited(
                aggregate_id=self.aggregate_id,
                aggregate_type=self.aggregate_type,
                aggregate_version=self.get_next_version(),
                amount=amount,
            )
        )


@pytest.fixture
def harness() -> InMemoryTestHarness:
    return InMemoryTestHarness()


async def _read_stream(harness: InMemoryTestHarness, agg_id):
    stream_id = StreamId(aggregate_id=agg_id, category="Account")
    return [e async for e in harness.event_store.read_stream(stream_id)]


class TestMultiAggregateGivenEvents:
    async def test_given_events_accepts_single_event(self, harness: InMemoryTestHarness) -> None:
        agg_id = uuid4()
        event = AccountCreated(
            aggregate_id=agg_id, aggregate_type="Account", aggregate_version=1, owner="alice"
        )
        await given_events(harness, event)

        envelopes = await _read_stream(harness, agg_id)
        assert len(envelopes) == 1
        assert envelopes[0].event.owner == "alice"

    async def test_given_events_heterogeneous_aggregate_ids(
        self, harness: InMemoryTestHarness
    ) -> None:
        id_1, id_2 = uuid4(), uuid4()
        events = [
            AccountCreated(
                aggregate_id=id_1, aggregate_type="Account", aggregate_version=1, owner="alice"
            ),
            AccountCreated(
                aggregate_id=id_2, aggregate_type="Account", aggregate_version=1, owner="bob"
            ),
            MoneyDeposited(
                aggregate_id=id_1, aggregate_type="Account", aggregate_version=2, amount=50.0
            ),
            MoneyDeposited(
                aggregate_id=id_2, aggregate_type="Account", aggregate_version=2, amount=100.0
            ),
        ]
        await given_events(harness, events)

        stream1 = await _read_stream(harness, id_1)
        stream2 = await _read_stream(harness, id_2)
        assert len(stream1) == 2
        assert stream1[0].event.owner == "alice" and stream1[1].event.amount == 50.0
        assert len(stream2) == 2
        assert stream2[0].event.owner == "bob" and stream2[1].event.amount == 100.0

    async def test_given_events_multi_step_incremental_seeding(
        self, harness: InMemoryTestHarness
    ) -> None:
        id_1, id_2 = uuid4(), uuid4()
        await given_events(
            harness,
            [
                AccountCreated(
                    aggregate_id=id_1, aggregate_type="Account", aggregate_version=1, owner="alice"
                ),
                AccountCreated(
                    aggregate_id=id_2, aggregate_type="Account", aggregate_version=1, owner="bob"
                ),
            ],
        )
        await given_events(
            harness,
            [
                MoneyDeposited(
                    aggregate_id=id_1, aggregate_type="Account", aggregate_version=2, amount=75.0
                ),
                MoneyDeposited(
                    aggregate_id=id_2, aggregate_type="Account", aggregate_version=2, amount=25.0
                ),
            ],
        )

        stream1 = await _read_stream(harness, id_1)
        assert len(stream1) == 2 and stream1[1].event.amount == 75.0

    async def test_given_events_with_explicit_expected_version(
        self, harness: InMemoryTestHarness
    ) -> None:
        id_1 = uuid4()
        await given_events(
            harness,
            AccountCreated(
                aggregate_id=id_1, aggregate_type="Account", aggregate_version=1, owner="alice"
            ),
            expected_version=ExpectedVersion.no_stream(),
        )
        assert len(await _read_stream(harness, id_1)) == 1

    async def test_multi_aggregate_command_evaluation(self, harness: InMemoryTestHarness) -> None:
        id_1, id_2 = uuid4(), uuid4()
        await given_events(
            harness,
            [
                AccountCreated(
                    aggregate_id=id_1, aggregate_type="Account", aggregate_version=1, owner="alice"
                ),
                AccountCreated(
                    aggregate_id=id_2, aggregate_type="Account", aggregate_version=1, owner="bob"
                ),
            ],
        )

        agg1 = AccountAggregate(id_1)
        agg1.load_from_history([e.event for e in await _read_stream(harness, id_1)])
        agg2 = AccountAggregate(id_2)
        agg2.load_from_history([e.event for e in await _read_stream(harness, id_2)])

        await harness.event_bus.publish(when_command(agg1, lambda a: a.deposit(50.0)))
        await harness.event_bus.publish(when_command(agg2, lambda a: a.deposit(100.0)))

        e1 = then_event_published(harness, MoneyDeposited, aggregate_id=id_1, amount=50.0)
        assert e1.aggregate_id == id_1
        e2 = then_event_published(harness, MoneyDeposited, aggregate_id=id_2, amount=100.0)
        assert e2.aggregate_id == id_2
