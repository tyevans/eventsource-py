"""Tests for DeciderScenario."""

from uuid import uuid4

import pytest

from eventsource.domain.exceptions import CommandRejectedError
from eventsource.testing import DeciderScenario
from tests.unit.domain.test_decider_aggregate import (
    Account,
    AccountOpened,
    DepositMoney,
    MoneyDeposited,
    OpenAccount,
)


class TestGivenWhenThen:
    def test_then_events_asserts_types_in_order(self) -> None:
        agg_id = uuid4()
        (
            DeciderScenario(Account)
            .given(AccountOpened(aggregate_id=agg_id, aggregate_version=1, owner="alice"))
            .when(DepositMoney(account_id=agg_id, amount=5.0))
            .then_events(MoneyDeposited)
        )

    def test_then_events_fails_on_wrong_type(self) -> None:
        agg_id = uuid4()
        scenario = (
            DeciderScenario(Account)
            .given(AccountOpened(aggregate_id=agg_id, aggregate_version=1, owner="alice"))
            .when(DepositMoney(account_id=agg_id, amount=5.0))
        )
        with pytest.raises(AssertionError):
            scenario.then_events(AccountOpened)

    def test_then_rejected_default_type_and_match(self) -> None:
        (
            DeciderScenario(Account)
            .when(DepositMoney(account_id=uuid4(), amount=5.0))
            .then_rejected(match="not open")
        )

    def test_then_rejected_accepts_custom_exception(self) -> None:
        (
            DeciderScenario(Account)
            .when(DepositMoney(account_id=uuid4(), amount=5.0))
            .then_rejected(CommandRejectedError)
        )

    def test_then_events_reports_unexpected_rejection(self) -> None:
        scenario = DeciderScenario(Account).when(DepositMoney(account_id=uuid4(), amount=5.0))
        with pytest.raises(AssertionError, match="rejected"):
            scenario.then_events(MoneyDeposited)

    def test_then_rejected_fails_when_events_produced(self) -> None:
        scenario = DeciderScenario(Account).when(OpenAccount(account_id=uuid4(), owner="alice"))
        with pytest.raises(AssertionError):
            scenario.then_rejected()

    def test_events_property_exposes_produced_events(self) -> None:
        scenario = DeciderScenario(Account).when(OpenAccount(account_id=uuid4(), owner="alice"))
        assert len(scenario.events) == 1
        assert isinstance(scenario.events[0], AccountOpened)

    def test_three_function_form(self) -> None:
        (
            DeciderScenario(
                decide=Account.decide,
                evolve=Account.evolve,
                initial_state=Account.initial_state,
            )
            .when(OpenAccount(account_id=uuid4(), owner="alice"))
            .then_events(AccountOpened)
        )

    def test_when_before_then_required(self) -> None:
        with pytest.raises(AssertionError, match="when"):
            DeciderScenario(Account).then_events(AccountOpened)

    def test_when_clears_stale_error_from_prior_rejection(self) -> None:
        scenario = DeciderScenario(Account).when(DepositMoney(account_id=uuid4(), amount=5.0))
        scenario.then_rejected()  # sanity: first when() did reject
        scenario.when(OpenAccount(account_id=uuid4(), owner="alice"))
        scenario.then_events(AccountOpened)
