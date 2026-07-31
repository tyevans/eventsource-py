"""Tests for the DomainCommand base model."""

from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from eventsource.commands import DomainCommand
from eventsource.events.base import DomainEvent


class OpenAccount(DomainCommand):
    owner_name: str


class SomethingHappened(DomainEvent):
    event_type: str = "SomethingHappened"
    aggregate_type: str = "Thing"


class TestDomainCommandDefaults:
    def test_command_id_generated(self) -> None:
        cmd = OpenAccount(owner_name="alice")
        assert isinstance(cmd.command_id, UUID)

    def test_two_commands_get_distinct_ids(self) -> None:
        a, b = OpenAccount(owner_name="a"), OpenAccount(owner_name="b")
        assert a.command_id != b.command_id
        assert a.correlation_id != b.correlation_id

    def test_issued_at_is_utc_aware(self) -> None:
        cmd = OpenAccount(owner_name="alice")
        assert cmd.issued_at.tzinfo is not None

    def test_actor_and_tenant_default_none(self) -> None:
        cmd = OpenAccount(owner_name="alice")
        assert cmd.actor_id is None
        assert cmd.tenant_id is None


class TestDomainCommandImmutability:
    def test_frozen(self) -> None:
        cmd = OpenAccount(owner_name="alice")
        with pytest.raises(ValidationError):
            cmd.owner_name = "bob"  # type: ignore[misc]


class TestCausedBy:
    def test_caused_by_copies_correlation_only(self) -> None:
        event = SomethingHappened(aggregate_id=uuid4(), aggregate_version=1)
        cmd = OpenAccount(owner_name="alice").caused_by(event)
        assert cmd.correlation_id == event.correlation_id
        assert cmd.owner_name == "alice"

    def test_caused_by_returns_new_instance(self) -> None:
        original = OpenAccount(owner_name="alice")
        event = SomethingHappened(aggregate_id=uuid4(), aggregate_version=1)
        chained = original.caused_by(event)
        assert chained is not original
        assert original.correlation_id != event.correlation_id


class TestMatchSupport:
    def test_class_pattern_with_keyword_capture(self) -> None:
        cmd: DomainCommand = OpenAccount(owner_name="alice")
        match cmd:
            case OpenAccount(owner_name=name):
                assert name == "alice"
            case _:
                pytest.fail("pattern did not match")


class TestCommandRejectedError:
    def test_is_eventsource_error_and_carries_command(self) -> None:
        from eventsource.exceptions import CommandRejectedError, EventSourceError

        cmd = OpenAccount(owner_name="alice")
        err = CommandRejectedError("account already open", command=cmd)
        assert isinstance(err, EventSourceError)
        assert err.command is cmd
        assert "already open" in str(err)

    def test_command_defaults_to_none(self) -> None:
        from eventsource.exceptions import CommandRejectedError

        assert CommandRejectedError("no").command is None
