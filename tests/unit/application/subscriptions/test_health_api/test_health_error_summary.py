"""The subscription health report must not carry payload values or stack traces."""

from __future__ import annotations

from uuid import uuid4

from pydantic import BaseModel, ValidationError

from eventsource.application.subscriptions.error_handling_types import (
    ErrorCategory,
    ErrorClassification,
    ErrorInfo,
    ErrorSeverity,
)
from eventsource.application.subscriptions.subscription import SubscriptionState

MARKER = "victim-secret@example.com"


class _Ledger(BaseModel):
    owner_id: str
    account_id: int


def _invalid_event_error() -> ValidationError:
    try:
        _Ledger.model_validate({"owner_id": MARKER, "account_id": MARKER})
    except ValidationError as error:
        return error
    raise AssertionError("expected a validation error")


async def test_health_report_keeps_marker_out_of_last_error(manager, mock_subscriber) -> None:
    subscription = await manager.subscribe(mock_subscriber, name="Ledger")
    await subscription.record_event_failed(_invalid_event_error())
    await subscription.transition_to(SubscriptionState.ERROR)

    health = await manager.health_check()
    comprehensive = manager.get_comprehensive_health()
    checked = manager.check_health("Ledger")

    assert MARKER not in repr(health)
    assert MARKER not in repr(comprehensive)
    assert MARKER not in repr(checked)
    last_error = health.subscriptions[0].last_error
    assert last_error is not None
    assert last_error.startswith("ValidationError(")
    assert "('account_id',):int_parsing" in last_error


async def test_single_subscription_health_keeps_marker_out_of_last_error(
    manager, mock_subscriber
) -> None:
    subscription = await manager.subscribe(mock_subscriber, name="Ledger")
    await subscription.record_event_failed(_invalid_event_error())

    sub_health = manager.get_subscription_health("Ledger")

    assert sub_health is not None
    assert MARKER not in repr(sub_health)
    assert sub_health.last_error is not None
    assert sub_health.last_error.startswith("ValidationError(")


def test_error_info_to_dict_omits_stacktrace() -> None:
    info = ErrorInfo(
        event_id=uuid4(),
        event_type="LedgerEntryRecorded",
        position=None,
        error_type="ValidationError",
        error_message="bad input",
        error_stacktrace="Traceback (most recent call last): secret frame",
        classification=ErrorClassification(
            category=ErrorCategory.PERMANENT,
            severity=ErrorSeverity.HIGH,
            retryable=False,
        ),
    )

    report = info.to_dict()

    assert "error_stacktrace" not in report
    assert "secret frame" not in repr(report)
    assert info.error_stacktrace.startswith("Traceback")
