"""Infrastructure exception taxonomy lives in the ports ring (ADR 0041)."""

from eventsource.domain.exceptions import EventSourceError
from eventsource.ports.exceptions import (
    CheckpointError,
    CheckpointNotFoundError,
    EventBusConnectionError,
    EventStoreConnectionError,
    LockAcquisitionError,
    LockNotHeldError,
    PositionDecodeError,
    PositionForeignError,
    SubscriptionAlreadyExistsError,
    SubscriptionConfigError,
    SubscriptionError,
    SubscriptionStateError,
    TransitionError,
)


class TestPortsExceptionsHome:
    def test_all_rooted_in_eventsource_error(self) -> None:
        for exc in (
            CheckpointError,
            CheckpointNotFoundError,
            EventBusConnectionError,
            EventStoreConnectionError,
            LockAcquisitionError,
            LockNotHeldError,
            PositionDecodeError,
            PositionForeignError,
            SubscriptionAlreadyExistsError,
            SubscriptionConfigError,
            SubscriptionError,
            SubscriptionStateError,
            TransitionError,
        ):
            assert issubclass(exc, EventSourceError)

    def test_domain_module_no_longer_exports_them(self) -> None:
        from eventsource.domain import exceptions as domain_exceptions

        for name in (
            "CheckpointError",
            "LockAcquisitionError",
            "LockNotHeldError",
            "PositionDecodeError",
            "PositionForeignError",
            "SubscriptionError",
        ):
            assert not hasattr(domain_exceptions, name)

    def test_top_level_reexports_still_work(self) -> None:
        # Only PositionDecodeError/PositionForeignError are in the top-level
        # __all__ today among the 13 moved names.
        from eventsource import PositionDecodeError as TopLevel  # noqa: F401
