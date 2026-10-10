"""Handler-related exceptions for the eventsource package."""

from __future__ import annotations

from eventsource.domain.exceptions.base import EventSourceError


class HandlerDispatchError(EventSourceError):
    """
    Raised after a delivery attempt when one or more handlers failed.

    Buses that dispatch a single delivery to multiple handlers must invoke
    every handler for that delivery -- one handler's failure must not skip
    the rest (error isolation). Once all handlers have run, if any failed,
    the bus raises this aggregate error so the caller's no-ack / redelivery
    path is unchanged: the individual failures are isolated from each other,
    but the delivery as a whole is still treated as failed and eligible for
    retry (and eventually the dead letter queue), exactly as if a single
    handler had raised.

    Attributes:
        failures: List of (handler_name, exception) pairs, one per handler
            that raised, in the order handlers were invoked.
    """

    def __init__(self, failures: list[tuple[str, Exception]]) -> None:
        self.failures = failures
        handler_names = ", ".join(name for name, _ in failures)
        super().__init__(f"{len(failures)} handler(s) failed during dispatch: {handler_names}")


class DuplicateHandlerError(EventSourceError):
    """
    Raised when two @handles methods in one class claim the same event type.

    Without this check, discovery order (alphabetical via dir()) silently
    picks one handler and drops the other's state mutation.
    """

    def __init__(
        self,
        owner_name: str,
        event_type: type,
        first_handler: str,
        second_handler: str,
    ) -> None:
        self.owner_name = owner_name
        self.event_type = event_type
        self.first_handler = first_handler
        self.second_handler = second_handler
        super().__init__(
            f"{owner_name} declares multiple handlers for "
            f"{event_type.__name__}: '{first_handler}' and '{second_handler}'. "
            f"Each event type may have exactly one @handles method per class."
        )


class HandlerSignatureError(EventSourceError):
    """
    Raised when an event handler has an invalid signature.

    This exception provides detailed guidance on how to fix invalid handler
    signatures, including expected signature patterns and hints for common
    mistakes.

    Attributes:
        handler_name: Name of the handler method
        owner_name: Name of the class containing the handler
        event_type: The event type from @handles decorator
        param_count: Actual number of parameters (excluding self)
        is_async_required: Whether async is required for this handler
    """

    def __init__(
        self,
        handler_name: str,
        owner_name: str,
        event_type: type,
        param_count: int,
        is_async_required: bool = True,
        reason: str | None = None,
    ) -> None:
        self.handler_name = handler_name
        self.owner_name = owner_name
        self.event_type = event_type
        self.param_count = param_count
        self.is_async_required = is_async_required
        self.reason = reason

        event_name = event_type.__name__
        async_prefix = "async " if is_async_required else ""

        if reason is not None:
            message = (
                f"Handler '{handler_name}' in {owner_name} is invalid for "
                f"@handles({event_name}): {reason}"
            )
        else:
            message = (
                f"Handler '{handler_name}' in {owner_name} has invalid signature "
                f"for @handles({event_name}).\n\n"
                f"Expected one of:\n"
                f"  {async_prefix}def {handler_name}(self, event: {event_name}) -> None\n"
                f"  {async_prefix}def {handler_name}(self, context, event: {event_name}) -> None\n\n"
                f"Got: {param_count} parameter(s) (excluding self)\n\n"
                f"Hint: Ensure your handler has exactly 1 or 2 parameters after 'self'."
            )

        super().__init__(message)


__all__ = [
    "DuplicateHandlerError",
    "HandlerDispatchError",
    "HandlerSignatureError",
]
