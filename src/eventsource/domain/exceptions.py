"""Library exceptions for the eventsource package."""

from typing import TYPE_CHECKING
from uuid import UUID

if TYPE_CHECKING:
    from collections.abc import Sequence

    from eventsource.domain.event import DomainEvent


class EventSourceError(Exception):
    """Base exception for eventsource library."""

    pass


class OptimisticLockError(EventSourceError):
    """Raised when there's a version conflict during event append."""

    def __init__(
        self, aggregate_id: UUID, expected_version: int | str, actual_version: int
    ) -> None:
        """
        Args:
            aggregate_id: The aggregate whose append was rejected
            expected_version: The version the caller required, or the name of
                the non-numeric `ExpectedVersion` kind they used
                (`"no_stream"`, `"stream_exists"`, `"any"`). Rendering the
                kind by name matters: a store that reported `no_stream` as
                the integer `0` told the user it expected a version they
                never wrote.
            actual_version: The stream's current version
        """
        self.aggregate_id = aggregate_id
        self.expected_version = expected_version
        self.actual_version = actual_version
        expectation = (
            f"version {expected_version}"
            if isinstance(expected_version, int)
            else str(expected_version)
        )
        super().__init__(
            f"Optimistic lock error for aggregate {aggregate_id}: "
            f"expected {expectation}, but current version is {actual_version}"
        )


class EventNotFoundError(EventSourceError):
    """Raised when an event cannot be found."""

    def __init__(self, event_id: UUID) -> None:
        self.event_id = event_id
        super().__init__(f"Event not found: {event_id}")


class ProjectionError(EventSourceError):
    """Raised when a projection fails to process an event."""

    def __init__(self, projection_name: str, event_id: UUID, message: str) -> None:
        self.projection_name = projection_name
        self.event_id = event_id
        super().__init__(f"Projection {projection_name} failed on event {event_id}: {message}")


class AggregateNotFoundError(EventSourceError):
    """Raised when an aggregate cannot be found."""

    def __init__(self, aggregate_id: UUID, aggregate_type: str | None = None) -> None:
        self.aggregate_id = aggregate_id
        self.aggregate_type = aggregate_type
        type_info = f" of type {aggregate_type}" if aggregate_type else ""
        super().__init__(f"Aggregate{type_info} not found: {aggregate_id}")


class CommandRejectedError(EventSourceError):
    """
    A command was rejected by domain logic.

    Raising this from ``decide()`` (or a command method) is a convention,
    not a requirement — any exception may be used. It gives application
    code one catchable type meaning "the domain said no" as distinct from
    a bug.

    Attributes:
        command: The rejected command object, when provided.
    """

    def __init__(self, message: str, command: object | None = None) -> None:
        self.command = command
        super().__init__(message)


class EventStoreError(EventSourceError):
    """Raised when there's an error in the event store."""

    pass


class EventBusError(EventSourceError):
    """Raised when there's an error in the event bus."""

    pass


class SerializationError(EventSourceError):
    """Raised when event serialization or deserialization fails."""

    def __init__(self, event_type: str, message: str) -> None:
        self.event_type = event_type
        super().__init__(f"Serialization error for {event_type}: {message}")


class EventVersionError(EventSourceError):
    """
    Raised when event version validation fails during aggregate event application.

    This error occurs when:
    - An event has a version gap (e.g., jumping from version 2 to version 5)
    - An event has a version regression (e.g., going from version 5 to version 3)
    - An event has an unexpected version number

    This validation helps ensure aggregate state integrity by detecting
    out-of-order or incorrectly versioned events.

    Attributes:
        expected_version: The version that was expected (current version + 1)
        actual_version: The version found in the event
        event_id: ID of the event with invalid version
        aggregate_id: ID of the aggregate being updated
    """

    def __init__(
        self,
        expected_version: int,
        actual_version: int,
        event_id: UUID,
        aggregate_id: UUID,
    ) -> None:
        self.expected_version = expected_version
        self.actual_version = actual_version
        self.event_id = event_id
        self.aggregate_id = aggregate_id
        super().__init__(
            f"Event version mismatch for aggregate {aggregate_id}: "
            f"expected version {expected_version}, got {actual_version} "
            f"(event_id: {event_id})"
        )


class UnhandledEventError(EventSourceError):
    """
    Raised when an event has no registered handler and strict mode is enabled.

    This error occurs in DeclarativeAggregate or DeclarativeProjection when:
    - An event type is applied/processed that has no @handles decorator
    - The class has unregistered_event_handling set to "error"

    This helps catch bugs such as:
    - Typos in handler names
    - Missing @handles decorators
    - State inconsistencies from silently ignored events

    Attributes:
        event_type: The name of the event type that wasn't handled
        event_id: ID of the unhandled event
        handler_class: Name of the aggregate/projection class
        available_handlers: List of event type names that have handlers
    """

    def __init__(
        self,
        event_type: str,
        event_id: UUID,
        handler_class: str,
        available_handlers: list[str],
    ) -> None:
        self.event_type = event_type
        self.event_id = event_id
        self.handler_class = handler_class
        self.available_handlers = available_handlers
        handlers_str = ", ".join(available_handlers) if available_handlers else "none"
        super().__init__(
            f"No handler registered for event type '{event_type}' "
            f"in {handler_class}. "
            f"Available handlers: {handlers_str}. "
            f"Add @handles({event_type}) decorator or set "
            f"unregistered_event_handling='ignore' or 'warn'."
        )


class AggregateNotCreatedError(EventSourceError):
    """
    Raised when accessing state of an aggregate before creation event.

    This error occurs when:
    1. Aggregate has `requires_creation_event = True`
    2. No events have been applied yet
    3. Code attempts to access `aggregate.state`

    Use `aggregate.state_or_none` or `aggregate.is_created` to safely
    check if the aggregate has been created.

    Attributes:
        aggregate_class: Name of the aggregate class that wasn't created
        suggestion: Optional hint for how to resolve the error
    """

    def __init__(self, aggregate_class: str, suggestion: str | None = None) -> None:
        self.aggregate_class = aggregate_class
        self.suggestion = suggestion

        message = f"{aggregate_class} has not been created. Apply a creation event first."
        if suggestion:
            message += f" Hint: {suggestion}"

        super().__init__(message)


class AggregateTypeMismatchError(EventSourceError):
    """An event class declares a different aggregate_type than its aggregate.

    Emitting `OrderShipped(aggregate_type="Shipment")` from an aggregate
    whose `aggregate_type` is `"Order"` used to be silently restamped to
    `"Order"` -- the declared value was accepted at import, then discarded
    at emit time with no signal. Since `aggregate_type` becomes the stream
    category, the disagreement is invisible in a save/load round-trip and
    only shows up as events missing from a category read.

    Attributes:
        event_class: Name of the event class with the divergent declaration
        event_aggregate_type: What the event class declares
        aggregate_class: Name of the aggregate emitting it
        aggregate_type: What the aggregate declares
    """

    def __init__(
        self,
        event_class: str,
        event_aggregate_type: str,
        aggregate_class: str,
        aggregate_type: str,
    ) -> None:
        self.event_class = event_class
        self.event_aggregate_type = event_aggregate_type
        self.aggregate_class = aggregate_class
        self.aggregate_type = aggregate_type
        super().__init__(
            f"{event_class} declares aggregate_type={event_aggregate_type!r} but is "
            f"emitted from {aggregate_class}, which declares {aggregate_type!r}. "
            f"An event's aggregate_type is its stream category, so the two must "
            f"agree. Drop the declaration from {event_class} (the aggregate stamps "
            f"it) or emit the event from the matching aggregate."
        )


class AggregateTypeNotSetError(EventSourceError):
    """
    Raised when a concrete aggregate class is constructed without declaring
    aggregate_type.

    Aggregate identity is not optional: aggregate_type becomes the stream
    category, so a missing value would silently create wrongly-typed
    streams (the old behavior was a silent "Unknown" default).
    """

    def __init__(self, class_name: str) -> None:
        self.class_name = class_name
        super().__init__(
            f"{class_name} does not declare 'aggregate_type'. Every concrete "
            f"aggregate class must set it to its stream category, e.g. "
            f'aggregate_type = "Order".'
        )


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


class DuplicateEventError(EventSourceError):
    """An event with this event_id already exists in the store."""


class SnapshotError(EventSourceError):
    """
    Base exception for snapshot-related errors.

    All snapshot exceptions inherit from this class, allowing callers
    to catch all snapshot errors with a single except clause.

    Example:
        >>> try:
        ...     snapshot = await load_snapshot(aggregate_id)
        ... except SnapshotError:
        ...     # Fall back to full event replay
        ...     pass
    """

    pass


class SnapshotDeserializationError(SnapshotError):
    """
    Raised when a snapshot cannot be deserialized.

    This typically occurs when:
    - The state JSON is malformed
    - The state doesn't match the expected Pydantic model
    - Required fields are missing from the state

    The system handles this gracefully by falling back to full event
    replay, which will recreate the snapshot with valid data.

    Attributes:
        aggregate_id: ID of the aggregate whose snapshot failed
        aggregate_type: Type of the aggregate
        original_error: The underlying deserialization error

    Example:
        >>> try:
        ...     state = state_type.model_validate(snapshot.state)
        ... except ValidationError as e:
        ...     raise SnapshotDeserializationError(
        ...         aggregate_id=snapshot.aggregate_id,
        ...         aggregate_type=snapshot.aggregate_type,
        ...         original_error=e,
        ...     )
    """

    def __init__(
        self,
        aggregate_id: UUID,
        aggregate_type: str,
        original_error: Exception | None = None,
        message: str | None = None,
    ) -> None:
        self.aggregate_id = aggregate_id
        self.aggregate_type = aggregate_type
        self.original_error = original_error

        if message:
            self.message = message
        else:
            self.message = f"Failed to deserialize snapshot for {aggregate_type}/{aggregate_id}"
            if original_error:
                self.message += f": {original_error}"

        super().__init__(self.message)

    def __repr__(self) -> str:
        return (
            f"SnapshotDeserializationError("
            f"aggregate_id={self.aggregate_id!r}, "
            f"aggregate_type={self.aggregate_type!r}, "
            f"original_error={self.original_error!r})"
        )


class SnapshotSchemaVersionError(SnapshotError):
    """
    Raised when snapshot schema version doesn't match aggregate schema version.

    This is a normal occurrence during schema evolution. When an aggregate's
    state model changes and its schema_version is incremented, existing
    snapshots become incompatible and must be regenerated.

    The system handles this gracefully by:
    1. Logging the version mismatch
    2. Falling back to full event replay
    3. Optionally creating a new snapshot with the updated schema

    Attributes:
        aggregate_id: ID of the aggregate
        aggregate_type: Type of the aggregate
        snapshot_schema_version: Schema version stored in the snapshot
        expected_schema_version: Schema version expected by the aggregate

    Example:
        >>> if snapshot.schema_version != aggregate_class.schema_version:
        ...     raise SnapshotSchemaVersionError(
        ...         aggregate_id=snapshot.aggregate_id,
        ...         aggregate_type=snapshot.aggregate_type,
        ...         snapshot_schema_version=snapshot.schema_version,
        ...         expected_schema_version=aggregate_class.schema_version,
        ...     )
    """

    def __init__(
        self,
        aggregate_id: UUID,
        aggregate_type: str,
        snapshot_schema_version: int,
        expected_schema_version: int,
    ) -> None:
        self.aggregate_id = aggregate_id
        self.aggregate_type = aggregate_type
        self.snapshot_schema_version = snapshot_schema_version
        self.expected_schema_version = expected_schema_version

        self.message = (
            f"Schema version mismatch for {aggregate_type}/{aggregate_id}: "
            f"snapshot has schema_version={snapshot_schema_version}, "
            f"but aggregate expects schema_version={expected_schema_version}"
        )

        super().__init__(self.message)

    def __repr__(self) -> str:
        return (
            f"SnapshotSchemaVersionError("
            f"aggregate_id={self.aggregate_id!r}, "
            f"aggregate_type={self.aggregate_type!r}, "
            f"snapshot_schema_version={self.snapshot_schema_version}, "
            f"expected_schema_version={self.expected_schema_version})"
        )


class SnapshotNotFoundError(SnapshotError):
    """
    Raised when a snapshot is expected but not found.

    Note: This exception is rarely raised in practice because missing
    snapshots are a normal condition (graceful fallback to event replay).
    It's provided for cases where a snapshot is explicitly required.

    Attributes:
        aggregate_id: ID of the aggregate
        aggregate_type: Type of the aggregate

    Example:
        >>> snapshot = await store.get_snapshot(aggregate_id, "Order")
        >>> if snapshot is None and require_snapshot:
        ...     raise SnapshotNotFoundError(aggregate_id, "Order")
    """

    def __init__(
        self,
        aggregate_id: UUID,
        aggregate_type: str,
    ) -> None:
        self.aggregate_id = aggregate_id
        self.aggregate_type = aggregate_type

        self.message = f"No snapshot found for {aggregate_type}/{aggregate_id}"

        super().__init__(self.message)

    def __repr__(self) -> str:
        return (
            f"SnapshotNotFoundError("
            f"aggregate_id={self.aggregate_id!r}, "
            f"aggregate_type={self.aggregate_type!r})"
        )


# =============================================================================
# Event registry exceptions
# =============================================================================
#
# Merged from the former eventsource.events.registry module (ring migration,
# events/ -> domain/).


class EventTypeNotFoundError(EventSourceError):
    """
    Raised when an event type is not found in the registry.

    Provides helpful error messages including a list of available event types.
    """

    def __init__(self, event_type: str, available_types: list[str]) -> None:
        self.event_type = event_type
        self.available_types = available_types
        available = ", ".join(sorted(available_types)) if available_types else "none"
        super().__init__(
            f"Unknown event type: '{event_type}'. "
            f"Available types: {available}. "
            f"Did you forget to register this event type?"
        )


class DuplicateEventTypeError(EventSourceError):
    """
    Raised when attempting to register a different class with an existing event type name.
    """

    def __init__(
        self,
        event_type: str,
        existing_class: "type[DomainEvent]",
        new_class: "type[DomainEvent]",
    ) -> None:
        self.event_type = event_type
        self.existing_class = existing_class
        self.new_class = new_class
        super().__init__(
            f"Event type '{event_type}' is already registered to {existing_class.__name__}. "
            f"Cannot register {new_class.__name__} with the same type name."
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

    Example:
        >>> from eventsource import handles
        >>> from eventsource.application.projections.handlers import HandlerRegistry
        >>>
        >>> class BadProjection:
        ...     @handles(OrderCreated)
        ...     async def _handle(self, a, b, c, event):  # Too many params
        ...         pass
        ...
        >>> # Raises HandlerSignatureError with helpful message
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


# Merged from the former eventsource.multitenancy.exceptions module (ADR 0038).
# All three were already rooted at EventSourceError -- no rebase, just relocation.
# Except-site audit found no site depending on these NOT being EventSourceError.


class TenantContextNotSetError(EventSourceError):
    """
    Raised when tenant context is required but not set.

    This typically occurs when:
    1. Using TenantDomainEvent.with_tenant_context() without context
    2. Using TenantAwareRepository without tenant context
    3. Using get_required_tenant() without context

    Solution: Use set_current_tenant() or tenant_scope() before
    operations requiring tenant context.

    Example:
        >>> from eventsource import get_required_tenant
        >>> try:
        ...     tenant = get_required_tenant()
        ... except TenantContextNotSetError:
        ...     print("No tenant context set")
        No tenant context set
    """

    def __init__(self) -> None:
        super().__init__(
            "No tenant context set. Use set_current_tenant() or tenant_scope() "
            "before performing multi-tenant operations."
        )


class TenantContextResetError(EventSourceError):
    """
    Raised when a tenant context token is reset out of LIFO order, or reset
    more than once.

    Tokens returned by set_current_tenant() must be reset in strict LIFO
    order (most-recently-set token first) -- the same order that
    tenant_scope() / tenant_scope_sync() already guarantee automatically via
    `with` / `async with` block nesting. Resetting out of order (or resetting
    the same token twice) is rejected outright rather than silently
    restoring a stale tenant, because a silently-resurrected stale tenant is
    a data-leak shape in a multi-tenant system.

    Solution: Reset tokens in the exact reverse order they were created, or
    -- preferably -- use tenant_scope() / tenant_scope_sync() instead of
    manual set_current_tenant()/reset_tenant_context() so LIFO ordering is
    enforced structurally rather than by convention.

    Example:
        >>> from eventsource.domain.tenant_context import (
        ...     set_current_tenant,
        ...     reset_tenant_context,
        ... )
        >>> from uuid import uuid4
        >>> token_a = set_current_tenant(uuid4())
        >>> token_b = set_current_tenant(uuid4())
        >>> try:
        ...     reset_tenant_context(token_a)  # out of order: token_b is on top
        ... except TenantContextResetError:
        ...     print("Reset out of LIFO order")
        Reset out of LIFO order
        >>> reset_tenant_context(token_b)
        >>> reset_tenant_context(token_a)
    """

    def __init__(self, detail: str) -> None:
        super().__init__(
            f"Cannot reset tenant context: {detail} Tenant context tokens must be "
            "reset in strict LIFO order (most-recently-set token first, and each "
            "token reset at most once). tenant_scope()/tenant_scope_sync() "
            "guarantee this automatically via `with`/`async with` nesting -- "
            "prefer those over manual set_current_tenant()/reset_tenant_context() "
            "calls."
        )


class TenantMismatchError(EventSourceError):
    """
    Raised when event tenant_id doesn't match expected tenant context.

    This typically occurs when saving events with a different tenant_id
    than the current context. This error prevents cross-tenant data leakage
    and ensures tenant isolation in multi-tenant applications.

    Attributes:
        expected: The expected tenant UUID from context
        actual: The actual tenant UUID found in the event(s)
        event_ids: List of event IDs that have mismatched tenant

    Example:
        >>> from uuid import uuid4
        >>> from eventsource import TenantMismatchError
        >>> tenant_a = uuid4()
        >>> tenant_b = uuid4()
        >>> event_id = uuid4()
        >>> error = TenantMismatchError(
        ...     expected=tenant_a,
        ...     actual=tenant_b,
        ...     event_ids=[event_id],
        ... )
        >>> print(error)  # doctest: +ELLIPSIS
        Tenant mismatch: expected ..., got .... Affected events: [...]
    """

    def __init__(
        self,
        expected: UUID,
        actual: UUID,
        event_ids: "Sequence[UUID]",
    ) -> None:
        self.expected = expected
        self.actual = actual
        self.event_ids = list(event_ids)

        # Format event IDs for message (limit to 5 for readability)
        event_list = ", ".join(str(eid) for eid in self.event_ids[:5])
        if len(self.event_ids) > 5:
            event_list += f"... and {len(self.event_ids) - 5} more"

        super().__init__(
            f"Tenant mismatch: expected {expected}, got {actual}. Affected events: [{event_list}]"
        )
