"""Snapshot serialization and deserialization mixin for aggregates."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, TypeVar, cast, get_args, get_origin

from pydantic import BaseModel

if TYPE_CHECKING:
    from uuid import UUID


class AggregateSnapshotMixin[TState: BaseModel]:
    """Mixin providing snapshot serialization and introspection for aggregates."""

    if TYPE_CHECKING:
        _aggregate_id: UUID
        _version: int
        _state: TState | None

    def _serialize_state(self) -> dict[str, Any]:
        """
        Serialize the current aggregate state for snapshotting.

        Converts the Pydantic state model to a JSON-compatible dictionary
        using model_dump(mode="json"). This ensures all nested models,
        UUIDs, datetimes, and other complex types are properly serialized.

        Returns:
            Dictionary representation of the state, suitable for JSON storage.
            Returns empty dict if state is None (new aggregate).

        Example:
            >>> order = OrderAggregate(uuid4())
            >>> order.create(customer_id=uuid4())
            >>> state_dict = order._serialize_state()
            >>> # state_dict can be stored as JSON in snapshot
        """
        if self._state is None:
            return {}
        return self._state.model_dump(mode="json")

    def _restore_from_snapshot(
        self,
        state_dict: dict[str, Any],
        version: int,
    ) -> None:
        """
        Restore aggregate state from a snapshot.

        Sets the aggregate's internal state and version from snapshot data.
        After calling this method, the aggregate is in the state it was
        when the snapshot was taken. Subsequent events can then be replayed
        to bring it to the current state.

        Args:
            state_dict: Serialized state dictionary from snapshot.
                       Should be the output of _serialize_state().
            version: Aggregate version when snapshot was taken.
                    Events with version > this will be replayed.

        Raises:
            ValidationError: If state_dict doesn't match TState schema.

        Note:
            This method is called by AggregateRepository before replaying
            events since the snapshot. User code should not call this directly.

        Example:
            >>> # Internal use by repository:
            >>> aggregate = OrderAggregate(aggregate_id)
            >>> aggregate._restore_from_snapshot(snapshot.state, snapshot.version)
            >>> aggregate.load_from_history(events_since_snapshot)
        """
        if not state_dict:
            # Empty state - leave as initial
            self._version = version
            return

        state_type = self._get_state_type()
        self._state = state_type.model_validate(state_dict)
        self._version = version

    def _get_state_type(self) -> type[TState]:
        """
        Get the state type (TState) from the Generic parameter.

        Uses Python's typing introspection to extract the concrete type
        used for TState in the subclass. This is needed for deserializing
        snapshot state back into the correct Pydantic model.

        Returns:
            The concrete type used for TState in this aggregate class.

        Raises:
            RuntimeError: If the state type cannot be determined.

        Example:
            >>> class OrderAggregate(AggregateRoot[OrderState]):
            ...     ...
            >>>
            >>> aggregate = OrderAggregate(uuid4())
            >>> state_type = aggregate._get_state_type()
            >>> assert state_type is OrderState
        """
        # Walk up the MRO to find the AggregateRoot parameterization
        for base in type(self).__mro__:
            if not hasattr(base, "__orig_bases__"):
                continue

            for orig_base in base.__orig_bases__:
                origin = get_origin(orig_base)

                # Check if this is a Generic base that's AggregateRoot or subclass
                if origin is None:
                    continue

                # Handle both AggregateRoot and DeclarativeAggregate
                try:
                    if (
                        issubclass(origin, AggregateSnapshotMixin)
                        and origin is not AggregateSnapshotMixin
                    ):
                        args = get_args(orig_base)
                        if args and not isinstance(args[0], TypeVar):
                            return cast(type[TState], args[0])
                except TypeError:
                    # issubclass can fail for some typing constructs
                    continue

        raise RuntimeError(
            f"Cannot determine state type for {type(self).__name__}. "
            "Ensure the class properly inherits from AggregateRoot[StateType]."
        )


__all__ = ["AggregateSnapshotMixin"]
