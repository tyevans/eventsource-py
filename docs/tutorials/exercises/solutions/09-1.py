"""
Tutorial 9 - Exercise 1 Solution: Retry Logic Implementation

This solution demonstrates implementing a generic retry mechanism for
handling concurrent updates with OptimisticLockError.

Run with: python 09-1.py
"""

import asyncio
from uuid import UUID, uuid4

from pydantic import BaseModel

from eventsource import (
    AggregateRepository,
    AggregateRoot,
    DomainEvent,
    InMemoryEventStore,
    OptimisticLockError,
    register_event,
)

# =============================================================================
# Events
# =============================================================================


@register_event
class CounterIncremented(DomainEvent):
    event_type: str = "CounterIncremented"
    aggregate_type: str = "Counter"


# =============================================================================
# State and Aggregate
# =============================================================================


class CounterState(BaseModel):
    counter_id: UUID
    value: int = 0


class CounterAggregate(AggregateRoot[CounterState]):
    aggregate_type = "Counter"

    def _get_initial_state(self) -> CounterState:
        return CounterState(counter_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, CounterIncremented):
            if self._state is None:
                self._state = CounterState(counter_id=self.aggregate_id, value=1)
            else:
                self._state = self._state.model_copy(
                    update={
                        "value": self._state.value + 1,
                    }
                )

    def increment(self) -> None:
        self.apply_event(
            CounterIncremented(
                aggregate_id=self.aggregate_id,
                aggregate_version=self.get_next_version(),
            )
        )


# =============================================================================
# Retry Logic Solution
# =============================================================================


async def save_with_retry(
    repo: AggregateRepository,
    aggregate_id: UUID,
    operation,  # Callable that modifies aggregate
    max_retries: int = 3,
    initial_delay: float = 0.01,
) -> None:
    """
    Save an aggregate with automatic retry on optimistic lock conflicts.

    This function implements the standard pattern for handling concurrent
    modifications in event-sourced systems:

    1. Load the latest aggregate state
    2. Apply the desired operation
    3. Attempt to save
    4. If conflict occurs, wait with exponential backoff and retry
    5. If all retries exhausted, propagate the error

    Args:
        repo: The aggregate repository
        aggregate_id: ID of the aggregate to modify
        operation: Function that takes an aggregate and modifies it.
                  This is called fresh each retry with the latest state.
        max_retries: Maximum number of retry attempts (default: 3)
        initial_delay: Initial delay before first retry in seconds (default: 0.01)

    Raises:
        OptimisticLockError: If all retries are exhausted without success
        AggregateNotFoundError: If the aggregate does not exist

    Example:
        >>> await save_with_retry(
        ...     repo,
        ...     order_id,
        ...     lambda order: order.ship(tracking_number),
        ...     max_retries=5,
        ... )
    """
    for attempt in range(max_retries):
        try:
            # Step 1: Load fresh aggregate state
            # This ensures we have the latest version after any concurrent updates
            aggregate = await repo.load(aggregate_id)

            # Step 2: Apply the operation to the aggregate
            # The operation might check business rules that could fail
            operation(aggregate)

            # Step 3: Save the aggregate
            # This is where OptimisticLockError can occur
            await repo.save(aggregate)

            # Success! Log and return
            print(f"Saved successfully on attempt {attempt + 1}")
            return

        except OptimisticLockError:
            # Step 4: Handle the conflict
            print(f"Conflict on attempt {attempt + 1}/{max_retries}")

            # Check if we have retries remaining
            if attempt == max_retries - 1:
                # Step 5: All retries exhausted - propagate the error
                print("All retries exhausted!")
                raise

            # Calculate exponential backoff delay
            # attempt 0: initial_delay * 1 = 0.01s
            # attempt 1: initial_delay * 2 = 0.02s
            # attempt 2: initial_delay * 4 = 0.04s
            delay = initial_delay * (2**attempt)
            await asyncio.sleep(delay)


# =============================================================================
# Demo
# =============================================================================


async def main():
    print("=== Retry Logic Exercise Solution ===\n")

    # Set up infrastructure
    store = InMemoryEventStore()
    repo = AggregateRepository(
        event_store=store,
        aggregate_factory=CounterAggregate,
        aggregate_type="Counter",
    )

    # Create initial counter
    counter_id = uuid4()
    counter = repo.create_new(counter_id)
    counter.increment()  # Start at 1
    await repo.save(counter)
    print(f"Counter created with value: {counter.state.value}\n")

    # Define the concurrent increment operation
    async def concurrent_increment():
        await save_with_retry(
            repo,
            counter_id,
            lambda agg: agg.increment(),
            max_retries=3,
        )

    # Run multiple increments concurrently
    # This will cause OptimisticLockError conflicts
    print("Running 5 concurrent increments...")
    await asyncio.gather(*[concurrent_increment() for _ in range(5)])

    # Verify final value
    final = await repo.load(counter_id)
    print(f"\nFinal counter value: {final.state.value}")
    print("Expected: 6 (started at 1, incremented 5 times)")

    # Verify with events
    stream = await store.get_events(counter_id, "Counter")
    print(f"Total events stored: {len(stream.events)}")


if __name__ == "__main__":
    asyncio.run(main())
