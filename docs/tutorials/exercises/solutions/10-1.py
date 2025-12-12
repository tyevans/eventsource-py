"""
Tutorial 10 - Exercise 1 Solution: Resumable Order Revenue Projection

This solution demonstrates checkpoint-based resumability for projections.
The projection tracks order revenue and can resume processing after
application restarts without losing progress.

Run with: python 10-1.py
"""

import asyncio
from uuid import uuid4

from eventsource import (
    CheckpointTrackingProjection,
    DomainEvent,
    InMemoryCheckpointRepository,
    register_event,
)

# =============================================================================
# Events
# =============================================================================


@register_event
class OrderPlaced(DomainEvent):
    """Event recording that an order was placed."""

    event_type: str = "OrderPlaced"
    aggregate_type: str = "Order"
    total: float


# =============================================================================
# Projection with Checkpoint Support
# =============================================================================


class OrderRevenueProjection(CheckpointTrackingProjection):
    """
    Tracks total revenue from orders.

    This projection demonstrates:
    - Automatic checkpoint tracking after each event
    - Resumable processing using checkpoint position
    - Reset capability for rebuilding

    Note: The in-memory counters (total_revenue, order_count) are lost
    on restart. In production, you would use a database-backed read model
    (DatabaseProjection) to persist these values as well.
    """

    def __init__(self, checkpoint_repo=None):
        super().__init__(checkpoint_repo=checkpoint_repo)
        self.total_revenue = 0.0
        self.order_count = 0

    def subscribed_to(self) -> list[type[DomainEvent]]:
        """Return event types this projection handles."""
        return [OrderPlaced]

    async def _process_event(self, event: DomainEvent) -> None:
        """
        Process an order event.

        This method is called by handle() and the checkpoint is
        automatically updated after successful processing.
        """
        if isinstance(event, OrderPlaced):
            self.total_revenue += event.total
            self.order_count += 1

    async def _truncate_read_models(self) -> None:
        """
        Reset the in-memory read model.

        Called by reset() to clear data before rebuilding.
        """
        self.total_revenue = 0.0
        self.order_count = 0


# =============================================================================
# Main Demo
# =============================================================================


async def main():
    print("=" * 60)
    print("Tutorial 10 - Exercise 1: Resumable Order Revenue Projection")
    print("=" * 60)
    print()

    # ==========================================================================
    # Setup: Create checkpoint repository and test events
    # ==========================================================================

    # The checkpoint repository persists across "restarts"
    # In production, this would be PostgreSQLCheckpointRepository
    checkpoint_repo = InMemoryCheckpointRepository()

    # Simulated event store - all order events
    all_events = [
        OrderPlaced(aggregate_id=uuid4(), total=100.00, aggregate_version=1),
        OrderPlaced(aggregate_id=uuid4(), total=250.00, aggregate_version=1),
        OrderPlaced(aggregate_id=uuid4(), total=75.50, aggregate_version=1),
        OrderPlaced(aggregate_id=uuid4(), total=300.00, aggregate_version=1),
        OrderPlaced(aggregate_id=uuid4(), total=150.00, aggregate_version=1),
    ]

    print(f"Total events in 'event store': {len(all_events)}")
    print(f"Total expected revenue: ${sum(e.total for e in all_events):.2f}")
    print()

    # ==========================================================================
    # Phase 1: Initial Processing (before restart)
    # ==========================================================================

    print("PHASE 1: Initial Processing")
    print("-" * 40)

    projection = OrderRevenueProjection(checkpoint_repo=checkpoint_repo)

    # Process first 3 events
    print("Processing first 3 events...")
    for i, event in enumerate(all_events[:3], 1):
        await projection.handle(event)
        print(f"  Event {i}: Order ${event.total:.2f}")

    print()
    print(f"Revenue after 3 events: ${projection.total_revenue:.2f}")
    print(f"Orders processed: {projection.order_count}")

    # Get and display checkpoint
    checkpoint = await projection.get_checkpoint()
    print(f"Checkpoint saved: {checkpoint}")

    # Get lag metrics
    metrics = await projection.get_lag_metrics()
    if metrics:
        print(f"Events processed (from metrics): {metrics['events_processed']}")

    print()

    # ==========================================================================
    # Phase 2: Simulate Application Restart
    # ==========================================================================

    print("PHASE 2: Simulating Application Restart")
    print("-" * 40)

    # Delete the projection (simulates application shutdown)
    print("Destroying projection instance...")
    del projection

    print("Application 'restarted'")
    print()

    # ==========================================================================
    # Phase 3: Resume After Restart
    # ==========================================================================

    print("PHASE 3: Resume After Restart")
    print("-" * 40)

    # Create new projection with SAME checkpoint repository
    projection2 = OrderRevenueProjection(checkpoint_repo=checkpoint_repo)

    # In-memory state is gone
    print("In-memory state after restart:")
    print(f"  Revenue: ${projection2.total_revenue:.2f} (reset to 0)")
    print(f"  Orders: {projection2.order_count} (reset to 0)")

    # But checkpoint is preserved!
    resumed_checkpoint = await projection2.get_checkpoint()
    print(f"\nCheckpoint preserved: {resumed_checkpoint}")

    # In a real application, you would query the event store for events
    # AFTER the checkpoint position and only process those.
    # For this demo, we know we processed 3, so we continue from index 3.

    print()
    print("Processing remaining events (4 and 5)...")
    for i, event in enumerate(all_events[3:], 4):
        await projection2.handle(event)
        print(f"  Event {i}: Order ${event.total:.2f}")

    print()
    print(f"Revenue (new events only): ${projection2.total_revenue:.2f}")
    print(f"Orders (new events only): {projection2.order_count}")

    # Final checkpoint
    final_checkpoint = await projection2.get_checkpoint()
    print(f"Final checkpoint: {final_checkpoint}")

    # ==========================================================================
    # Important Notes
    # ==========================================================================

    print()
    print("=" * 60)
    print("IMPORTANT NOTES FOR PRODUCTION")
    print("=" * 60)
    print()
    print("1. In-Memory State vs. Checkpoint:")
    print("   - The checkpoint tracks the POSITION in the event stream")
    print("   - The in-memory counters (revenue, count) are LOST on restart")
    print("   - For persistent read models, use DatabaseProjection with SQL")
    print()
    print("2. Full Recovery Pattern:")
    print("   - Option A: Use database-backed read model (recommended)")
    print("   - Option B: Replay all events from beginning on startup")
    print("   - Option C: Replay events since last checkpoint + restore state")
    print()
    print("3. Production Setup:")
    print("   - Use PostgreSQLCheckpointRepository for durable checkpoints")
    print("   - Use DatabaseProjection for persistent read models")
    print("   - Monitor lag_seconds for health checks")
    print()

    # ==========================================================================
    # Bonus: Demonstrate Reset and Rebuild
    # ==========================================================================

    print("=" * 60)
    print("BONUS: Reset and Rebuild")
    print("=" * 60)
    print()

    print("Before reset:")
    print(f"  Events processed: {projection2.order_count}")
    cp = await projection2.get_checkpoint()
    print(f"  Checkpoint: {cp}")

    # Reset clears both checkpoint and read model
    await projection2.reset()

    print("\nAfter reset:")
    print(f"  Events processed: {projection2.order_count}")
    cp = await projection2.get_checkpoint()
    print(f"  Checkpoint: {cp}")

    # Rebuild by replaying all events
    print("\nRebuilding from scratch...")
    for event in all_events:
        await projection2.handle(event)

    print("\nAfter rebuild:")
    print(f"  Revenue: ${projection2.total_revenue:.2f}")
    print(f"  Orders: {projection2.order_count}")
    cp = await projection2.get_checkpoint()
    print(f"  Checkpoint: {cp}")


if __name__ == "__main__":
    asyncio.run(main())
