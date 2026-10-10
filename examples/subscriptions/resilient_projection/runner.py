"""Execution runner and demo orchestrator for resilient subscription."""

import asyncio
import logging
import signal
import sys
from typing import Any
from uuid import uuid4

from eventsource import (
    AggregateRepository,
    InMemoryCheckpointRepository,
    InMemoryDLQRepository,
    InMemoryEventBus,
    InMemoryEventStore,
)
from eventsource.application.subscriptions import (
    ErrorCategory,
    ErrorHandlingConfig,
    ErrorSeverity,
    SubscriptionConfig,
    SubscriptionManager,
)

from .domain import PaymentAggregate
from .projection import (
    PaymentAnalyticsProjection,
    on_any_error,
    on_critical_error,
    on_transient_error,
)

logger = logging.getLogger(__name__)


async def main() -> None:
    """Demonstrate resilient subscription patterns."""
    print("=" * 60)
    print("Resilient Subscription Manager Example")
    print("Production-Ready Payment Analytics Projection")
    print("=" * 60)

    # -------------------------------------------------------------------------
    # Step 1: Set up infrastructure
    # -------------------------------------------------------------------------
    print("\n1. Setting up infrastructure...")

    event_store = InMemoryEventStore()
    event_bus = InMemoryEventBus()
    checkpoint_repo = InMemoryCheckpointRepository()
    dlq_repo = InMemoryDLQRepository()

    repo = AggregateRepository(
        event_store=event_store,
        aggregate_factory=PaymentAggregate,
        event_publisher=event_bus,
    )

    print("   - Event store: InMemoryEventStore")
    print("   - Event bus: InMemoryEventBus")
    print("   - Checkpoint repo: InMemoryCheckpointRepository")
    print("   - DLQ repo: InMemoryDLQRepository")

    # -------------------------------------------------------------------------
    # Step 2: Configure error handling
    # -------------------------------------------------------------------------
    print("\n2. Configuring error handling...")

    # ErrorHandlingConfig controls error tracking and DLQ behavior
    # Retry settings are configured in SubscriptionConfig
    error_handling_config = ErrorHandlingConfig(
        max_recent_errors=100,  # Keep last 100 errors in memory
        dlq_enabled=True,  # Send to DLQ after all retries fail
    )

    print("   Max recent errors: 100")
    print("   DLQ enabled: True")

    # -------------------------------------------------------------------------
    # Step 3: Create subscription manager with resilience features
    # -------------------------------------------------------------------------
    print("\n3. Creating SubscriptionManager with resilience features...")

    manager = SubscriptionManager(
        event_store=event_store,
        event_bus=event_bus,
        checkpoint_repo=checkpoint_repo,
        dlq_repo=dlq_repo,
        error_handling_config=error_handling_config,
        shutdown_timeout=10.0,  # 10 second graceful shutdown
        drain_timeout=5.0,  # 5 seconds to drain in-flight events
    )

    # Register error callbacks
    manager.on_error(on_any_error)
    manager.on_error_category(ErrorCategory.TRANSIENT, on_transient_error)
    manager.on_error_severity(ErrorSeverity.CRITICAL, on_critical_error)

    print("   Error callbacks registered")

    # -------------------------------------------------------------------------
    # Step 4: Create and subscribe the projection
    # -------------------------------------------------------------------------
    print("\n4. Creating PaymentAnalyticsProjection...")

    # Lower failure rates for demo to see some successful processing
    projection = PaymentAnalyticsProjection(
        failure_rate=0.2,  # 20% transient failures
        permanent_failure_rate=0.02,  # 2% permanent failures
    )

    config = SubscriptionConfig(
        start_from="beginning",
        batch_size=50,
        max_retries=3,
        initial_retry_delay=0.1,
        max_retry_delay=2.0,
        continue_on_error=True,  # Continue processing after DLQ
    )

    await manager.subscribe(projection, config=config, name="PaymentAnalytics")
    print("   Projection subscribed: PaymentAnalytics")

    # -------------------------------------------------------------------------
    # Step 5: Create test events
    # -------------------------------------------------------------------------
    print("\n5. Creating test events...")

    order_ids = [uuid4() for _ in range(10)]

    # Create successful payments
    for i, order_id in enumerate(order_ids[:7]):
        payment = repo.create_new(uuid4())
        payment.receive(order_id, 100.0 + i * 25.0)
        await repo.save(payment)

    # Create failed payment
    payment = repo.create_new(uuid4())
    payment.fail(order_ids[7], "Insufficient funds")
    await repo.save(payment)

    # Create refund
    payment = repo.create_new(uuid4())
    payment.receive(order_ids[8], 200.0)
    await repo.save(payment)

    refund_payment = repo.create_new(uuid4())
    refund_payment.refund(order_ids[8], 50.0, "Partial refund")
    await repo.save(refund_payment)

    print(f"   Created {10} test events")

    # -------------------------------------------------------------------------
    # Step 6: Start processing with graceful shutdown support
    # -------------------------------------------------------------------------
    print("\n6. Starting subscription manager...")
    print("   (Press Ctrl+C to trigger graceful shutdown)")

    # Set up signal handlers for graceful shutdown
    shutdown_event = asyncio.Event()

    def signal_handler(signum: int, frame: Any) -> None:
        logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        shutdown_event.set()

    # Register signal handlers (only works on Unix-like systems)
    if sys.platform != "win32":
        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)

    # Start the manager
    await manager.start()

    # Wait for events to be processed
    print("\n7. Processing events (with simulated failures)...")
    await asyncio.sleep(2.0)

    # -------------------------------------------------------------------------
    # Step 7: Check health and error stats
    # -------------------------------------------------------------------------
    print("\n8. Health and error statistics:")

    # Get comprehensive health
    health = manager.get_comprehensive_health()
    print(f"   Overall status: {health.get('status', 'unknown')}")
    print(f"   Running: {health.get('running', False)}")

    # Get error stats
    error_stats = manager.get_error_stats()
    print(f"   Total errors: {error_stats.get('total_errors', 0)}")
    print(f"   DLQ count: {error_stats.get('total_dlq_count', 0)}")

    # Get projection stats
    stats = projection.get_stats()
    print("\n   Projection Stats:")
    print(f"   - Total revenue: ${stats['total_revenue']:.2f}")
    print(f"   - Total refunds: ${stats['total_refunds']:.2f}")
    print(f"   - Net revenue: ${stats['net_revenue']:.2f}")
    print(f"   - Payments: {stats['payment_count']}")
    print(f"   - Events processed: {stats['events_processed']}")
    print(f"   - Circuit breaker: {stats['circuit_breaker_state']}")

    # -------------------------------------------------------------------------
    # Step 8: Check DLQ for failed events
    # -------------------------------------------------------------------------
    print("\n9. Checking Dead Letter Queue...")

    dlq_entries = await dlq_repo.get_failed_events()
    print(f"   DLQ entries: {len(dlq_entries)}")

    for entry in dlq_entries[:3]:  # Show first 3
        print(f"   - Event {entry.event_id}: {entry.error_message[:50]}...")

    dlq_stats = await dlq_repo.get_failure_stats()
    print(f"   DLQ stats: {dlq_stats}")

    # -------------------------------------------------------------------------
    # Step 9: Demonstrate manual DLQ resolution
    # -------------------------------------------------------------------------
    if dlq_entries:
        print("\n10. Demonstrating DLQ resolution...")

        # In production, you would:
        # 1. Investigate the failure
        # 2. Fix the underlying issue
        # 3. Either replay the event or mark as resolved

        # Mark the first entry as resolved
        first_entry = dlq_entries[0]
        await dlq_repo.mark_resolved(first_entry.id, "admin-user")
        print(f"   Marked DLQ entry {first_entry.id} as resolved")

    # -------------------------------------------------------------------------
    # Step 10: Graceful shutdown
    # -------------------------------------------------------------------------
    print("\n11. Initiating graceful shutdown...")

    # Request shutdown
    manager.request_shutdown()

    # Wait for shutdown to complete
    await manager.stop()

    # Verify final checkpoint
    checkpoint = await checkpoint_repo.get_checkpoint("PaymentAnalytics")
    if checkpoint:
        print(f"   Final checkpoint position: {checkpoint}")

    print("\n" + "=" * 60)
    print("Example completed successfully!")
    print("=" * 60)
    print("\nKey Takeaways:")
    print("- Transient errors are retried with exponential backoff")
    print("- Permanent errors are sent to the Dead Letter Queue")
    print("- Circuit breaker prevents cascading failures")
    print("- Graceful shutdown ensures no event loss")
    print("- Health checks enable monitoring integration")
