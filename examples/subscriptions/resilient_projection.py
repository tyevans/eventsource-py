"""
Resilient Subscription Manager Example - Production-Ready Projection

This example demonstrates production-ready patterns:
- Retry with exponential backoff for transient failures
- Circuit breaker to prevent cascading failures
- Dead Letter Queue (DLQ) for permanently failed events
- Graceful shutdown with signal handling (SIGTERM, SIGINT)
- Health monitoring and error tracking
- Custom error callbacks for alerting

This is the pattern you should use for production deployments.

Run with: python -m examples.subscriptions.resilient_projection
"""

import asyncio
import logging
import random
import signal
import sys
from uuid import UUID, uuid4

from pydantic import BaseModel

from eventsource import (
    AggregateRepository,
    AggregateRoot,
    DomainEvent,
    InMemoryCheckpointRepository,
    InMemoryDLQRepository,
    InMemoryEventBus,
    InMemoryEventStore,
    register_event,
)
from eventsource.subscriptions import (
    CircuitBreaker,
    CircuitBreakerConfig,
    ErrorCategory,
    ErrorHandlingConfig,
    ErrorInfo,
    ErrorSeverity,
    SubscriptionConfig,
    SubscriptionManager,
)

# =============================================================================
# Configure Logging
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


# =============================================================================
# Domain Events
# =============================================================================


@register_event
class PaymentReceived(DomainEvent):
    """Event emitted when a payment is received."""

    event_type: str = "PaymentReceived"
    aggregate_type: str = "Payment"

    order_id: UUID
    amount: float
    currency: str = "USD"


@register_event
class PaymentFailed(DomainEvent):
    """Event emitted when a payment fails."""

    event_type: str = "PaymentFailed"
    aggregate_type: str = "Payment"

    order_id: UUID
    reason: str


@register_event
class PaymentRefunded(DomainEvent):
    """Event emitted when a payment is refunded."""

    event_type: str = "PaymentRefunded"
    aggregate_type: str = "Payment"

    order_id: UUID
    amount: float
    reason: str


# =============================================================================
# Aggregate
# =============================================================================


class PaymentState(BaseModel):
    """Payment aggregate state."""

    payment_id: UUID
    order_id: UUID | None = None
    amount: float = 0.0
    status: str = "pending"


class PaymentAggregate(AggregateRoot[PaymentState]):
    """Event-sourced payment aggregate."""

    aggregate_type = "Payment"

    def _get_initial_state(self) -> PaymentState:
        return PaymentState(payment_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, PaymentReceived):
            self._state = PaymentState(
                payment_id=self.aggregate_id,
                order_id=event.order_id,
                amount=event.amount,
                status="received",
            )
        elif isinstance(event, PaymentFailed):
            if self._state:
                self._state = self._state.model_copy(update={"status": "failed"})
        elif isinstance(event, PaymentRefunded) and self._state:
            self._state = self._state.model_copy(update={"status": "refunded"})

    def receive(self, order_id: UUID, amount: float) -> None:
        """Record a payment receipt."""
        event = PaymentReceived(
            aggregate_id=self.aggregate_id,
            order_id=order_id,
            amount=amount,
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)

    def fail(self, order_id: UUID, reason: str) -> None:
        """Record a payment failure."""
        event = PaymentFailed(
            aggregate_id=self.aggregate_id,
            order_id=order_id,
            reason=reason,
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)

    def refund(self, order_id: UUID, amount: float, reason: str) -> None:
        """Record a payment refund."""
        event = PaymentRefunded(
            aggregate_id=self.aggregate_id,
            order_id=order_id,
            amount=amount,
            reason=reason,
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)


# =============================================================================
# Resilient Payment Analytics Projection
# =============================================================================


class PaymentAnalyticsProjection:
    """
    Production-ready projection with built-in resilience.

    This projection demonstrates:
    - Simulated transient failures that trigger retry logic
    - Simulated permanent failures that go to DLQ
    - Revenue tracking with error handling
    """

    def __init__(
        self,
        failure_rate: float = 0.3,  # 30% transient failure rate
        permanent_failure_rate: float = 0.05,  # 5% permanent failure rate
    ):
        """
        Initialize the projection.

        Args:
            failure_rate: Probability of transient failure (for demo)
            permanent_failure_rate: Probability of permanent failure (for demo)
        """
        self.failure_rate = failure_rate
        self.permanent_failure_rate = permanent_failure_rate

        # Read model
        self.total_revenue: float = 0.0
        self.total_refunds: float = 0.0
        self.payment_count: int = 0
        self.failed_payment_count: int = 0
        self.events_processed: int = 0

        # Circuit breaker for external service calls
        self._circuit_breaker = CircuitBreaker(
            config=CircuitBreakerConfig(
                failure_threshold=3,  # Open after 3 failures
                recovery_timeout=10.0,  # Try recovery after 10 seconds
            )
        )

    def subscribed_to(self) -> list[type[DomainEvent]]:
        """Declare which event types this projection handles."""
        return [PaymentReceived, PaymentFailed, PaymentRefunded]

    async def handle(self, event: DomainEvent) -> None:
        """
        Process a single event with simulated failures.

        In production, failures would come from:
        - Database connection errors
        - External API failures
        - Network timeouts
        """
        self.events_processed += 1

        # Simulate permanent failure (e.g., data validation error)
        if random.random() < self.permanent_failure_rate:
            raise ValueError(
                f"Data validation error for event {event.event_id} - this error cannot be retried"
            )

        # Simulate transient failure (e.g., network timeout)
        if random.random() < self.failure_rate:
            raise ConnectionError(f"Transient network error processing event {event.event_id}")

        # Process the event using circuit breaker
        await self._process_with_circuit_breaker(event)

    async def _process_with_circuit_breaker(self, event: DomainEvent) -> None:
        """Process event through circuit breaker."""

        async def process():
            await self._update_read_model(event)

        await self._circuit_breaker.execute(process, "update_read_model")

    async def _update_read_model(self, event: DomainEvent) -> None:
        """Update the in-memory read model."""
        if isinstance(event, PaymentReceived):
            self.total_revenue += event.amount
            self.payment_count += 1
            logger.info(
                f"Payment received: ${event.amount:.2f}, total revenue=${self.total_revenue:.2f}"
            )

        elif isinstance(event, PaymentFailed):
            self.failed_payment_count += 1
            logger.info(
                f"Payment failed: {event.reason}, total failures={self.failed_payment_count}"
            )

        elif isinstance(event, PaymentRefunded):
            self.total_refunds += event.amount
            logger.info(
                f"Payment refunded: ${event.amount:.2f}, total refunds=${self.total_refunds:.2f}"
            )

    def get_stats(self) -> dict:
        """Get analytics statistics."""
        return {
            "total_revenue": self.total_revenue,
            "total_refunds": self.total_refunds,
            "net_revenue": self.total_revenue - self.total_refunds,
            "payment_count": self.payment_count,
            "failed_payment_count": self.failed_payment_count,
            "events_processed": self.events_processed,
            "circuit_breaker_state": self._circuit_breaker.state.value,
        }


# =============================================================================
# Error Callback Functions
# =============================================================================
# These would typically send alerts to monitoring systems


async def on_any_error(error_info: ErrorInfo) -> None:
    """Called for any error - log to monitoring system."""
    logger.warning(f"Error notification: {error_info.error_type} - {error_info.error_message}")


async def on_transient_error(error_info: ErrorInfo) -> None:
    """Called for transient errors - these will be retried."""
    logger.info(
        f"Transient error (will retry): {error_info.error_message}, "
        f"attempt {error_info.retry_count + 1}"
    )


async def on_critical_error(error_info: ErrorInfo) -> None:
    """Called for critical errors - send alert!"""
    logger.error(
        f"CRITICAL ERROR - ALERT REQUIRED: {error_info.error_type} - {error_info.error_message}"
    )
    # In production: await send_pagerduty_alert(error_info)


# =============================================================================
# Main Demo
# =============================================================================


async def main():
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
        aggregate_type="Payment",
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

    def signal_handler(signum, frame):
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


if __name__ == "__main__":
    asyncio.run(main())
