"""
Tutorial 19 - Exercise 1 Solution: Event-Driven Microservices

This solution demonstrates building a microservices communication layer
with RabbitMQ using topic-based routing for event distribution.

Run with: python docs/tutorials/exercises/solutions/19-1.py

Prerequisites:
- RabbitMQ running on localhost:5672
- pip install eventsource-py[rabbitmq]
"""

import asyncio
import contextlib
from uuid import uuid4

from eventsource import DomainEvent, default_registry, register_event
from eventsource.bus.rabbitmq import (
    RABBITMQ_AVAILABLE,
    RabbitMQEventBus,
    RabbitMQEventBusConfig,
)

if not RABBITMQ_AVAILABLE:
    print("Install rabbitmq: pip install eventsource-py[rabbitmq]")
    exit(1)


# =============================================================================
# Step 1: Define events for different domains
# =============================================================================


@register_event
class CustomerCreated(DomainEvent):
    """Event emitted when a new customer is registered."""

    event_type: str = "CustomerCreated"
    aggregate_type: str = "Customer"
    name: str
    email: str


@register_event
class OrderCreated(DomainEvent):
    """Event emitted when a new order is placed."""

    event_type: str = "OrderCreated"
    aggregate_type: str = "Order"
    customer_id: str
    total: float


@register_event
class PaymentReceived(DomainEvent):
    """Event emitted when payment is received for an order."""

    event_type: str = "PaymentReceived"
    aggregate_type: str = "Payment"
    order_id: str
    amount: float


# =============================================================================
# Step 2: Create service handlers
# =============================================================================


class CustomerService:
    """
    Handles customer-related events.

    In a real microservices architecture, this would be a separate service
    that maintains customer data and responds to customer events.
    """

    def __init__(self):
        self.customers: dict[str, str] = {}

    async def handle(self, event: DomainEvent) -> None:
        if isinstance(event, CustomerCreated):
            self.customers[str(event.aggregate_id)] = event.name
            print(f"  [CustomerService] Registered customer: {event.name} ({event.email})")


class OrderService:
    """
    Handles order-related events.

    Maintains order state and tracks all orders in the system.
    """

    def __init__(self):
        self.orders: list[dict] = []

    async def handle(self, event: DomainEvent) -> None:
        if isinstance(event, OrderCreated):
            self.orders.append(
                {
                    "id": str(event.aggregate_id),
                    "customer_id": event.customer_id,
                    "total": event.total,
                }
            )
            print(
                f"  [OrderService] New order ${event.total:.2f} for customer {event.customer_id[:8]}..."
            )


class PaymentService:
    """
    Handles payment-related events.

    Tracks payments and can reconcile them with orders.
    """

    def __init__(self):
        self.payments: list[dict] = []

    async def handle(self, event: DomainEvent) -> None:
        if isinstance(event, PaymentReceived):
            self.payments.append(
                {
                    "id": str(event.aggregate_id),
                    "order_id": event.order_id,
                    "amount": event.amount,
                }
            )
            print(
                f"  [PaymentService] Payment ${event.amount:.2f} received for order {event.order_id[:8]}..."
            )


class NotificationService:
    """
    Handles all events for notification purposes.

    This service demonstrates wildcard subscription - it receives
    ALL events and can send appropriate notifications.
    """

    def __init__(self):
        self.notifications: list[str] = []

    async def handle(self, event: DomainEvent) -> None:
        notification = f"Event: {event.event_type} at {event.occurred_at}"
        self.notifications.append(notification)
        print(f"  [NotificationService] {event.event_type} notification queued")


# =============================================================================
# Main Application
# =============================================================================


async def main():
    print("=" * 60)
    print("Exercise 19-1: Event-Driven Microservices with RabbitMQ")
    print("=" * 60)
    print()

    # -------------------------------------------------------------------------
    # Step 3: Configure RabbitMQ with topic exchange
    # -------------------------------------------------------------------------
    print("Step 1: Configuring RabbitMQ event bus...")

    config = RabbitMQEventBusConfig(
        rabbitmq_url="amqp://guest:guest@localhost:5672/",
        exchange_name="microservices",
        exchange_type="topic",  # Topic exchange for pattern-based routing
        consumer_group="exercise19",
        prefetch_count=10,
        enable_dlq=True,
        max_retries=3,
    )

    bus = RabbitMQEventBus(config=config, event_registry=default_registry)

    print(f"   Exchange: {config.exchange_name} (type: {config.exchange_type})")
    print(f"   Queue: {config.queue_name}")
    print(f"   Consumer: {config.consumer_name}")
    print()

    # -------------------------------------------------------------------------
    # Step 4: Create and subscribe service handlers
    # -------------------------------------------------------------------------
    print("Step 2: Setting up microservice handlers...")

    customer_svc = CustomerService()
    order_svc = OrderService()
    payment_svc = PaymentService()
    notification_svc = NotificationService()

    # Each service subscribes to its relevant event types
    # With topic exchange, routing keys are: {aggregate_type}.{event_type}
    # E.g., "Customer.CustomerCreated", "Order.OrderCreated"

    bus.subscribe(CustomerCreated, customer_svc)
    bus.subscribe(OrderCreated, order_svc)
    bus.subscribe(PaymentReceived, payment_svc)

    # Notification service receives ALL events (wildcard)
    bus.subscribe_to_all_events(notification_svc)

    print("   CustomerService -> CustomerCreated")
    print("   OrderService -> OrderCreated")
    print("   PaymentService -> PaymentReceived")
    print("   NotificationService -> ALL events (wildcard)")
    print()

    try:
        # Connect to RabbitMQ
        print("Step 3: Connecting to RabbitMQ...")
        await bus.connect()
        print(f"   Connected: {bus.is_connected}")
        print()

        # ---------------------------------------------------------------------
        # Step 5: Publish events from different domains
        # ---------------------------------------------------------------------
        print("Step 4: Publishing domain events...")

        # Create a customer
        customer_id = uuid4()
        await bus.publish(
            [
                CustomerCreated(
                    aggregate_id=customer_id,
                    name="Alice Smith",
                    email="alice@example.com",
                    aggregate_version=1,
                )
            ]
        )
        print("   Published: CustomerCreated (Customer.CustomerCreated)")

        # Create an order for the customer
        order_id = uuid4()
        await bus.publish(
            [
                OrderCreated(
                    aggregate_id=order_id,
                    customer_id=str(customer_id),
                    total=199.99,
                    aggregate_version=1,
                )
            ]
        )
        print("   Published: OrderCreated (Order.OrderCreated)")

        # Receive payment for the order
        payment_id = uuid4()
        await bus.publish(
            [
                PaymentReceived(
                    aggregate_id=payment_id,
                    order_id=str(order_id),
                    amount=199.99,
                    aggregate_version=1,
                )
            ]
        )
        print("   Published: PaymentReceived (Payment.PaymentReceived)")
        print()

        # Consume events
        print("Step 5: Consuming events...")
        print()

        consume_task = asyncio.create_task(bus.start_consuming())
        await asyncio.sleep(3)  # Wait for message processing

        await bus.stop_consuming()
        consume_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await consume_task

        # ---------------------------------------------------------------------
        # Step 6: Verify service isolation and routing
        # ---------------------------------------------------------------------
        print()
        print("Step 6: Verifying service isolation...")
        print()
        print("   Service Results:")
        print(f"      CustomerService: {len(customer_svc.customers)} customer(s)")
        print(f"      OrderService: {len(order_svc.orders)} order(s)")
        print(f"      PaymentService: {len(payment_svc.payments)} payment(s)")
        print(f"      NotificationService: {len(notification_svc.notifications)} notification(s)")
        print()

        # Verify each service received only its events
        assert len(customer_svc.customers) == 1, "CustomerService should have 1 customer"
        assert len(order_svc.orders) == 1, "OrderService should have 1 order"
        assert len(payment_svc.payments) == 1, "PaymentService should have 1 payment"
        assert len(notification_svc.notifications) == 3, (
            "NotificationService should have 3 notifications"
        )

        print("   All assertions passed!")
        print()

        # Show statistics
        stats = bus.get_stats_dict()
        print("   Bus Statistics:")
        print(f"      Events published: {stats['events_published']}")
        print(f"      Events consumed: {stats['events_consumed']}")
        print(f"      Processed successfully: {stats['events_processed_success']}")
        print(f"      Processing failures: {stats['events_processed_failed']}")

    except Exception as e:
        print(f"\nError: {e}")
        print("\nMake sure RabbitMQ is running:")
        print("  docker run -d -p 5672:5672 -p 15672:15672 rabbitmq:3-management")
        raise

    finally:
        await bus.disconnect()
        print()
        print("   Disconnected from RabbitMQ")

    print()
    print("=" * 60)
    print("Exercise 19-1 Complete!")
    print("=" * 60)
    print()
    print("Key Learnings:")
    print("  1. Topic exchanges route based on routing key patterns")
    print("  2. Each service subscribes only to its relevant events")
    print("  3. Wildcard handlers receive ALL events for cross-cutting concerns")
    print("  4. Consumer groups enable independent event processing")
    print("  5. The same event can be delivered to multiple handlers")


if __name__ == "__main__":
    asyncio.run(main())
