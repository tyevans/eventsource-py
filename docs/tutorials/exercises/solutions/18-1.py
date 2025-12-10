"""
Tutorial 18 - Exercise 1 Solution: High-Throughput Event Pipeline

This solution demonstrates building a sensor data pipeline with Kafka,
including throughput monitoring and metrics aggregation.

Prerequisites:
- Kafka running on localhost:9092
- pip install eventsource-py[kafka]

Run with: python 18-1.py
"""

import asyncio
import contextlib
import time
from uuid import uuid4

from eventsource import DomainEvent, default_registry, register_event
from eventsource.bus.kafka import (
    KAFKA_AVAILABLE,
    KafkaEventBus,
    KafkaEventBusConfig,
)

if not KAFKA_AVAILABLE:
    print("Kafka not available. Install with: pip install eventsource-py[kafka]")
    exit(1)


# =============================================================================
# Step 1: Define a SensorReading event
# =============================================================================


@register_event
class SensorReading(DomainEvent):
    """
    Event representing a sensor reading.

    In a real IoT system, this might include:
    - Sensor ID and location
    - Multiple measurement values
    - Reading timestamp from the sensor
    - Quality/confidence indicators
    """

    event_type: str = "SensorReading"
    aggregate_type: str = "Sensor"
    temperature: float
    humidity: float


# =============================================================================
# Step 2: Create MetricsAggregator handler
# =============================================================================


class MetricsAggregator:
    """
    Aggregates metrics from sensor readings.

    This handler demonstrates:
    - Counting events processed
    - Computing running aggregates
    - Thread-safe operation (single-threaded async)

    In production, you would likely:
    - Persist aggregates to a database
    - Use time-windowed aggregations
    - Implement checkpointing for recovery
    """

    def __init__(self):
        self.count = 0
        self.total_temp = 0.0
        self.min_temp = float("inf")
        self.max_temp = float("-inf")
        self.total_humidity = 0.0

    async def handle(self, event: DomainEvent) -> None:
        """Handle a domain event."""
        if isinstance(event, SensorReading):
            self.count += 1
            self.total_temp += event.temperature
            self.total_humidity += event.humidity
            self.min_temp = min(self.min_temp, event.temperature)
            self.max_temp = max(self.max_temp, event.temperature)

    @property
    def avg_temperature(self) -> float:
        """Calculate average temperature."""
        return self.total_temp / max(self.count, 1)

    @property
    def avg_humidity(self) -> float:
        """Calculate average humidity."""
        return self.total_humidity / max(self.count, 1)

    def get_stats(self) -> dict:
        """Get all statistics as a dictionary."""
        return {
            "count": self.count,
            "avg_temperature": self.avg_temperature,
            "min_temperature": self.min_temp if self.count > 0 else None,
            "max_temperature": self.max_temp if self.count > 0 else None,
            "avg_humidity": self.avg_humidity,
        }


# =============================================================================
# Main Exercise
# =============================================================================


async def main():
    print("=" * 60)
    print("Exercise 18-1: High-Throughput Event Pipeline")
    print("=" * 60)
    print()

    # -------------------------------------------------------------------------
    # Step 3: Configure Kafka bus with metrics enabled
    # -------------------------------------------------------------------------
    print("Step 1: Configuring Kafka bus with metrics...")

    config = KafkaEventBusConfig(
        bootstrap_servers="localhost:9092",
        topic_prefix="exercise18",
        consumer_group="sensor-aggregator",
        # Performance tuning for throughput
        batch_size=32768,  # 32KB batches
        linger_ms=10,  # Wait 10ms for more messages
        compression_type="gzip",  # Compress for network efficiency
        # Observability
        enable_metrics=True,
        enable_tracing=True,
    )

    bus = KafkaEventBus(config=config, event_registry=default_registry)
    print(f"   Topic: {config.topic_name}")
    print(f"   Consumer group: {config.consumer_group}")
    print(f"   Metrics enabled: {config.enable_metrics}")

    # -------------------------------------------------------------------------
    # Step 4: Create bus and subscribe handler
    # -------------------------------------------------------------------------
    aggregator = MetricsAggregator()
    bus.subscribe(SensorReading, aggregator.handle)
    print("   Handler subscribed!")

    try:
        # Connect to Kafka
        print("\nConnecting to Kafka...")
        await bus.connect()
        print("Connected!")

        # -------------------------------------------------------------------------
        # Step 5: Publish 1000 events
        # -------------------------------------------------------------------------
        num_events = 1000
        print(f"\nStep 2: Publishing {num_events} events...")

        start = time.perf_counter()

        # Generate sensor readings with realistic variation
        events = [
            SensorReading(
                aggregate_id=uuid4(),
                temperature=20.0 + (i % 10) + (i % 7) * 0.5,  # 20-29.5 C
                humidity=50.0 + (i % 20) - 5,  # 45-65%
                aggregate_version=1,
            )
            for i in range(num_events)
        ]

        # Publish in batches for efficiency
        batch_size = 100
        for i in range(0, num_events, batch_size):
            batch = events[i : i + batch_size]
            await bus.publish(batch)
            if (i + batch_size) % 500 == 0:
                print(f"   Published {i + batch_size} events...")

        publish_time = time.perf_counter() - start
        throughput = num_events / publish_time

        print(f"\n   Published {num_events} events in {publish_time:.3f}s")
        print(f"   Throughput: {throughput:.0f} events/sec")

        # -------------------------------------------------------------------------
        # Step 6: Consume and display results
        # -------------------------------------------------------------------------
        print("\nStep 3: Consuming events...")

        # Start consuming in background
        consume_task = bus.start_consuming_in_background()

        # Wait for events to be processed
        consume_start = time.perf_counter()
        timeout = 10.0  # Maximum wait time
        check_interval = 0.5

        while time.perf_counter() - consume_start < timeout:
            await asyncio.sleep(check_interval)
            progress = (aggregator.count / num_events) * 100
            print(f"   Progress: {aggregator.count}/{num_events} ({progress:.1f}%)")
            if aggregator.count >= num_events:
                break

        consume_time = time.perf_counter() - consume_start

        # Stop consuming
        await bus.stop_consuming()
        consume_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await consume_task

        # -------------------------------------------------------------------------
        # Display Results
        # -------------------------------------------------------------------------
        print("\n" + "=" * 60)
        print("Step 4: Results")
        print("=" * 60)

        print("\nThroughput Metrics:")
        print(f"   Events published: {num_events}")
        print(f"   Publish time: {publish_time:.3f}s")
        print(f"   Publish throughput: {throughput:.0f} events/sec")
        print(f"   Consume time: {consume_time:.3f}s")

        print("\nAggregation Results:")
        stats = aggregator.get_stats()
        print(f"   Events processed: {stats['count']}")
        print(f"   Avg temperature: {stats['avg_temperature']:.2f}C")
        print(f"   Min temperature: {stats['min_temperature']:.2f}C")
        print(f"   Max temperature: {stats['max_temperature']:.2f}C")
        print(f"   Avg humidity: {stats['avg_humidity']:.2f}%")

        print("\nKafka Bus Statistics:")
        bus_stats = bus.get_stats_dict()
        print(f"   Events published: {bus_stats['events_published']}")
        print(f"   Events consumed: {bus_stats['events_consumed']}")
        print(f"   Successfully processed: {bus_stats['events_processed_success']}")
        print(f"   Handler errors: {bus_stats['handler_errors']}")

        # Verify success
        success = aggregator.count >= num_events * 0.95  # Allow 5% tolerance
        if success:
            print("\n" + "=" * 60)
            print("SUCCESS! Exercise completed.")
            print("=" * 60)
        else:
            print("\n" + "=" * 60)
            print(f"PARTIAL: Only {aggregator.count}/{num_events} events processed.")
            print("Try increasing the consume timeout.")
            print("=" * 60)

    except Exception as e:
        print(f"\nError: {e}")
        print("\nMake sure Kafka is running:")
        print("  docker compose -f docker-compose.kafka.yml up -d")

    finally:
        await bus.disconnect()
        print("\nDisconnected from Kafka")


if __name__ == "__main__":
    asyncio.run(main())
