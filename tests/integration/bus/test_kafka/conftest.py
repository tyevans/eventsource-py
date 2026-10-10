"""Fixtures and testcontainer configuration for Kafka integration tests."""

from __future__ import annotations

from collections.abc import AsyncGenerator, Generator
from typing import Any
from uuid import uuid4

import pytest
import pytest_asyncio

from eventsource import (
    KAFKA_AVAILABLE,
    EventRegistry,
    KafkaEventBus,
    KafkaEventBusConfig,
)
from tests.integration.conftest import (
    DOCKER_AVAILABLE,
    TESTCONTAINERS_AVAILABLE,
    TestItemCreated,
    TestItemUpdated,
    TestOrderCreated,
)

# ============================================================================
# Testcontainers Detection
# ============================================================================

KAFKA_TESTCONTAINERS_AVAILABLE = False

try:
    from testcontainers.kafka import KafkaContainer  # type: ignore[import-untyped]

    KAFKA_TESTCONTAINERS_AVAILABLE = True
except ImportError:
    KafkaContainer = None


# ============================================================================
# Skip Conditions
# ============================================================================

skip_if_no_kafka_infra = pytest.mark.skipif(
    not (
        TESTCONTAINERS_AVAILABLE
        and DOCKER_AVAILABLE
        and KAFKA_AVAILABLE
        and KAFKA_TESTCONTAINERS_AVAILABLE
    ),
    reason="Kafka test infrastructure not available (requires testcontainers-kafka, docker, and aiokafka)",
)


# ============================================================================
# Kafka Container Fixture
# ============================================================================


@pytest.fixture(scope="session")
def kafka_container() -> Generator[Any]:
    """
    Provide Kafka container for integration tests.

    Uses testcontainers to automatically start and stop a Kafka container.
    Container is shared across all tests in the session for efficiency.
    """
    if not KAFKA_TESTCONTAINERS_AVAILABLE or not DOCKER_AVAILABLE:
        pytest.skip("Kafka testcontainer not available")

    container = KafkaContainer("confluentinc/cp-kafka:7.5.0")
    container.start()

    # Give Kafka time to fully initialize
    import time

    time.sleep(5)

    yield container

    container.stop()


@pytest.fixture(scope="session")
def kafka_bootstrap_servers(kafka_container: Any) -> str:
    """Get Kafka bootstrap servers from container."""
    return str(kafka_container.get_bootstrap_server())


@pytest.fixture
def kafka_event_bus_factory(
    kafka_bootstrap_servers: str,
) -> Any:
    """
    Factory fixture for creating Kafka event bus instances.

    Returns a factory function that creates a new, unconnected KafkaEventBus.
    Each test is responsible for calling connect() and disconnect().
    """

    def create_bus(
        topic_prefix: str | None = None,
        consumer_group: str | None = None,
        enable_dlq: bool = True,
        max_retries: int = 3,
        retry_base_delay: float = 0.1,
    ) -> KafkaEventBus:
        registry = EventRegistry()
        registry.register(TestItemCreated)
        registry.register(TestItemUpdated)
        registry.register(TestOrderCreated)

        # Use unique topic prefix and consumer group for test isolation
        unique_suffix = uuid4().hex[:8]
        config = KafkaEventBusConfig(
            bootstrap_servers=kafka_bootstrap_servers,
            topic_prefix=topic_prefix or f"test_{unique_suffix}",
            consumer_group=consumer_group or f"test_group_{unique_suffix}",
            enable_dlq=enable_dlq,
            max_retries=max_retries,
            retry_base_delay=retry_base_delay,
            retry_max_delay=1.0,  # Fast retries for tests
            enable_tracing=False,  # Disable tracing for tests
        )

        return KafkaEventBus(config=config, event_registry=registry)

    return create_bus


@pytest_asyncio.fixture
async def kafka_event_bus(
    kafka_event_bus_factory: Any,
) -> AsyncGenerator[KafkaEventBus]:
    """
    Provide Kafka event bus for integration tests.

    Creates a fresh bus instance, connects it within the test's event loop,
    and ensures proper cleanup after the test.
    """
    bus = kafka_event_bus_factory()
    await bus.connect()

    yield bus

    # Clean up: stop consuming if active and disconnect
    if bus.is_consuming:
        await bus.stop_consuming()
    if bus.is_connected:
        await bus.disconnect()


@pytest.fixture
def sample_customer_id() -> Any:
    """Provide a sample customer ID."""
    return uuid4()


__all__ = [
    "KAFKA_TESTCONTAINERS_AVAILABLE",
    "kafka_bootstrap_servers",
    "kafka_container",
    "kafka_event_bus",
    "kafka_event_bus_factory",
    "sample_customer_id",
    "skip_if_no_kafka_infra",
]
