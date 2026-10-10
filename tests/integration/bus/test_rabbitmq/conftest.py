"""Shared fixtures and configuration for RabbitMQ integration tests."""

from __future__ import annotations

from collections.abc import AsyncGenerator, Generator
from typing import Any
from uuid import uuid4

import pytest
import pytest_asyncio

from eventsource import (
    RABBITMQ_AVAILABLE,
    EventRegistry,
    RabbitMQEventBus,
    RabbitMQEventBusConfig,
)
from tests.integration.conftest import (
    DOCKER_AVAILABLE,
    TESTCONTAINERS_AVAILABLE,
    TestItemCreated,
    TestItemUpdated,
    TestOrderCreated,
)

skip_if_no_rabbitmq_infra = pytest.mark.skipif(
    not (TESTCONTAINERS_AVAILABLE and DOCKER_AVAILABLE and RABBITMQ_AVAILABLE),
    reason="RabbitMQ test infrastructure not available (requires testcontainers, docker, and aio-pika)",
)


@pytest.fixture(scope="session")
def rabbitmq_container() -> Generator[Any]:
    """
    Provide RabbitMQ container for integration tests.

    Uses testcontainers to automatically start and stop a RabbitMQ container.
    Container is shared across all tests in the session for efficiency.
    """
    if not TESTCONTAINERS_AVAILABLE or not DOCKER_AVAILABLE:
        pytest.skip("RabbitMQ testcontainer not available")

    from testcontainers.core.container import DockerContainer  # type: ignore[import-untyped]
    from testcontainers.core.waiting_utils import wait_for_logs  # type: ignore[import-untyped]

    # Use RabbitMQ with management plugin for additional debugging capability
    container = DockerContainer("rabbitmq:3-management")
    container.with_exposed_ports(5672, 15672)
    container.with_env("RABBITMQ_DEFAULT_USER", "guest")
    container.with_env("RABBITMQ_DEFAULT_PASS", "guest")
    container.start()

    # Wait for RabbitMQ to be ready
    wait_for_logs(container, "started TCP listener on", timeout=60)

    yield container

    container.stop()


@pytest.fixture(scope="session")
def rabbitmq_connection_url(rabbitmq_container: Any) -> str:
    """Get RabbitMQ connection URL from container."""
    host = rabbitmq_container.get_container_host_ip()
    port = rabbitmq_container.get_exposed_port(5672)
    return f"amqp://guest:guest@{host}:{port}/"


@pytest.fixture
def rabbitmq_event_bus_factory(
    rabbitmq_connection_url: str,
) -> Any:
    """
    Factory fixture for creating RabbitMQ event bus instances.

    Returns a factory function that creates a new, unconnected RabbitMQEventBus.
    Each test is responsible for calling connect() and disconnect().
    """

    def create_bus(
        exchange_name: str = "test_events",
        consumer_group: str = "test_group",
        enable_dlq: bool = True,
        max_retries: int = 3,
    ) -> RabbitMQEventBus:
        registry = EventRegistry()
        registry.register(TestItemCreated)
        registry.register(TestItemUpdated)
        registry.register(TestOrderCreated)

        config = RabbitMQEventBusConfig(
            rabbitmq_url=rabbitmq_connection_url,
            exchange_name=exchange_name,
            consumer_group=consumer_group,
            prefetch_count=10,
            enable_dlq=enable_dlq,
            max_retries=max_retries,
            durable=False,  # Use non-durable for tests to allow cleanup
            auto_delete=True,  # Auto-delete for test isolation
        )

        return RabbitMQEventBus(config=config, event_registry=registry)

    return create_bus


@pytest_asyncio.fixture
async def rabbitmq_event_bus(
    rabbitmq_event_bus_factory: Any,
) -> AsyncGenerator[RabbitMQEventBus]:
    """
    Provide RabbitMQ event bus for integration tests.

    Creates a fresh bus instance, connects it within the test's event loop,
    and ensures proper cleanup after the test.
    """
    # Use unique exchange name to avoid conflicts between tests
    unique_suffix = str(uuid4())[:8]
    bus = rabbitmq_event_bus_factory(
        exchange_name=f"test_events_{unique_suffix}",
        consumer_group=f"test_group_{unique_suffix}",
    )
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
