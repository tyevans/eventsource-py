"""Container detection, docker availability, and session container fixtures."""

from __future__ import annotations

import subprocess
from collections.abc import Generator
from typing import Any

import pytest

TESTCONTAINERS_AVAILABLE = False
POSTGRES_CONTAINER = None
REDIS_CONTAINER = None

try:
    from testcontainers.postgres import PostgresContainer  # type: ignore[import-untyped]
    from testcontainers.redis import RedisContainer  # type: ignore[import-untyped]

    TESTCONTAINERS_AVAILABLE = True
except ImportError:
    PostgresContainer = None
    RedisContainer = None


def is_docker_available() -> bool:
    """Check if Docker is available for running containers."""
    try:
        result = subprocess.run(
            ["docker", "info"],
            capture_output=True,
            timeout=5,
        )
        return result.returncode == 0
    except (subprocess.TimeoutExpired, FileNotFoundError, Exception):
        return False


DOCKER_AVAILABLE = is_docker_available()

skip_if_no_testcontainers = pytest.mark.skipif(
    not TESTCONTAINERS_AVAILABLE, reason="testcontainers not installed (pip install testcontainers)"
)

skip_if_no_docker = pytest.mark.skipif(
    not DOCKER_AVAILABLE, reason="Docker not available or not running"
)

skip_if_no_postgres_infra = pytest.mark.skipif(
    not (TESTCONTAINERS_AVAILABLE and DOCKER_AVAILABLE),
    reason="PostgreSQL test infrastructure not available",
)

skip_if_no_redis_infra = pytest.mark.skipif(
    not (TESTCONTAINERS_AVAILABLE and DOCKER_AVAILABLE),
    reason="Redis test infrastructure not available",
)


@pytest.fixture(scope="session")
def postgres_container() -> Generator[Any]:
    """
    Provide PostgreSQL container for integration tests.

    Uses testcontainers to automatically start and stop a PostgreSQL container.
    Container is shared across all tests in the session for efficiency.
    """
    if not TESTCONTAINERS_AVAILABLE or not DOCKER_AVAILABLE:
        pytest.skip("PostgreSQL testcontainer not available")

    container = PostgresContainer("postgres:15")
    container.start()

    yield container

    container.stop()


@pytest.fixture(scope="session")
def postgres_connection_url(postgres_container: Any) -> str:
    """Get PostgreSQL connection URL from container."""
    # testcontainers returns psycopg2 URL, convert to asyncpg
    url = str(postgres_container.get_connection_url())
    return url.replace("postgresql://", "postgresql+asyncpg://").replace("psycopg2", "asyncpg")


@pytest.fixture(scope="session")
def redis_container() -> Generator[Any]:
    """
    Provide Redis container for integration tests.

    Uses testcontainers to automatically start and stop a Redis container.
    Container is shared across all tests in the session for efficiency.
    """
    if not TESTCONTAINERS_AVAILABLE or not DOCKER_AVAILABLE:
        pytest.skip("Redis testcontainer not available")

    container = RedisContainer("redis:7")
    container.start()

    yield container

    container.stop()


@pytest.fixture(scope="session")
def redis_connection_url(redis_container: Any) -> str:
    """Get Redis connection URL from container."""
    host = redis_container.get_container_host_ip()
    port = redis_container.get_exposed_port(6379)
    return f"redis://{host}:{port}"
