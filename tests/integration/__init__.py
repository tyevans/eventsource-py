"""
Integration tests for the eventsource library.

These tests require actual PostgreSQL and Redis instances, either via:
- testcontainers (automatic container provisioning)
- docker-compose (manual container management)
- existing infrastructure

Tests are skipped automatically if required infrastructure is not available.

Run integration tests:
    pytest tests/integration/ -v

Run only PostgreSQL tests:
    pytest tests/integration/ -v -m postgres

Run only Redis tests:
    pytest tests/integration/ -v -m redis

Skip integration tests:
    pytest tests/ -v -m "not integration"
"""
