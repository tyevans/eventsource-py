"""Shared configuration and fixtures for Redis integration tests."""

from __future__ import annotations

import pytest

from eventsource import REDIS_AVAILABLE

if not REDIS_AVAILABLE:
    pytest.skip("Redis not available", allow_module_level=True)
