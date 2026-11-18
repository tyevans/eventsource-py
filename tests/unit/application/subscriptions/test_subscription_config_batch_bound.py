"""SubscriptionConfig rejects batch sizes above MAX_BATCH_SIZE (TASK-0486)."""

from __future__ import annotations

import pytest

from eventsource.application.subscriptions.config import SubscriptionConfig


def test_batch_size_at_maximum_is_accepted() -> None:
    assert SubscriptionConfig(batch_size=10_000).batch_size == 10_000


def test_batch_size_above_maximum_is_rejected() -> None:
    with pytest.raises(ValueError, match="batch_size must be <= 10000"):
        SubscriptionConfig(batch_size=10_001)
