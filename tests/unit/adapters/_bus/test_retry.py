"""Unit and property tests for RetryPolicy."""

import random

import pytest
from hypothesis import given
from hypothesis import strategies as st

from eventsource.adapters._bus.retry import RetryPolicy


def test_no_jitter_gives_exact_exponential_backoff() -> None:
    policy = RetryPolicy(base_delay=1.0, max_delay=60.0, jitter=0.0)

    assert policy.delay_for(0) == 1.0
    assert policy.delay_for(1) == 2.0
    assert policy.delay_for(2) == 4.0
    assert policy.delay_for(3) == 8.0


def test_delay_is_capped_at_max_delay() -> None:
    policy = RetryPolicy(base_delay=1.0, max_delay=10.0, jitter=0.0)

    assert policy.delay_for(20) == 10.0


def test_jitter_is_symmetric_not_one_sided() -> None:
    """Regression: Kafka previously used one-sided positive jitter, which
    pushed delays above max_delay and never shortened them."""
    policy = RetryPolicy(base_delay=10.0, max_delay=10.0, jitter=0.5)
    rng = random.Random(0)

    samples = [policy.delay_for(0, rng=rng) for _ in range(200)]

    assert min(samples) < 10.0, "jitter never reduces the delay -- it is one-sided"
    assert max(samples) > 10.0, "jitter never increases the delay"


def test_delay_never_goes_negative() -> None:
    policy = RetryPolicy(base_delay=1.0, max_delay=60.0, jitter=1.0)
    rng = random.Random(1234)

    for _ in range(500):
        assert policy.delay_for(0, rng=rng) >= 0.0


def test_should_retry_respects_max_retries() -> None:
    policy = RetryPolicy(max_retries=3)

    assert policy.should_retry(0) is True
    assert policy.should_retry(2) is True
    assert policy.should_retry(3) is False
    assert policy.should_retry(99) is False


def test_negative_retry_count_is_rejected() -> None:
    policy = RetryPolicy()

    with pytest.raises(ValueError, match="retry_count must be >= 0"):
        policy.delay_for(-1)


@pytest.mark.parametrize(
    "kwargs, match",
    [
        ({"base_delay": 0.0}, "base_delay must be > 0"),
        ({"max_delay": 0.0}, "max_delay must be > 0"),
        ({"base_delay": 10.0, "max_delay": 1.0}, "max_delay must be >= base_delay"),
        ({"jitter": -0.1}, "jitter must be between 0.0 and 1.0"),
        ({"jitter": 1.5}, "jitter must be between 0.0 and 1.0"),
        ({"max_retries": -1}, "max_retries must be >= 0"),
    ],
)
def test_invalid_config_is_rejected(kwargs: dict[str, float], match: str) -> None:
    with pytest.raises(ValueError, match=match):
        RetryPolicy(**kwargs)  # type: ignore[arg-type]


# =============================================================================
# Property tests
# =============================================================================


@given(
    retry_count=st.integers(min_value=0, max_value=64),
    base_delay=st.floats(min_value=0.001, max_value=10.0),
    max_delay=st.floats(min_value=0.001, max_value=3600.0),
    jitter=st.floats(min_value=0.0, max_value=1.0),
    seed=st.integers(min_value=0, max_value=2**32 - 1),
)
def test_delay_is_finite_non_negative_and_bounded(
    retry_count: int,
    base_delay: float,
    max_delay: float,
    jitter: float,
    seed: int,
) -> None:
    """For any valid policy and retry count, the delay is a sane number.

    The upper bound is max_delay * (1 + jitter): the cap applies to the
    exponential term, and symmetric jitter can push at most `jitter` above it.
    """
    effective_max = max(max_delay, base_delay)
    policy = RetryPolicy(base_delay=base_delay, max_delay=effective_max, jitter=jitter)

    delay = policy.delay_for(retry_count, rng=random.Random(seed))

    assert delay == delay, "delay is NaN"
    assert delay != float("inf")
    assert delay >= 0.0
    assert delay <= effective_max * (1.0 + jitter) + 1e-9


@given(
    n=st.integers(min_value=0, max_value=32),
    base_delay=st.floats(min_value=0.001, max_value=10.0),
    max_delay=st.floats(min_value=0.001, max_value=3600.0),
)
def test_jitter_free_delay_is_non_decreasing(n: int, base_delay: float, max_delay: float) -> None:
    """Without jitter, delay_for is monotonically non-decreasing in retry_count."""
    effective_max = max(max_delay, base_delay)
    policy = RetryPolicy(base_delay=base_delay, max_delay=effective_max, jitter=0.0)

    assert policy.delay_for(n + 1) >= policy.delay_for(n)


@given(
    base_delay=st.floats(min_value=0.001, max_value=10.0),
    max_delay=st.floats(min_value=0.001, max_value=3600.0),
    seed=st.integers(min_value=0, max_value=2**32 - 1),
)
def test_zero_jitter_is_deterministic(base_delay: float, max_delay: float, seed: int) -> None:
    effective_max = max(max_delay, base_delay)
    policy = RetryPolicy(base_delay=base_delay, max_delay=effective_max, jitter=0.0)

    a = policy.delay_for(3, rng=random.Random(seed))
    b = policy.delay_for(3, rng=random.Random(seed + 1))

    assert a == b
