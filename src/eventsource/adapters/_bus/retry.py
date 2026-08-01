"""Shared retry policy for event bus implementations.

Kafka and RabbitMQ both implemented exponential backoff with jitter, using
identically-named config fields but different jitter distributions: Kafka
applied one-sided positive jitter (delays only ever grew, and could exceed the
advertised max_delay), RabbitMQ applied symmetric jitter clamped at zero. This
module settles that on the symmetric form.
"""

from __future__ import annotations

import random
from dataclasses import dataclass

# Module-level RNG for retry timing. Not cryptographic -- jitter only needs to
# decorrelate concurrent consumers, not resist prediction.
_DEFAULT_RNG = random.Random()  # nosec B311


@dataclass(frozen=True)
class RetryPolicy:
    """Exponential backoff with symmetric jitter.

    The delay for attempt ``n`` is ``min(base_delay * 2**n, max_delay)``, with
    symmetric jitter of +/- ``jitter`` (as a fraction) applied afterwards and
    the result clamped at zero.

    Attributes:
        base_delay: Delay in seconds for the first retry. Must be > 0.
        max_delay: Ceiling for the exponential term, in seconds. Must be > 0
            and >= base_delay.
        jitter: Fraction of the delay to randomize, 0.0 to 1.0. At 0.1, the
            delay varies by +/-10%.
        max_retries: Number of retries before giving up. 0 means never retry.

    Example:
        >>> policy = RetryPolicy(base_delay=1.0, max_delay=60.0, jitter=0.1)
        >>> policy.delay_for(3)     # ~8s, +/- 10%
        >>> policy.should_retry(3)
        False
    """

    base_delay: float = 1.0
    max_delay: float = 60.0
    jitter: float = 0.1
    max_retries: int = 3

    def __post_init__(self) -> None:
        if self.base_delay <= 0:
            raise ValueError(f"base_delay must be > 0, got {self.base_delay}")
        if self.max_delay <= 0:
            raise ValueError(f"max_delay must be > 0, got {self.max_delay}")
        if self.max_delay < self.base_delay:
            raise ValueError(
                f"max_delay must be >= base_delay, got {self.max_delay} < {self.base_delay}"
            )
        if not 0.0 <= self.jitter <= 1.0:
            raise ValueError(f"jitter must be between 0.0 and 1.0, got {self.jitter}")
        if self.max_retries < 0:
            raise ValueError(f"max_retries must be >= 0, got {self.max_retries}")

    def delay_for(self, retry_count: int, rng: random.Random | None = None) -> float:
        """Compute the delay in seconds before the next retry.

        Args:
            retry_count: Zero-based attempt number.
            rng: Random source, for deterministic testing. Defaults to a
                module-level RNG.

        Returns:
            Delay in seconds, always >= 0.

        Raises:
            ValueError: If retry_count is negative.
        """
        if retry_count < 0:
            raise ValueError(f"retry_count must be >= 0, got {retry_count}")

        # 2**retry_count overflows to inf for very large counts; cap the
        # exponent so the min() below stays meaningful.
        capped_exponent = min(retry_count, 512)
        delay = min(self.base_delay * (2.0**capped_exponent), self.max_delay)

        if self.jitter > 0:
            source = rng if rng is not None else _DEFAULT_RNG
            jitter_range = delay * self.jitter
            delay = max(0.0, delay + source.uniform(-jitter_range, jitter_range))

        return delay

    def should_retry(self, retry_count: int) -> bool:
        """Whether another retry is permitted after ``retry_count`` attempts."""
        return retry_count < self.max_retries


__all__ = ["RetryPolicy"]
