"""Configuration classes for subscription management."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Literal
from uuid import UUID

from eventsource.ports.positions import Position

if TYPE_CHECKING:
    from eventsource.application.subscriptions.retry import CircuitBreakerConfig, RetryConfig
    from eventsource.domain.event import DomainEvent


StartPosition = Literal["beginning", "end", "checkpoint"] | Position


class CheckpointStrategy(Enum):
    """Strategy for when to update checkpoints during event processing."""

    EVERY_EVENT = "every_event"
    EVERY_BATCH = "every_batch"
    PERIODIC = "periodic"


@dataclass(frozen=True)
class SubscriptionConfig:
    """Configuration for how a subscription reads events, checkpoints, filters, and retries."""

    start_from: StartPosition = "checkpoint"
    batch_size: int = 100
    checkpoint_strategy: CheckpointStrategy = CheckpointStrategy.EVERY_BATCH
    checkpoint_interval_seconds: float = 5.0
    processing_timeout: float = 30.0
    event_types: tuple[type[DomainEvent], ...] | None = None
    aggregate_types: tuple[str, ...] | None = None
    tenant_id: UUID | None = None
    continue_on_error: bool = True
    max_retries: int = 5
    initial_retry_delay: float = 1.0
    max_retry_delay: float = 60.0
    retry_exponential_base: float = 2.0
    retry_jitter: float = 0.1
    circuit_breaker_enabled: bool = True
    circuit_breaker_failure_threshold: int = 5
    circuit_breaker_recovery_timeout: float = 30.0

    def __post_init__(self) -> None:
        """Validate configuration values."""
        if self.batch_size < 1:
            raise ValueError(
                f"batch_size must be positive, got {self.batch_size}. "
                "Use a value like 100 (default) or adjust based on your processing speed."
            )
        if self.processing_timeout <= 0:
            raise ValueError(
                f"processing_timeout must be positive, got {self.processing_timeout}. "
                "Use a value like 30.0 (default) seconds."
            )
        if self.checkpoint_interval_seconds <= 0:
            raise ValueError(
                f"checkpoint_interval_seconds must be positive, got {self.checkpoint_interval_seconds}."
            )
        if self.max_retries < 0:
            raise ValueError(f"max_retries must be >= 0, got {self.max_retries}.")
        if self.initial_retry_delay <= 0:
            raise ValueError(
                f"initial_retry_delay must be positive, got {self.initial_retry_delay}."
            )
        if self.max_retry_delay <= 0:
            raise ValueError(f"max_retry_delay must be positive, got {self.max_retry_delay}.")
        if self.max_retry_delay < self.initial_retry_delay:
            raise ValueError(
                f"max_retry_delay ({self.max_retry_delay}) must be >= "
                f"initial_retry_delay ({self.initial_retry_delay})."
            )
        if self.retry_exponential_base <= 1.0:
            raise ValueError(
                f"retry_exponential_base must be > 1.0, got {self.retry_exponential_base}."
            )
        if not 0.0 <= self.retry_jitter <= 1.0:
            raise ValueError(f"retry_jitter must be between 0.0 and 1.0, got {self.retry_jitter}.")
        if self.circuit_breaker_failure_threshold < 1:
            raise ValueError(
                f"circuit_breaker_failure_threshold must be >= 1, "
                f"got {self.circuit_breaker_failure_threshold}."
            )
        if self.circuit_breaker_recovery_timeout <= 0:
            raise ValueError(
                f"circuit_breaker_recovery_timeout must be positive, "
                f"got {self.circuit_breaker_recovery_timeout}."
            )

    def get_retry_config(self) -> RetryConfig:
        """Get retry configuration from subscription config."""
        from eventsource.application.subscriptions.retry import RetryConfig

        return RetryConfig(
            max_retries=self.max_retries,
            initial_delay=self.initial_retry_delay,
            max_delay=self.max_retry_delay,
            exponential_base=self.retry_exponential_base,
            jitter=self.retry_jitter,
        )

    def get_circuit_breaker_config(self) -> CircuitBreakerConfig:
        """Get circuit breaker configuration from subscription config."""
        from eventsource.application.subscriptions.retry import CircuitBreakerConfig

        return CircuitBreakerConfig(
            failure_threshold=self.circuit_breaker_failure_threshold,
            recovery_timeout=self.circuit_breaker_recovery_timeout,
        )


def create_catch_up_config(
    batch_size: int = 1000,
    checkpoint_every_batch: bool = True,
) -> SubscriptionConfig:
    """Create a configuration optimized for catch-up scenarios."""
    return SubscriptionConfig(
        start_from="checkpoint",
        batch_size=batch_size,
        checkpoint_strategy=(
            CheckpointStrategy.EVERY_BATCH
            if checkpoint_every_batch
            else CheckpointStrategy.PERIODIC
        ),
    )


def create_live_only_config() -> SubscriptionConfig:
    """Create a configuration for live-only subscriptions."""
    return SubscriptionConfig(
        start_from="end",
        batch_size=100,
        checkpoint_strategy=CheckpointStrategy.EVERY_EVENT,
    )
