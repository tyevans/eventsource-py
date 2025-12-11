"""
Configuration classes for subscription management.

This module provides:
- SubscriptionConfig: Configuration for individual subscriptions
- StartPosition: Type alias for subscription start positions
- CheckpointStrategy: Enum for checkpoint update strategies
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Literal
from uuid import UUID

if TYPE_CHECKING:
    from eventsource.events.base import DomainEvent
    from eventsource.subscriptions.retry import CircuitBreakerConfig, RetryConfig


# Type alias for start position
# Can be a string literal or an integer position
StartPosition = Literal["beginning", "end", "checkpoint"] | int


class CheckpointStrategy(Enum):
    """
    Strategy for when to update checkpoints during event processing.

    Attributes:
        EVERY_EVENT: Update checkpoint after each event (safest, slowest)
        EVERY_BATCH: Update checkpoint after each batch (default, balanced)
        PERIODIC: Update checkpoint on a time interval (fastest, riskiest)
    """

    EVERY_EVENT = "every_event"
    EVERY_BATCH = "every_batch"
    PERIODIC = "periodic"


@dataclass(frozen=True)
class SubscriptionConfig:
    """
    Configuration for a subscription.

    Controls how the subscription reads events, manages checkpoints,
    and handles backpressure.

    Attributes:
        start_from: Where to start reading events
            - "beginning": Start from global position 0
            - "end": Start from current end (live-only)
            - "checkpoint": Resume from last checkpoint (default)
            - int: Start from specific global position
        batch_size: Number of events to read in each batch during catch-up
        max_in_flight: Maximum events being processed concurrently
        backpressure_threshold: Fraction (0-1) at which to signal backpressure
        checkpoint_strategy: When to persist checkpoint updates
        checkpoint_interval_seconds: Interval for PERIODIC strategy
        processing_timeout: Max seconds to wait for event processing
        shutdown_timeout: Max seconds to wait during graceful shutdown
        event_types: Event types to filter (None = all types)
        aggregate_types: Aggregate types to filter (None = all types)
        tenant_id: Tenant ID to filter events by (None = all tenants).
            When specified, only events belonging to the specified tenant
            are processed. Useful for tenant-specific migrations and
            multi-tenant event streaming scenarios.
        continue_on_error: Whether to continue after DLQ'd events

    Example:
        >>> config = SubscriptionConfig(
        ...     start_from="beginning",
        ...     batch_size=500,
        ...     max_in_flight=2000,
        ...     backpressure_threshold=0.8,
        ... )
        >>>
        >>> # Tenant-scoped subscription for multi-tenant migration
        >>> from uuid import UUID
        >>> tenant_config = SubscriptionConfig(
        ...     tenant_id=UUID("12345678-1234-5678-1234-567812345678"),
        ...     start_from="beginning",
        ... )
    """

    # Starting position
    start_from: StartPosition = "checkpoint"

    # Batch processing settings
    batch_size: int = 100
    max_in_flight: int = 1000

    # Backpressure settings
    backpressure_threshold: float = 0.8

    # Checkpoint behavior
    checkpoint_strategy: CheckpointStrategy = CheckpointStrategy.EVERY_BATCH
    checkpoint_interval_seconds: float = 5.0

    # Timeouts
    processing_timeout: float = 30.0
    shutdown_timeout: float = 30.0

    # Filtering (optional)
    event_types: tuple[type[DomainEvent], ...] | None = None
    aggregate_types: tuple[str, ...] | None = None
    tenant_id: UUID | None = None

    # Error handling
    continue_on_error: bool = True

    # Retry settings
    max_retries: int = 5
    initial_retry_delay: float = 1.0
    max_retry_delay: float = 60.0
    retry_exponential_base: float = 2.0
    retry_jitter: float = 0.1

    # Circuit breaker settings
    circuit_breaker_enabled: bool = True
    circuit_breaker_failure_threshold: int = 5
    circuit_breaker_recovery_timeout: float = 30.0

    def __post_init__(self) -> None:
        """Validate configuration values."""
        # Validate batch_size
        if self.batch_size < 1:
            raise ValueError(
                f"batch_size must be positive, got {self.batch_size}. "
                "Use a value like 100 (default) or adjust based on your processing speed."
            )

        # Validate max_in_flight
        if self.max_in_flight < 1:
            raise ValueError(
                f"max_in_flight must be positive, got {self.max_in_flight}. "
                "Use a value like 1000 (default) for reasonable backpressure."
            )

        # Validate start_from if integer
        if isinstance(self.start_from, int) and self.start_from < 0:
            raise ValueError(
                f"start_from position must be >= 0, got {self.start_from}. "
                "Use 0 for beginning, or 'beginning'/'end'/'checkpoint' strings."
            )

        # Validate timeouts
        if self.processing_timeout <= 0:
            raise ValueError(
                f"processing_timeout must be positive, got {self.processing_timeout}. "
                "Use a value like 30.0 (default) seconds."
            )

        if self.shutdown_timeout <= 0:
            raise ValueError(
                f"shutdown_timeout must be positive, got {self.shutdown_timeout}. "
                "Use a value like 30.0 (default) seconds."
            )

        # Validate checkpoint_interval
        if self.checkpoint_interval_seconds <= 0:
            raise ValueError(
                f"checkpoint_interval_seconds must be positive, got {self.checkpoint_interval_seconds}."
            )

        # Validate backpressure_threshold
        if not 0.0 <= self.backpressure_threshold <= 1.0:
            raise ValueError(
                f"backpressure_threshold must be between 0.0 and 1.0, "
                f"got {self.backpressure_threshold}."
            )

        # Validate retry settings
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

        # Validate circuit breaker settings
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
        """
        Get retry configuration from subscription config.

        Returns:
            RetryConfig instance with settings from this config
        """
        from eventsource.subscriptions.retry import RetryConfig

        return RetryConfig(
            max_retries=self.max_retries,
            initial_delay=self.initial_retry_delay,
            max_delay=self.max_retry_delay,
            exponential_base=self.retry_exponential_base,
            jitter=self.retry_jitter,
        )

    def get_circuit_breaker_config(self) -> CircuitBreakerConfig:
        """
        Get circuit breaker configuration from subscription config.

        Returns:
            CircuitBreakerConfig instance with settings from this config
        """
        from eventsource.subscriptions.retry import CircuitBreakerConfig

        return CircuitBreakerConfig(
            failure_threshold=self.circuit_breaker_failure_threshold,
            recovery_timeout=self.circuit_breaker_recovery_timeout,
        )


def create_catch_up_config(
    batch_size: int = 1000,
    checkpoint_every_batch: bool = True,
) -> SubscriptionConfig:
    """
    Create a configuration optimized for catch-up scenarios.

    Uses larger batch sizes and batch checkpointing for faster processing.

    Args:
        batch_size: Events per batch (default 1000 for catch-up)
        checkpoint_every_batch: Whether to checkpoint after each batch

    Returns:
        SubscriptionConfig optimized for catch-up
    """
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
    """
    Create a configuration for live-only subscriptions.

    Starts from the end of the event store, receiving only new events.

    Returns:
        SubscriptionConfig for live-only subscription
    """
    return SubscriptionConfig(
        start_from="end",
        batch_size=100,
        checkpoint_strategy=CheckpointStrategy.EVERY_EVENT,
    )
