"""Checkpoint-error reporting for LiveRunner delivery.

A checkpoint save that fails after a successful handler call is a checkpoint
error, not an event failure. The event stays counted as processed and is not
dispatched to the error handler. The failure is logged here and then re-raised.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from eventsource.observability.error_summary import error_summary

logger = logging.getLogger(__name__)


@asynccontextmanager
async def checkpoint_errors(subscription: str, event_id: str) -> AsyncIterator[None]:
    """Log a failed checkpoint save as a checkpoint error, then re-raise it."""
    try:
        yield
    except Exception as e:
        logger.error(
            "Live checkpoint save failed after the event was handled",
            extra={"subscription": subscription, "event_id": event_id, "error": error_summary(e)},
        )
        raise


__all__ = ["checkpoint_errors"]
