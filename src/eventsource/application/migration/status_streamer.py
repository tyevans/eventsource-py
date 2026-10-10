"""
StatusStreamer - Real-time migration status streaming.

This module provides async iterator-based status streaming for real-time
migration monitoring. It implements the observer pattern to allow multiple
clients to subscribe to status updates for a migration.

This module implements P4-002: Implement MigrationStatus Streaming.

Features:
    - Async generator/iterator for streaming status updates
    - Support for multiple simultaneous subscribers per migration
    - Configurable update interval
    - Automatic cleanup on disconnect or migration completion
    - Phase change, progress update, and error notifications
    - OpenTelemetry tracing support

Usage:
    >>> from eventsource.application.migration import MigrationCoordinator, StatusStreamer
    >>>
    >>> # Create streamer from coordinator
    >>> streamer = StatusStreamer(
    ...     coordinator=coordinator,
    ...     migration_id=migration_id,
    ... )
    >>>
    >>> # Subscribe to status updates
    >>> async for status in streamer.stream_status():
    ...     print(f"Phase: {status.phase}, Progress: {status.progress_percent}%")
    ...     if status.phase.is_terminal:
    ...         break
    >>>
    >>> # With configurable update interval
    >>> async for status in streamer.stream_status(update_interval=0.5):
    ...     handle_status(status)

See Also:
    - Task: P4-002-status-streaming.md
    - FRD: docs/tasks/multi-tenant-live-migration/multi-tenant-live-migration.md
"""

from __future__ import annotations

from eventsource.application.migration.status_streamer_core import StatusStreamer
from eventsource.application.migration.status_streamer_manager import StatusStreamManager

__all__ = [
    "StatusStreamManager",
    "StatusStreamer",
]
