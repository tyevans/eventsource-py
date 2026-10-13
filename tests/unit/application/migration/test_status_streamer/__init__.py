"""StatusStreamer unit test package facade."""

from tests.unit.application.migration.test_status_streamer.test_status_stream_manager import (
    TestCoordinatorIntegration,
    TestStatusStreamManagerCleanup,
    TestStatusStreamManagerClose,
    TestStatusStreamManagerGetStreamer,
    TestStatusStreamManagerInit,
)
from tests.unit.application.migration.test_status_streamer.test_status_streamer_lifecycle import (
    TestStatusStreamerClose,
    TestStatusStreamerInit,
    TestStatusStreamerStatusChanged,
)
from tests.unit.application.migration.test_status_streamer.test_status_streamer_streaming import (
    TestStatusStreamerMultipleSubscribers,
    TestStatusStreamerStreamStatus,
)

__all__ = [  # pragma: allowlist secret
    "TestCoordinatorIntegration",
    "TestStatusStreamManagerCleanup",
    "TestStatusStreamManagerClose",
    "TestStatusStreamManagerGetStreamer",
    "TestStatusStreamManagerInit",
    "TestStatusStreamerClose",
    "TestStatusStreamerInit",
    "TestStatusStreamerMultipleSubscribers",
    "TestStatusStreamerStatusChanged",
    "TestStatusStreamerStreamStatus",
]
