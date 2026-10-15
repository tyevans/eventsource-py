"""Unit tests for projection replay package."""

from tests.unit.application.projections.test_replay._fixtures import (
    TENANT_A,
    TENANT_B,
    Collecting,
    InvoiceIssued,
    NonAdvancingFeed,
    OrderPlaced,
    PositionlessFeed,
    RecordingFeed,
    RejectsEverything,
    RejectsOne,
)
from tests.unit.application.projections.test_replay.test_replay_bounded_reads import (
    TestMaxEventsGuardsAgainstANonAdvancingCursor,
    TestScopingReachesTheAdaptersQuery,
    TestTheReadIsBounded,
    TestTheScopedReplaySeesOnlyThatSlice,
)
from tests.unit.application.projections.test_replay.test_replay_failures_and_errors import (
    TestAPoisonEventDoesNotStopTheRebuild,
    TestFailedCountsEventsAndFailuresCountRejections,
    TestSingleProjectionAndSucceededReport,
    TestStrictRaisesOnTheFirstRejection,
    TestTheFailureListIsBoundedAndSaysSo,
)

__all__ = [  # pragma: allowlist secret
    "Collecting",
    "InvoiceIssued",
    "NonAdvancingFeed",
    "OrderPlaced",
    "PositionlessFeed",
    "RecordingFeed",
    "RejectsEverything",
    "RejectsOne",
    "TENANT_A",
    "TENANT_B",
    "TestAPoisonEventDoesNotStopTheRebuild",
    "TestFailedCountsEventsAndFailuresCountRejections",
    "TestMaxEventsGuardsAgainstANonAdvancingCursor",
    "TestScopingReachesTheAdaptersQuery",
    "TestSingleProjectionAndSucceededReport",
    "TestStrictRaisesOnTheFirstRejection",
    "TestTheFailureListIsBoundedAndSaysSo",
    "TestTheReadIsBounded",
    "TestTheScopedReplaySeesOnlyThatSlice",
]
