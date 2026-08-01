"""Tests for the dead letter queue port."""

import dataclasses
from datetime import UTC, datetime
from typing import Any, get_protocol_members
from uuid import UUID, uuid4

import pytest

from eventsource.ports.dlq import (
    DLQEntry,
    DLQRepository,
    DLQStats,
    ProjectionFailureCount,
)


class TestDLQEntry:
    def test_defaults(self) -> None:
        entry = DLQEntry(
            id=1,
            event_id=uuid4(),
            projection_name="P",
            event_type="Created",
            event_data="{}",
            error_message="boom",
        )
        assert entry.error_stacktrace is None
        assert entry.retry_count == 0
        assert entry.first_failed_at is None
        assert entry.last_failed_at is None
        assert entry.status == "failed"
        assert entry.resolved_at is None
        assert entry.resolved_by is None

    def test_is_mutable_for_post_construction_resolution(self) -> None:
        entry = DLQEntry(
            id=1,
            event_id=uuid4(),
            projection_name="P",
            event_type="Created",
            event_data="{}",
            error_message="boom",
        )
        now = datetime.now(UTC)
        entry.resolved_at = now
        entry.resolved_by = "alice"
        entry.status = "resolved"
        assert entry.resolved_at == now
        assert entry.resolved_by == "alice"
        assert entry.status == "resolved"


class TestDLQStats:
    def test_defaults(self) -> None:
        stats = DLQStats()
        assert stats.total_failed == 0
        assert stats.total_retrying == 0
        assert stats.affected_projections == 0
        assert stats.oldest_failure is None

    def test_is_frozen(self) -> None:
        stats = DLQStats()
        with pytest.raises(dataclasses.FrozenInstanceError):
            stats.total_failed = 1  # type: ignore[misc]


class TestProjectionFailureCount:
    def test_defaults(self) -> None:
        count = ProjectionFailureCount(projection_name="P")
        assert count.failure_count == 0
        assert count.oldest_failure is None
        assert count.most_recent_failure is None

    def test_is_frozen(self) -> None:
        count = ProjectionFailureCount(projection_name="P")
        with pytest.raises(dataclasses.FrozenInstanceError):
            count.failure_count = 1  # type: ignore[misc]


class DLQRepositoryStub:
    async def add_failed_event(
        self,
        event_id: UUID,
        projection_name: str,
        event_type: str,
        event_data: dict[str, Any],
        error: Exception,
        retry_count: int = 0,
    ) -> None:
        return None

    async def get_failed_events(
        self,
        projection_name: str | None = None,
        status: str = "failed",
        limit: int = 100,
    ) -> list[DLQEntry]:
        return []

    async def get_failed_event_by_id(self, dlq_id: int | str) -> DLQEntry | None:
        return None

    async def mark_resolved(self, dlq_id: int | str, resolved_by: str | UUID) -> None:
        return None

    async def mark_retrying(self, dlq_id: int | str) -> None:
        return None

    async def get_failure_stats(self) -> DLQStats:
        return DLQStats()

    async def get_projection_failure_counts(self) -> list[ProjectionFailureCount]:
        return []

    async def delete_resolved_events(self, older_than_days: int = 30) -> int:
        return 0


class TestDLQRepositoryProtocol:
    def test_stub_satisfies_protocol(self) -> None:
        assert isinstance(DLQRepositoryStub(), DLQRepository)

    def test_dlq_repository_protocol_has_no_alias_methods(self) -> None:
        members = set(get_protocol_members(DLQRepository))
        assert "list_failed_events" not in members
        assert "get_failed_event" not in members
        assert "get_failed_events" in members
        assert "get_failed_event_by_id" in members
