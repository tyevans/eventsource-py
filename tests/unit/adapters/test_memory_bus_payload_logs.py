"""A failing handler's log records never carry the event payload marker.

The bus is driven through its public ``publish`` path. The handler validates the
event's payload with pydantic, so the raised ``ValidationError`` echoes the marker
value. The assertion reads only the captured log records.
"""

from __future__ import annotations

import logging
from uuid import UUID

import pytest
from pydantic import BaseModel

from eventsource.adapters.memory.bus import InMemoryEventBus
from eventsource.domain.event import DomainEvent

MARKER = "victim-secret@example.com"


class AccountOpened(DomainEvent):
    aggregate_type: str = "Account"
    owner_ref: str


class _Owner(BaseModel):
    owner_id: UUID


async def test_failed_handler_keeps_payload_marker_out_of_log_records(
    caplog: pytest.LogCaptureFixture,
) -> None:
    bus = InMemoryEventBus(enable_tracing=False)

    async def handler(event: AccountOpened) -> None:
        _Owner.model_validate({"owner_id": event.owner_ref})

    bus.subscribe(AccountOpened, handler)  # type: ignore[arg-type]
    with caplog.at_level(logging.DEBUG):
        await bus.publish([AccountOpened(aggregate_id=UUID(int=1), owner_ref=MARKER)])

    assert bus.get_stats()["handler_errors"] == 1
    failed = [r for r in caplog.records if "failed processing" in r.getMessage()]
    assert len(failed) == 1 and failed[0].exc_info is None
    for record in caplog.records:
        assert MARKER not in record.getMessage(), record.getMessage()
        assert MARKER not in repr(record.args)
        assert MARKER not in repr(record.__dict__)
