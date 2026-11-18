"""Owner tenant of a stream, derived from its events (ADR-0113)."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any
from uuid import UUID


def snapshot_owner(events: Iterable[Any]) -> UUID | None:
    """Return the tenant that owns a stream with these events, or None.

    All events must carry the same tenant. A mixed stream, an untenanted event,
    or an empty stream has no owner, so the snapshot is stored untenanted and a
    scoped load ignores it (fail closed).
    """
    owners = {getattr(event, "tenant_id", None) for event in events}
    if len(owners) != 1:
        return None
    (owner,) = owners
    return owner


__all__ = ["snapshot_owner"]
