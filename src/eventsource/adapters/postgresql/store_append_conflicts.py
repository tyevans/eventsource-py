"""Classify a PostgreSQL append `IntegrityError` as an event_id or aggregate-version conflict."""

from __future__ import annotations

from sqlalchemy.exc import IntegrityError

_EVENT_ID_UNIQUE_CONSTRAINT = "events_event_id_key"
_EVENT_IDS_PRIMARY_KEY_CONSTRAINT = "event_ids_pkey"
_AGGREGATE_VERSION_UNIQUE_CONSTRAINT = "uq_events_aggregate_version"


def classify_integrity_error(e: IntegrityError) -> str | None:
    """Classify an append `IntegrityError` by the real constraint name or substring fallback."""
    constraint_name = getattr(e.orig, "constraint_name", None) or getattr(
        getattr(e.orig, "__cause__", None), "constraint_name", None
    )
    if constraint_name in (_EVENT_ID_UNIQUE_CONSTRAINT, _EVENT_IDS_PRIMARY_KEY_CONSTRAINT):
        return "event_id"
    if constraint_name == _AGGREGATE_VERSION_UNIQUE_CONSTRAINT:
        return "aggregate_version"
    if constraint_name is not None:
        return None

    error_str = str(e).lower()
    if "event_id" in error_str:
        return "event_id"
    if "uq_events_aggregate_version" in error_str or (
        "unique" in error_str and "aggregate" in error_str and "version" in error_str
    ):
        return "aggregate_version"
    return None


__all__ = ["classify_integrity_error"]
