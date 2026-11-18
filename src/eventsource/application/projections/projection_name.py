"""Resolution of the optional `projection_name` constructor argument (ADR-0114).

The projection name is the checkpoint key and the DLQ partition key. A
per-tenant instance passes a name such as `"OrderSummary:<tenant_id>"` so that
each instance keeps its own checkpoint row.
"""

import unicodedata

MAX_PROJECTION_NAME_LENGTH = 255


def resolve_projection_name(projection_name: str | None, default: str) -> str:
    """Return the validated name, or `default` when no name was given.

    Raises:
        TypeError: The name is not a `str`.
        ValueError: The name is blank, longer than 255 characters (the
            `projection_checkpoints` column limit), or contains a control
            character.
    """
    if projection_name is None:
        return default
    if not isinstance(projection_name, str):
        raise TypeError(f"projection_name must be a str, not {type(projection_name).__name__}")
    if not projection_name.strip():
        raise ValueError("projection_name must be a non-empty string")
    if len(projection_name) > MAX_PROJECTION_NAME_LENGTH:
        raise ValueError(f"projection_name must be at most {MAX_PROJECTION_NAME_LENGTH} characters")
    if any(unicodedata.category(ch) == "Cc" for ch in projection_name):
        raise ValueError("projection_name must not contain control characters")
    return projection_name


__all__ = ["MAX_PROJECTION_NAME_LENGTH", "resolve_projection_name"]
