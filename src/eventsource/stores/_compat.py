"""
Helpers for the stores module.
"""

from datetime import datetime


def validate_timestamp(
    value: datetime | None,
    param_name: str = "timestamp",
) -> datetime | None:
    """
    Validate a timestamp parameter.

    Args:
        value: Timestamp as datetime or None
        param_name: Parameter name for the error message

    Returns:
        datetime or None (passes through unchanged)

    Raises:
        TypeError: If value is neither datetime nor None
    """
    if value is None:
        return None

    if isinstance(value, datetime):
        return value

    raise TypeError(f"{param_name} must be datetime or None, got {type(value).__name__}")
