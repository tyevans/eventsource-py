"""
Backward compatibility helpers for the stores module.

This module provides utilities for maintaining backward compatibility
during API transitions, particularly for timestamp type normalization.
"""

import warnings
from datetime import UTC, datetime


def normalize_timestamp(
    value: datetime | float | int | None,
    param_name: str = "timestamp",
) -> datetime | None:
    """
    Normalize a timestamp parameter to datetime.

    Accepts datetime (preferred) or float/int (deprecated, Unix timestamp).
    This helper enables a smooth migration from float timestamps to datetime
    while maintaining backward compatibility.

    Args:
        value: Timestamp as datetime, float, int, or None
        param_name: Parameter name for the warning message

    Returns:
        datetime or None

    Raises:
        TypeError: If value is neither datetime, float, int, nor None

    Example:
        >>> from datetime import datetime, UTC
        >>> # datetime passes through unchanged
        >>> dt = datetime.now(UTC)
        >>> normalize_timestamp(dt) == dt
        True
        >>>
        >>> # float is converted with deprecation warning
        >>> import time
        >>> result = normalize_timestamp(time.time())  # DeprecationWarning
        >>> isinstance(result, datetime)
        True
        >>>
        >>> # None passes through unchanged
        >>> normalize_timestamp(None) is None
        True
    """
    if value is None:
        return None

    if isinstance(value, datetime):
        return value

    if isinstance(value, int | float):
        warnings.warn(
            f"Passing {param_name} as float (Unix timestamp) is deprecated. "
            f"Use datetime instead: datetime.fromtimestamp({value}, UTC). "
            "Float timestamps will not be accepted in version 0.3.0.",
            DeprecationWarning,
            stacklevel=3,
        )
        return datetime.fromtimestamp(value, UTC)

    raise TypeError(f"{param_name} must be datetime or None, got {type(value).__name__}")
