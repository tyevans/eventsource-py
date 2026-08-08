"""Backend-agnostic shared internals for store adapters.

Sibling to `_sql/` (dialect-specific) and `_bus/` (transport-specific): what
lands here is shared by adapters that have no technology in common, because
it is port semantics rather than storage mechanics. Adapters-internal, not
public API -- users get these behaviors through the adapters.
"""

from eventsource.adapters._common.expected_version import check_expected, describe_expected
from eventsource.adapters._common.readmodel_filters import (
    POSTGRESQL,
    SQLITE,
    SqlDialect,
    check_filters,
    filter_to_sql,
    matches_filter,
)
from eventsource.adapters._common.registry_check import check_registered

__all__ = [
    "POSTGRESQL",
    "SQLITE",
    "SqlDialect",
    "check_expected",
    "check_filters",
    "check_registered",
    "describe_expected",
    "filter_to_sql",
    "matches_filter",
]
