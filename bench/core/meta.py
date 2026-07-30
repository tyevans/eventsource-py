"""Run metadata so reported numbers are never context-free (spec: methodology)."""

import os
import platform
import subprocess
import sys
from datetime import UTC, datetime
from typing import Any


def _git_commit() -> str:
    try:
        out = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
        return out.stdout.strip() or "unknown"
    except OSError:
        return "unknown"


def collect_metadata() -> dict[str, Any]:
    try:
        from importlib.metadata import version

        eventsource_version = version("eventsource-py")
    except Exception:
        eventsource_version = "unknown"
    return {
        "timestamp": datetime.now(UTC).isoformat(),
        "commit": _git_commit(),
        "python": sys.version.split()[0],
        "platform": platform.platform(),
        "cpu_count": os.cpu_count(),
        "eventsource_version": eventsource_version,
    }
