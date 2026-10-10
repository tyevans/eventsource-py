"""Standard attribute tests for checkpoint repository implementations."""

from __future__ import annotations

import subprocess
from pathlib import Path


class TestCheckpointRepositoryStandardAttributes:
    """Tests for standard attribute usage in CheckpointRepository implementations."""

    @property
    def _repo_root(self) -> Path:
        return Path(__file__).resolve().parents[4]

    def test_uses_standard_attribute_constants(self):
        """Verify CheckpointRepository implementations use ATTR_* constants."""
        result = subprocess.run(
            ["grep", "-c", "ATTR_", "src/eventsource/adapters/sql/checkpoints.py"],
            capture_output=True,
            text=True,
            cwd=self._repo_root,
        )
        count = int(result.stdout.strip())
        assert count >= 10, f"Expected at least 10 ATTR_* usages, found {count}"

    def test_no_duplicate_otel_available(self):
        """Verify no duplicate OTEL_AVAILABLE definition in checkpoints.py."""
        result = subprocess.run(
            ["grep", "-c", "OTEL_AVAILABLE = ", "src/eventsource/adapters/sql/checkpoints.py"],
            capture_output=True,
            text=True,
            cwd=self._repo_root,
        )
        assert result.stdout.strip() == "0", (
            f"Found {result.stdout.strip()} definitions of OTEL_AVAILABLE in checkpoints.py"
        )

    def test_imports_from_observability_module(self):
        """Verify checkpoints.py imports tracing from observability module."""
        result = subprocess.run(
            [
                "grep",
                "-c",
                "from eventsource.observability import",
                "src/eventsource/adapters/sql/checkpoints.py",
            ],
            capture_output=True,
            text=True,
            cwd=self._repo_root,
        )
        count = int(result.stdout.strip())
        assert count >= 1, "checkpoints.py should import from eventsource.observability"
