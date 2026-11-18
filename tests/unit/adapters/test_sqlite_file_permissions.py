"""SQLite database files are created owner-only (0600); existing files keep their mode.

Blackbox through `SQLiteEventStore`: the store creates the file on first
use, and the test inspects the file mode on disk. POSIX only, because
Windows does not model Unix permission bits.
"""

from __future__ import annotations

import os
import stat
import sys
from pathlib import Path

import pytest

from eventsource.adapters.sqlite.store import SQLiteEventStore
from eventsource.ports import ExpectedVersion
from eventsource.testing.conformance_ports._fixtures import (
    make_conformance_registry,
    make_event,
    make_stream,
)
from tests.conftest import skip_if_no_aiosqlite

pytestmark = [
    pytest.mark.sqlite,
    skip_if_no_aiosqlite,
    pytest.mark.skipif(sys.platform == "win32", reason="POSIX file modes only"),
]


def _mode(path: Path) -> int:
    return stat.S_IMODE(os.stat(path).st_mode)


async def _append_one(database: Path) -> None:
    stream = make_stream()
    async with SQLiteEventStore(str(database), make_conformance_registry()) as store:
        await store.append(stream, [make_event(stream.aggregate_id)], ExpectedVersion.no_stream())


class TestSQLiteDatabaseFileMode:
    async def test_new_database_file_is_owner_only(self, tmp_path: Path) -> None:
        database = tmp_path / "events.db"
        assert not database.exists()

        await _append_one(database)

        assert _mode(database) == 0o600

    async def test_existing_database_file_keeps_its_mode(self, tmp_path: Path) -> None:
        database = tmp_path / "shared.db"
        database.touch()
        os.chmod(database, 0o644)

        await _append_one(database)

        assert _mode(database) == 0o644
