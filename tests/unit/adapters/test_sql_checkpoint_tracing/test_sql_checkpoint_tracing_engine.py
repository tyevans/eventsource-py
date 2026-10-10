"""SQLite engine helper for checkpoint tracing tests."""

from __future__ import annotations


async def _sqlite_checkpoint_engine(tmp_path, *, with_events: bool = True):
    """Build a SQLite engine with the checkpoints (and optionally events) schema."""
    from eventsource import create_async_engine
    from eventsource.adapters.sql.schemas import get_schema

    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/tracing.db")
    async with engine.begin() as conn:
        raw = await conn.get_raw_connection()
        await raw.driver_connection.executescript(get_schema("checkpoints", backend="sqlite"))
        if with_events:
            await raw.driver_connection.executescript(get_schema("events", backend="sqlite"))
    return engine
