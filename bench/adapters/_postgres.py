"""Shared PostgreSQL helpers: DSN handling, schema setup, cleanup.

Schema is applied over a raw asyncpg connection because asyncpg's simple
query protocol accepts multi-statement scripts, so get_schema("all") does
not need fragile statement splitting (unlike SQLAlchemy text()).
"""

import os

DEFAULT_URL = "postgresql+asyncpg://bench:bench@localhost:5434/eventsource_bench"


def postgres_url() -> str:
    return os.environ.get("BENCH_POSTGRES_URL", DEFAULT_URL)


def asyncpg_dsn(url: str) -> str:
    return url.replace("postgresql+asyncpg://", "postgresql://")


async def ping(dsn: str) -> str | None:
    try:
        import asyncpg
    except ImportError:
        return "postgresql extra not installed (asyncpg missing)"
    try:
        conn = await asyncpg.connect(dsn, timeout=3)
        await conn.close()
    except Exception as exc:  # noqa: BLE001 - any failure means "not available"
        return f"postgres unreachable at {dsn}: {exc}"
    return None


async def ensure_schema(dsn: str) -> None:
    import asyncpg

    from eventsource.migrations import get_schema

    conn = await asyncpg.connect(dsn, timeout=10)
    try:
        await conn.execute(get_schema("events", backend="postgresql"))
        await conn.execute(get_schema("snapshots", backend="postgresql"))
    finally:
        await conn.close()


async def truncate(dsn: str) -> None:
    import asyncpg

    conn = await asyncpg.connect(dsn, timeout=10)
    try:
        await conn.execute("TRUNCATE TABLE events, snapshots CASCADE")
    finally:
        await conn.close()
