"""Unit tests for SQLCheckpointRepository lag metrics calculation."""

from __future__ import annotations

from datetime import datetime
from uuid import uuid4

import pytest
from sqlalchemy import text

from eventsource.ports.checkpoints import LagMetrics


class TestSQLCheckpointRepositoryLag:
    """The unified repository must behave identically on both dialects - Lag Metrics."""

    @pytest.fixture
    async def sqlite_engine(self, tmp_path):
        from eventsource import create_async_engine
        from eventsource.adapters.sql.schemas import get_schema

        engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/cp.db")
        async with engine.begin() as conn:
            raw = await conn.get_raw_connection()
            await raw.driver_connection.executescript(get_schema("checkpoints", backend="sqlite"))
            await raw.driver_connection.executescript(get_schema("events", backend="sqlite"))
        yield engine
        await engine.dispose()

    async def test_get_lag_metrics_returns_none_when_no_checkpoint(self, sqlite_engine):
        from eventsource.adapters.sql.checkpoints import SQLCheckpointRepository

        repo = SQLCheckpointRepository(sqlite_engine)
        result = await repo.get_lag_metrics("NonExistent")
        assert result is None

    async def test_get_lag_metrics_zero_when_checkpoint_matches_latest_event(self, sqlite_engine):
        """When last_event_id == latest_event_id, lag is reported as 0 even if
        the raw timestamp arithmetic would otherwise suggest a nonzero gap."""
        from datetime import UTC
        from datetime import datetime as dt

        from eventsource.adapters.sql.checkpoints import SQLCheckpointRepository

        repo = SQLCheckpointRepository(sqlite_engine)
        projection_name = "TestProjection"
        event_id = uuid4()

        now = dt.now(UTC).isoformat()
        async with sqlite_engine.begin() as conn:
            await conn.execute(
                text("""
                    INSERT INTO events (event_id, event_type, aggregate_type, aggregate_id,
                                       version, timestamp, payload)
                    VALUES (:event_id, :event_type, :aggregate_type, :aggregate_id,
                            :version, :timestamp, :payload)
                    """),
                {
                    "event_id": str(event_id),
                    "event_type": "TestEvent",
                    "aggregate_type": "TestAggregate",
                    "aggregate_id": str(uuid4()),
                    "version": 1,
                    "timestamp": now,
                    "payload": "{}",
                },
            )

        await repo.update_checkpoint(projection_name, event_id, "TestEvent")

        result = await repo.get_lag_metrics(projection_name, event_types=["TestEvent"])
        assert result is not None
        assert result.last_event_id == result.latest_event_id
        assert result.lag_seconds == 0.0

    async def test_get_lag_metrics_zero_when_raw_lag_negative(self, sqlite_engine):
        """A checkpoint processed AFTER the latest relevant event (raw lag < 0,
        e.g. clock skew) must be clamped to 0, not reported as negative."""
        from datetime import UTC, timedelta
        from datetime import datetime as dt

        from eventsource.adapters.sql.checkpoints import SQLCheckpointRepository

        repo = SQLCheckpointRepository(sqlite_engine)
        projection_name = "TestProjection"
        event_id = uuid4()
        other_event_id = uuid4()

        earlier = (dt.now(UTC) - timedelta(minutes=5)).isoformat()
        async with sqlite_engine.begin() as conn:
            await conn.execute(
                text("""
                    INSERT INTO events (event_id, event_type, aggregate_type, aggregate_id,
                                       version, timestamp, payload)
                    VALUES (:event_id, :event_type, :aggregate_type, :aggregate_id,
                            :version, :timestamp, :payload)
                    """),
                {
                    "event_id": str(other_event_id),
                    "event_type": "TestEvent",
                    "aggregate_type": "TestAggregate",
                    "aggregate_id": str(uuid4()),
                    "version": 1,
                    "timestamp": earlier,
                    "payload": "{}",
                },
            )

        await repo.update_checkpoint(projection_name, event_id, "TestEvent")

        result = await repo.get_lag_metrics(projection_name, event_types=["TestEvent"])
        assert result is not None
        assert result.lag_seconds == 0.0

    async def test_get_lag_metrics_returns_data_for_existing_checkpoint(self, sqlite_engine):
        from eventsource.adapters.sql.checkpoints import SQLCheckpointRepository

        repo = SQLCheckpointRepository(sqlite_engine)
        projection_name = "TestProjection"
        event_id = uuid4()

        await repo.update_checkpoint(projection_name, event_id, "TestEvent")

        result = await repo.get_lag_metrics(projection_name)
        assert result is not None
        assert isinstance(result, LagMetrics)
        assert result.projection_name == projection_name
        assert result.last_event_id == str(event_id)
        assert result.events_processed == 1

    async def test_lag_metrics_has_timestamp_info(self, sqlite_engine):
        from eventsource.adapters.sql.checkpoints import SQLCheckpointRepository

        repo = SQLCheckpointRepository(sqlite_engine)
        projection_name = "TestProjection"
        event_id = uuid4()

        await repo.update_checkpoint(projection_name, event_id, "TestEvent")

        result = await repo.get_lag_metrics(projection_name)
        assert result is not None
        assert result.last_processed_at is not None
        datetime.fromisoformat(result.last_processed_at)

    async def test_lag_metrics_with_events(self, sqlite_engine):
        from datetime import UTC
        from datetime import datetime as dt

        from eventsource.adapters.sql.checkpoints import SQLCheckpointRepository

        repo = SQLCheckpointRepository(sqlite_engine)
        projection_name = "TestProjection"
        event_id = uuid4()

        now = dt.now(UTC).isoformat()
        async with sqlite_engine.begin() as conn:
            await conn.execute(
                text("""
                    INSERT INTO events (event_id, event_type, aggregate_type, aggregate_id,
                                       version, timestamp, payload)
                    VALUES (:event_id, :event_type, :aggregate_type, :aggregate_id,
                            :version, :timestamp, :payload)
                    """),
                {
                    "event_id": str(event_id),
                    "event_type": "TestEvent",
                    "aggregate_type": "TestAggregate",
                    "aggregate_id": str(uuid4()),
                    "version": 1,
                    "timestamp": now,
                    "payload": "{}",
                },
            )

        await repo.update_checkpoint(projection_name, event_id, "TestEvent")

        result = await repo.get_lag_metrics(projection_name, event_types=["TestEvent"])
        assert result is not None
        assert result.projection_name == projection_name
        assert result.latest_event_id == str(event_id)

    async def test_lag_metrics_without_event_types_uses_latest_event(self, sqlite_engine):
        """Cross-dialect invariant: with NO `event_types` filter,
        `get_lag_metrics` must find the true latest relevant event."""
        from datetime import UTC
        from datetime import datetime as dt

        from eventsource.adapters.sql.checkpoints import SQLCheckpointRepository

        repo = SQLCheckpointRepository(sqlite_engine)
        projection_name = "TestProjection"
        event_id = uuid4()

        now = dt.now(UTC).isoformat()
        async with sqlite_engine.begin() as conn:
            await conn.execute(
                text("""
                    INSERT INTO events (event_id, event_type, aggregate_type, aggregate_id,
                                       version, timestamp, payload)
                    VALUES (:event_id, :event_type, :aggregate_type, :aggregate_id,
                            :version, :timestamp, :payload)
                    """),
                {
                    "event_id": str(event_id),
                    "event_type": "TestEvent",
                    "aggregate_type": "TestAggregate",
                    "aggregate_id": str(uuid4()),
                    "version": 1,
                    "timestamp": now,
                    "payload": "{}",
                },
            )

        await repo.update_checkpoint(projection_name, event_id, "TestEvent")

        result = await repo.get_lag_metrics(projection_name)
        assert result is not None
        assert result.latest_event_id == str(event_id)

    async def test_lag_metrics_with_event_type_filter(self, sqlite_engine):
        from datetime import UTC
        from datetime import datetime as dt

        from eventsource.adapters.sql.checkpoints import SQLCheckpointRepository

        repo = SQLCheckpointRepository(sqlite_engine)
        projection_name = "TestProjection"
        event_id_1, event_id_2 = uuid4(), uuid4()

        now = dt.now(UTC).isoformat()
        async with sqlite_engine.begin() as conn:
            for event_id, event_type in ((event_id_1, "TypeA"), (event_id_2, "TypeB")):
                await conn.execute(
                    text("""
                        INSERT INTO events (event_id, event_type, aggregate_type, aggregate_id,
                                           version, timestamp, payload)
                        VALUES (:event_id, :event_type, :aggregate_type, :aggregate_id,
                                 :version, :timestamp, :payload)
                        """),
                    {
                        "event_id": str(event_id),
                        "event_type": event_type,
                        "aggregate_type": "TestAggregate",
                        "aggregate_id": str(uuid4()),
                        "version": 1,
                        "timestamp": now,
                        "payload": "{}",
                    },
                )

        await repo.update_checkpoint(projection_name, event_id_1, "TypeA")

        result = await repo.get_lag_metrics(projection_name, event_types=["TypeA"])
        assert result is not None
        assert result.latest_event_id == str(event_id_1)

    async def test_lag_metrics_with_multiple_event_type_filter(self, sqlite_engine):
        """event_types with more than one entry exercises the multi-placeholder
        IN-clause join (":et0", ":et1", ...), not just the single-item case."""
        from datetime import UTC
        from datetime import datetime as dt

        from eventsource.adapters.sql.checkpoints import SQLCheckpointRepository

        repo = SQLCheckpointRepository(sqlite_engine)
        projection_name = "TestProjection"
        event_id_1, event_id_2, event_id_3 = uuid4(), uuid4(), uuid4()

        now = dt.now(UTC).isoformat()
        async with sqlite_engine.begin() as conn:
            for event_id, event_type in (
                (event_id_1, "TypeA"),
                (event_id_2, "TypeB"),
                (event_id_3, "TypeC"),
            ):
                await conn.execute(
                    text("""
                        INSERT INTO events (event_id, event_type, aggregate_type, aggregate_id,
                                           version, timestamp, payload)
                        VALUES (:event_id, :event_type, :aggregate_type, :aggregate_id,
                                :version, :timestamp, :payload)
                        """),
                    {
                        "event_id": str(event_id),
                        "event_type": event_type,
                        "aggregate_type": "TestAggregate",
                        "aggregate_id": str(uuid4()),
                        "version": 1,
                        "timestamp": now,
                        "payload": "{}",
                    },
                )

        await repo.update_checkpoint(projection_name, event_id_1, "TypeA")

        result = await repo.get_lag_metrics(projection_name, event_types=["TypeB", "TypeC"])
        assert result is not None
        assert result.latest_event_id in (str(event_id_2), str(event_id_3))
        assert result.latest_event_id != str(event_id_1)

    async def test_get_lag_metrics_reports_positive_lag_for_stale_checkpoint(self, sqlite_engine):
        """A checkpoint stuck on an OLDER event than the latest relevant one
        must report a real, positive `lag_seconds` -- not silently clamp to 0."""
        from datetime import UTC, timedelta
        from datetime import datetime as dt

        from eventsource.adapters.sql.checkpoints import SQLCheckpointRepository

        repo = SQLCheckpointRepository(sqlite_engine)
        projection_name = "TestProjection"
        stale_event_id = uuid4()
        newer_event_id = uuid4()

        await repo.update_checkpoint(projection_name, stale_event_id, "TestEvent")

        newer_timestamp = (dt.now(UTC) + timedelta(seconds=12.3)).isoformat()
        async with sqlite_engine.begin() as conn:
            await conn.execute(
                text("""
                    INSERT INTO events (event_id, event_type, aggregate_type, aggregate_id,
                                       version, timestamp, payload)
                    VALUES (:event_id, :event_type, :aggregate_type, :aggregate_id,
                            :version, :timestamp, :payload)
                    """),
                {
                    "event_id": str(newer_event_id),
                    "event_type": "TestEvent",
                    "aggregate_type": "TestAggregate",
                    "aggregate_id": str(uuid4()),
                    "version": 1,
                    "timestamp": newer_timestamp,
                    "payload": "{}",
                },
            )

        result = await repo.get_lag_metrics(projection_name, event_types=["TestEvent"])
        assert result is not None
        assert result.last_event_id == str(stale_event_id)
        assert result.latest_event_id == str(newer_event_id)
        assert result.last_event_id != result.latest_event_id
        assert 12.0 <= result.lag_seconds <= 12.6
        assert result.lag_seconds == round(result.lag_seconds, 1)

    async def test_get_lag_metrics_sub_second_lag_is_not_clamped_to_zero(self, sqlite_engine):
        """A small positive raw lag (< 1 second) must be reported as-is, not
        clamped to 0 by an off-by-one boundary on the up-to-date check."""
        from datetime import UTC, timedelta
        from datetime import datetime as dt

        from eventsource.adapters.sql.checkpoints import SQLCheckpointRepository

        repo = SQLCheckpointRepository(sqlite_engine)
        projection_name = "TestProjection"
        stale_event_id = uuid4()
        newer_event_id = uuid4()

        await repo.update_checkpoint(projection_name, stale_event_id, "TestEvent")

        newer_timestamp = (dt.now(UTC) + timedelta(seconds=0.5)).isoformat()
        async with sqlite_engine.begin() as conn:
            await conn.execute(
                text("""
                    INSERT INTO events (event_id, event_type, aggregate_type, aggregate_id,
                                       version, timestamp, payload)
                    VALUES (:event_id, :event_type, :aggregate_type, :aggregate_id,
                            :version, :timestamp, :payload)
                    """),
                {
                    "event_id": str(newer_event_id),
                    "event_type": "TestEvent",
                    "aggregate_type": "TestAggregate",
                    "aggregate_id": str(uuid4()),
                    "version": 1,
                    "timestamp": newer_timestamp,
                    "payload": "{}",
                },
            )

        result = await repo.get_lag_metrics(projection_name, event_types=["TestEvent"])
        assert result is not None
        assert 0.0 < result.lag_seconds < 1.0

    async def test_get_lag_metrics_zero_when_no_relevant_event(self, sqlite_engine):
        """A checkpoint exists but no event matches the `event_types`
        filter, so `latest_event_time` is None and `raw_lag` never gets
        computed from a timestamp diff -- it must fall back to its default
        of 0.0, not some other sentinel."""
        from eventsource.adapters.sql.checkpoints import SQLCheckpointRepository

        repo = SQLCheckpointRepository(sqlite_engine)
        projection_name = "TestProjection"
        await repo.update_checkpoint(projection_name, uuid4(), "TestEvent")

        result = await repo.get_lag_metrics(projection_name, event_types=["NoSuchType"])
        assert result is not None
        assert result.latest_event_id is None
        assert result.lag_seconds == 0.0
