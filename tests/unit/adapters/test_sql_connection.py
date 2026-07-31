"""
Unit tests for the shared SQL connection helper.

Tests the sql_connection async context manager for:
- Handling AsyncEngine inputs with write=True
- Handling AsyncEngine inputs with write=False
- Passing through AsyncConnection inputs directly (never committed)
- Proper cleanup on success and on error
- Independent contexts across repeated engine uses
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from eventsource.adapters._sql.connection import sql_connection


class TestSqlConnection:
    """Tests for sql_connection context manager."""

    @pytest.mark.asyncio
    async def test_with_async_engine_write_true(self):
        """AsyncEngine input with write=True uses begin()."""
        mock_connection = AsyncMock()
        mock_engine = MagicMock()

        mock_begin_context = AsyncMock()
        mock_begin_context.__aenter__.return_value = mock_connection
        mock_begin_context.__aexit__.return_value = None
        mock_engine.begin.return_value = mock_begin_context

        with patch(
            "eventsource.adapters._sql.connection.isinstance",
            side_effect=lambda obj, cls: obj is mock_engine,
        ):
            async with sql_connection(mock_engine, write=True) as conn:
                assert conn is mock_connection

        mock_engine.begin.assert_called_once()
        assert not hasattr(mock_engine, "connect") or not mock_engine.connect.called

    @pytest.mark.asyncio
    async def test_with_async_engine_write_false(self):
        """AsyncEngine input with write=False uses connect()."""
        mock_connection = AsyncMock()
        mock_engine = MagicMock()

        mock_connect_context = AsyncMock()
        mock_connect_context.__aenter__.return_value = mock_connection
        mock_connect_context.__aexit__.return_value = None
        mock_engine.connect.return_value = mock_connect_context

        with patch(
            "eventsource.adapters._sql.connection.isinstance",
            side_effect=lambda obj, cls: obj is mock_engine,
        ):
            async with sql_connection(mock_engine, write=False) as conn:
                assert conn is mock_connection

        mock_engine.connect.assert_called_once()
        assert not hasattr(mock_engine, "begin") or not mock_engine.begin.called

    @pytest.mark.asyncio
    async def test_with_async_connection_write_true(self):
        """AsyncConnection input is yielded directly, not committed, write=True."""
        mock_connection = AsyncMock()

        async with sql_connection(mock_connection, write=True) as conn:
            assert conn is mock_connection

        assert not hasattr(mock_connection, "begin") or not mock_connection.begin.called
        assert not hasattr(mock_connection, "connect") or not mock_connection.connect.called
        assert not mock_connection.commit.called

    @pytest.mark.asyncio
    async def test_with_async_connection_write_false(self):
        """AsyncConnection input is yielded directly, not committed, write=False."""
        mock_connection = AsyncMock()

        async with sql_connection(mock_connection, write=False) as conn:
            assert conn is mock_connection

        assert not hasattr(mock_connection, "begin") or not mock_connection.begin.called
        assert not hasattr(mock_connection, "connect") or not mock_connection.connect.called
        assert not mock_connection.commit.called

    @pytest.mark.asyncio
    async def test_exception_propagates_and_engine_context_exits(self):
        """Exceptions propagate and the engine context manager still exits."""
        mock_connection = AsyncMock()
        mock_engine = MagicMock()

        exit_called = []

        async def mock_aexit(self, exc_type, exc_val, exc_tb):
            exit_called.append((exc_type, exc_val))
            return False

        mock_begin_context = AsyncMock()
        mock_begin_context.__aenter__.return_value = mock_connection
        mock_begin_context.__aexit__ = mock_aexit
        mock_engine.begin.return_value = mock_begin_context

        test_error = ValueError("Test error")

        with (
            patch(
                "eventsource.adapters._sql.connection.isinstance",
                side_effect=lambda obj, cls: obj is mock_engine,
            ),
            pytest.raises(ValueError, match="Test error"),
        ):
            async with sql_connection(mock_engine, write=True) as _:
                raise test_error

        assert len(exit_called) == 1
        assert exit_called[0][0] is ValueError
        assert exit_called[0][1] is test_error

    @pytest.mark.asyncio
    async def test_engine_creates_new_connection_each_time(self):
        """Two sequential uses of the same engine each get their own context."""
        connections = []

        def create_mock_context():
            mock_conn = AsyncMock()
            mock_conn.connection_id = len(connections)
            connections.append(mock_conn)

            mock_context = AsyncMock()
            mock_context.__aenter__.return_value = mock_conn
            mock_context.__aexit__.return_value = None
            return mock_context

        mock_engine = MagicMock()
        mock_engine.begin.side_effect = lambda: create_mock_context()

        with patch(
            "eventsource.adapters._sql.connection.isinstance",
            side_effect=lambda obj, cls: obj is mock_engine,
        ):
            async with sql_connection(mock_engine, write=True) as conn1:
                assert conn1.connection_id == 0

            async with sql_connection(mock_engine, write=True) as conn2:
                assert conn2.connection_id == 1

        assert len(connections) == 2
        assert mock_engine.begin.call_count == 2
