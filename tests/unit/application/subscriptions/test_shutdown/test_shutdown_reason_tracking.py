"""Unit tests for ShutdownReason tracking in ShutdownCoordinator."""

import asyncio
import signal
import sys
from unittest.mock import AsyncMock

import pytest

from eventsource.application.subscriptions.shutdown import (
    ShutdownCoordinator,
    ShutdownPhase,
    ShutdownReason,
    ShutdownResult,
)


class TestShutdownReasonTracking:
    """Tests for ShutdownReason tracking in ShutdownCoordinator."""

    @pytest.mark.asyncio
    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="Signal handling tests not supported on Windows",
    )
    async def test_sigterm_sets_reason(self):
        """Test SIGTERM sets correct reason."""
        coordinator = ShutdownCoordinator()
        await coordinator._handle_signal(signal.SIGTERM)
        assert coordinator._shutdown_reason == ShutdownReason.SIGNAL_SIGTERM

    @pytest.mark.asyncio
    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="Signal handling tests not supported on Windows",
    )
    async def test_sigint_sets_reason(self):
        """Test SIGINT sets correct reason."""
        coordinator = ShutdownCoordinator()
        await coordinator._handle_signal(signal.SIGINT)
        assert coordinator._shutdown_reason == ShutdownReason.SIGNAL_SIGINT

    def test_programmatic_shutdown_default_reason(self):
        """Test programmatic shutdown uses default reason."""
        coordinator = ShutdownCoordinator()
        coordinator.request_shutdown()
        assert coordinator._shutdown_reason == ShutdownReason.PROGRAMMATIC

    def test_programmatic_shutdown_custom_reason(self):
        """Test programmatic shutdown with custom reason."""
        coordinator = ShutdownCoordinator()
        coordinator.request_shutdown(ShutdownReason.HEALTH_CHECK)
        assert coordinator._shutdown_reason == ShutdownReason.HEALTH_CHECK

    @pytest.mark.asyncio
    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="Signal handling tests not supported on Windows",
    )
    async def test_double_signal_sets_reason(self):
        """Test double signal sets DOUBLE_SIGNAL reason."""
        coordinator = ShutdownCoordinator()
        await coordinator._handle_signal(signal.SIGTERM)
        await coordinator._handle_signal(signal.SIGTERM)  # Second signal
        assert coordinator._shutdown_reason == ShutdownReason.DOUBLE_SIGNAL

    def test_shutdown_result_includes_reason(self):
        """Test ShutdownResult includes reason in to_dict."""
        result = ShutdownResult(
            phase=ShutdownPhase.STOPPED,
            duration_seconds=5.0,
            subscriptions_stopped=1,
            events_drained=10,
            reason=ShutdownReason.SIGNAL_SIGTERM,
        )
        result_dict = result.to_dict()
        assert result_dict["reason"] == "signal_sigterm"

    def test_shutdown_result_none_reason(self):
        """Test ShutdownResult handles None reason."""
        result = ShutdownResult(
            phase=ShutdownPhase.STOPPED,
            duration_seconds=5.0,
            subscriptions_stopped=1,
            events_drained=10,
            reason=None,
        )
        result_dict = result.to_dict()
        assert result_dict["reason"] is None

    def test_reset_clears_reason(self):
        """Test reset clears shutdown reason."""
        coordinator = ShutdownCoordinator()
        coordinator.request_shutdown()
        coordinator.reset()
        assert coordinator._shutdown_reason is None

    @pytest.mark.asyncio
    async def test_programmatic_shutdown_reason_in_result(self):
        """Test programmatic shutdown reason is included in result."""
        coordinator = ShutdownCoordinator()
        coordinator.request_shutdown(ShutdownReason.HEALTH_CHECK)

        stop_func = AsyncMock()
        result = await coordinator.shutdown(stop_func=stop_func)

        assert result.reason == ShutdownReason.HEALTH_CHECK

    @pytest.mark.asyncio
    async def test_timeout_sets_reason(self):
        """Test timeout sets TIMEOUT reason."""
        coordinator = ShutdownCoordinator(drain_timeout=0.1)
        stop_func = AsyncMock()

        async def slow_drain():
            await asyncio.sleep(10.0)  # Will timeout
            return 0

        coordinator.request_shutdown()  # PROGRAMMATIC reason
        result = await coordinator.shutdown(
            stop_func=stop_func,
            drain_func=slow_drain,
        )

        # Reason should be TIMEOUT because drain timed out
        assert result.reason == ShutdownReason.TIMEOUT
        assert result.forced is True
