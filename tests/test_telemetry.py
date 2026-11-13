"""
Unit tests for telemetry.py — specifically the start_rpc_metrics helper.

The metric instruments are mocked so these tests run in CI with zero external infra.
"""

from unittest.mock import MagicMock

import pytest

# The real module is imported: with the telemetry variables unset (the default in CI) it loads no SDK and the
# instruments are API no-ops, which the fixture below replaces with mocks. Nothing is stubbed in sys.modules, so the
# other test files can use the real OpenTelemetry API/SDK.
import telemetry  # noqa: E402  (src/ must be on sys.path)


@pytest.fixture(autouse=True)
def _mock_instruments(monkeypatch):
    for name in ("rpc_server_duration", "rpc_server_requests_total", "rpc_server_active_requests"):
        monkeypatch.setattr(telemetry, name, MagicMock())


# ---------------------------------------------------------------------------
# Tests for start_rpc_metrics
# ---------------------------------------------------------------------------


class TestStartRpcMetrics:
    def test_returns_callable(self):
        end = telemetry.start_rpc_metrics("TestMethod")
        assert callable(end)

    def test_end_callable_accepts_status_code(self):
        end = telemetry.start_rpc_metrics("TestMethod")
        # Should not raise
        end("0")

    def test_end_callable_accepts_error_status(self):
        end = telemetry.start_rpc_metrics("TestMethod")
        end("13")  # INTERNAL

    def test_active_requests_incremented_on_start(self):
        counter = telemetry.rpc_server_active_requests
        counter.add.reset_mock()

        telemetry.start_rpc_metrics("MyMethod")

        first_call = counter.add.call_args_list[0]
        assert first_call[0][0] == 1  # value=1 (increment)

    def test_active_requests_decremented_on_end(self):
        counter = telemetry.rpc_server_active_requests
        counter.add.reset_mock()

        end = telemetry.start_rpc_metrics("MyMethod")
        end("0")

        calls = counter.add.call_args_list
        assert len(calls) == 2
        assert calls[0][0][0] == 1  # increment on start
        assert calls[1][0][0] == -1  # decrement on end

    def test_duration_recorded_on_end(self):
        hist = telemetry.rpc_server_duration
        hist.record.reset_mock()

        end = telemetry.start_rpc_metrics("MyMethod")
        end("0")

        hist.record.assert_called_once()
        elapsed = hist.record.call_args[0][0]
        assert elapsed >= 0

    def test_request_counter_incremented_on_end(self):
        ctr = telemetry.rpc_server_requests_total
        ctr.add.reset_mock()

        end = telemetry.start_rpc_metrics("MyMethod")
        end("0")

        ctr.add.assert_called_once()
        assert ctr.add.call_args[0][0] == 1

    def test_attributes_contain_method_name(self):
        counter = telemetry.rpc_server_active_requests
        counter.add.reset_mock()

        telemetry.start_rpc_metrics("SpecificMethodName")

        attrs = counter.add.call_args[0][1]
        assert attrs["rpc_method"] == "SpecificMethodName"

    def test_attributes_contain_service_name(self):
        counter = telemetry.rpc_server_active_requests
        counter.add.reset_mock()

        telemetry.start_rpc_metrics("AnyMethod")

        attrs = counter.add.call_args[0][1]
        assert "hipstershop.EmailService" in attrs["rpc_service"]

    def test_end_attributes_contain_grpc_status_code(self):
        hist = telemetry.rpc_server_duration
        hist.record.reset_mock()

        end = telemetry.start_rpc_metrics("AnyMethod")
        end("13")

        final_attrs = hist.record.call_args[0][1]
        assert final_attrs["rpc_grpc_status_code"] == "13"

    def test_independent_calls_do_not_share_state(self):
        """Two concurrent metric spans should track independently."""
        hist = telemetry.rpc_server_duration
        hist.record.reset_mock()

        end1 = telemetry.start_rpc_metrics("Method1")
        end2 = telemetry.start_rpc_metrics("Method2")
        end1("0")
        end2("0")

        assert hist.record.call_count == 2
        attrs1 = hist.record.call_args_list[0][0][1]
        attrs2 = hist.record.call_args_list[1][0][1]
        assert attrs1["rpc_method"] == "Method1"
        assert attrs2["rpc_method"] == "Method2"
