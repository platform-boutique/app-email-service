"""
Unit tests for telemetry.start_rpc_metrics (instruments replaced by mocks, no external infra).
"""

from unittest.mock import MagicMock

import pytest

import telemetry  # noqa: E402  (src/ must be on sys.path; the real module, nothing is stubbed in sys.modules)

_duration = MagicMock()
_requests_total = MagicMock()
_active_requests = MagicMock()


@pytest.fixture(autouse=True)
def _mock_instruments(monkeypatch):
    """Replace the API no-op instruments with mocks; fresh call history for every test."""
    for mock in (_duration, _requests_total, _active_requests):
        mock.reset_mock()
    monkeypatch.setattr(telemetry, "rpc_server_duration", _duration)
    monkeypatch.setattr(telemetry, "rpc_server_requests_total", _requests_total)
    monkeypatch.setattr(telemetry, "rpc_server_active_requests", _active_requests)


# ---------------------------------------------------------------------------
# Tests for start_rpc_metrics
# ---------------------------------------------------------------------------


class TestStartRpcMetrics:
    def setup_method(self):
        """Reset call history on the shared mock instruments before each test."""
        _duration.record.reset_mock()
        _requests_total.add.reset_mock()
        _active_requests.add.reset_mock()

    def test_returns_callable(self):
        end = telemetry.start_rpc_metrics("TestMethod")
        assert callable(end)

    def test_end_callable_accepts_status_code(self):
        end = telemetry.start_rpc_metrics("TestMethod")
        end("0")  # should not raise

    def test_end_callable_accepts_error_status(self):
        end = telemetry.start_rpc_metrics("TestMethod")
        end("13")  # INTERNAL — should not raise

    def test_active_requests_incremented_on_start(self):
        telemetry.start_rpc_metrics("MyMethod")
        first_call = _active_requests.add.call_args_list[0]
        assert first_call[0][0] == 1

    def test_active_requests_decremented_on_end(self):
        end = telemetry.start_rpc_metrics("MyMethod")
        end("0")
        calls = _active_requests.add.call_args_list
        assert len(calls) == 2
        assert calls[0][0][0] == 1  # increment on start
        assert calls[1][0][0] == -1  # decrement on end

    def test_duration_recorded_on_end(self):
        end = telemetry.start_rpc_metrics("MyMethod")
        end("0")
        _duration.record.assert_called_once()
        elapsed = _duration.record.call_args[0][0]
        assert elapsed >= 0

    def test_request_counter_incremented_on_end(self):
        end = telemetry.start_rpc_metrics("MyMethod")
        end("0")
        _requests_total.add.assert_called_once()
        assert _requests_total.add.call_args[0][0] == 1

    def test_attributes_contain_method_name(self):
        telemetry.start_rpc_metrics("SpecificMethodName")
        attrs = _active_requests.add.call_args[0][1]
        assert attrs["rpc_method"] == "SpecificMethodName"

    def test_attributes_contain_service_name(self):
        telemetry.start_rpc_metrics("AnyMethod")
        attrs = _active_requests.add.call_args[0][1]
        assert "hipstershop.EmailService" in attrs["rpc_service"]

    def test_end_attributes_contain_grpc_status_code(self):
        end = telemetry.start_rpc_metrics("AnyMethod")
        end("13")
        final_attrs = _duration.record.call_args[0][1]
        assert final_attrs["rpc_grpc_status_code"] == "13"

    def test_independent_calls_do_not_share_state(self):
        """Two concurrent metric spans must track their method names independently."""
        end1 = telemetry.start_rpc_metrics("Method1")
        end2 = telemetry.start_rpc_metrics("Method2")
        end1("0")
        end2("0")

        assert _duration.record.call_count == 2
        attrs1 = _duration.record.call_args_list[0][0][1]
        attrs2 = _duration.record.call_args_list[1][0][1]
        assert attrs1["rpc_method"] == "Method1"
        assert attrs2["rpc_method"] == "Method2"
