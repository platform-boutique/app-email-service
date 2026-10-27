"""
Telemetry contract, JSON log lines and SIGTERM handling.

The contract tests call the pure helpers in telemetry.py. The process tests run the REAL modules in a subprocess
(no stubs): they copy src/ and templates/ to a temp dir, compile the proto there (what the Dockerfile does), start
the server with every telemetry variable unset, call it, send SIGTERM and check the exit code and the log lines.
"""

import importlib.util
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent


def _load_telemetry():
    # Private module name: the other test files stub opentelemetry and import `telemetry` themselves, so this file
    # must not leave a real `telemetry` in sys.modules.
    spec = importlib.util.spec_from_file_location("_telemetry_contract", ROOT / "src" / "telemetry.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


telemetry = _load_telemetry()
_TELEMETRY_VARS = (
    "OTEL_EXPORTER_OTLP_ENDPOINT",
    "COLLECTOR_SERVICE_ADDR",
    "OTEL_SDK_DISABLED",
    "ENABLE_TRACING",
    "PYROSCOPE_ADDR",
    "ENABLE_PROFILING",
)


# ---------------------------------------------------------------------------
# Contract (pure functions)
# ---------------------------------------------------------------------------


class TestOtlpEndpoint:
    def test_unset_is_off(self):
        assert telemetry.otlp_endpoint({}) is None

    def test_empty_is_off(self):
        assert telemetry.otlp_endpoint({"OTEL_EXPORTER_OTLP_ENDPOINT": "", "COLLECTOR_SERVICE_ADDR": " "}) is None

    def test_endpoint_is_used(self):
        assert telemetry.otlp_endpoint({"OTEL_EXPORTER_OTLP_ENDPOINT": "http://alloy:4317"}) == "http://alloy:4317"

    def test_legacy_collector_addr(self):
        assert telemetry.otlp_endpoint({"COLLECTOR_SERVICE_ADDR": "alloy:4317"}) == "http://alloy:4317"

    def test_endpoint_wins_over_legacy(self):
        env = {"OTEL_EXPORTER_OTLP_ENDPOINT": "http://a:4317", "COLLECTOR_SERVICE_ADDR": "b:4317"}
        assert telemetry.otlp_endpoint(env) == "http://a:4317"

    @pytest.mark.parametrize("value", ["true", "TRUE", " True "])
    def test_sdk_disabled_forces_off(self, value):
        env = {"OTEL_EXPORTER_OTLP_ENDPOINT": "http://alloy:4317", "OTEL_SDK_DISABLED": value}
        assert telemetry.otlp_endpoint(env) is None

    def test_sdk_disabled_false_keeps_on(self):
        env = {"OTEL_EXPORTER_OTLP_ENDPOINT": "http://alloy:4317", "OTEL_SDK_DISABLED": "false"}
        assert telemetry.otlp_endpoint(env) == "http://alloy:4317"

    def test_legacy_enable_tracing_0_forces_off(self):
        env = {"OTEL_EXPORTER_OTLP_ENDPOINT": "http://alloy:4317", "ENABLE_TRACING": "0"}
        assert telemetry.otlp_endpoint(env) is None


class TestPyroscopeAddress:
    def test_unset_is_off(self):
        assert telemetry.pyroscope_address({}) is None

    def test_empty_is_off(self):
        assert telemetry.pyroscope_address({"PYROSCOPE_ADDR": ""}) is None

    def test_set_is_on(self):
        assert telemetry.pyroscope_address({"PYROSCOPE_ADDR": "http://alloy:4040"}) == "http://alloy:4040"

    def test_set_is_on_with_enable_profiling_1(self):
        env = {"PYROSCOPE_ADDR": "http://alloy:4040", "ENABLE_PROFILING": "1"}
        assert telemetry.pyroscope_address(env) == "http://alloy:4040"

    def test_enable_profiling_0_forces_off(self):
        assert telemetry.pyroscope_address({"PYROSCOPE_ADDR": "http://alloy:4040", "ENABLE_PROFILING": "0"}) is None

    def test_enable_profiling_alone_is_off(self):
        assert telemetry.pyroscope_address({"ENABLE_PROFILING": "1"}) is None


# ---------------------------------------------------------------------------
# Real process
# ---------------------------------------------------------------------------


def _clean_env(**extra):
    env = {k: v for k, v in os.environ.items() if k not in _TELEMETRY_VARS}
    env.update(extra)
    return env


def test_real_import_with_everything_unset_loads_no_sdk():
    """Off means off: no exporter or SDK module is imported, nothing is printed."""
    code = (
        "import sys, telemetry; "
        "print(telemetry.TELEMETRY_ENABLED, telemetry.PROFILING_ENABLED, "
        "any(m.startswith(('opentelemetry.sdk', 'opentelemetry.exporter', 'pyroscope')) for m in sys.modules)); "
        "telemetry.start_rpc_metrics('X')('0'); telemetry.shutdown()"
    )
    proc = subprocess.run(
        [sys.executable, "-c", code], cwd=ROOT / "src", env=_clean_env(), capture_output=True, text=True, timeout=30
    )
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout.split() == ["False", "False", "False"]
    assert proc.stderr == ""


def _free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def test_shutdown_is_bounded_when_the_collector_is_unreachable():
    """Telemetry on, nobody listening: exporter retries must not keep the process alive (it used to take ~20 s)."""
    code = (
        "import telemetry; from opentelemetry import trace; "
        "assert telemetry.TELEMETRY_ENABLED; "
        "trace.get_tracer('t').start_span('s').end(); "
        "telemetry.start_rpc_metrics('X')('0'); telemetry.shutdown()"
    )
    env = _clean_env(OTEL_EXPORTER_OTLP_ENDPOINT=f"http://127.0.0.1:{_free_port()}")
    started = time.monotonic()
    proc = subprocess.run(
        [sys.executable, "-c", code], cwd=ROOT / "src", env=env, capture_output=True, text=True, timeout=30
    )
    elapsed = time.monotonic() - started
    assert proc.returncode == 0, proc.stderr
    assert elapsed < telemetry.SHUTDOWN_TIMEOUT_SECONDS + 3  # + interpreter start and SDK import


@pytest.fixture(scope="module")
def app_dir(tmp_path_factory):
    pytest.importorskip("grpc_tools")
    app = tmp_path_factory.mktemp("app")
    shutil.copytree(ROOT / "src", app / "src", ignore=shutil.ignore_patterns("__pycache__"))
    shutil.copytree(ROOT / "templates", app / "templates")
    gen = app / "src" / "generated"
    subprocess.run(
        [
            sys.executable,
            "-m",
            "grpc_tools.protoc",
            f"-I{ROOT / 'proto'}",
            f"--python_out={gen}",
            f"--grpc_python_out={gen}",
            str(ROOT / "proto" / "email.proto"),
        ],
        check=True,
    )
    stub = gen / "email_pb2_grpc.py"
    stub.write_text(stub.read_text().replace("import email_pb2 as email__pb2", "from . import email_pb2 as email__pb2"))
    return app


def test_starts_with_telemetry_unset_logs_json_and_exits_0_on_sigterm(app_dir):
    import grpc

    port = _free_port()
    proc = subprocess.Popen(
        [sys.executable, "src/server.py"],
        cwd=app_dir,
        env=_clean_env(PORT=str(port), PYTHONUNBUFFERED="1"),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        for name in ("generated", "generated.email_pb2", "generated.email_pb2_grpc"):
            sys.modules.pop(name, None)
        sys.path.insert(0, str(app_dir / "src"))
        try:
            from generated import email_pb2, email_pb2_grpc
        finally:
            sys.path.pop(0)
            for name in ("generated", "generated.email_pb2", "generated.email_pb2_grpc"):
                sys.modules.pop(name, None)

        with grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
            grpc.channel_ready_future(channel).result(timeout=15)
            # A quote in the message used to produce an invalid JSON log line.
            email_pb2_grpc.EmailServiceStub(channel).SendOrderConfirmation(
                email_pb2.SendOrderConfirmationRequest(email='quote"d@example.com'), timeout=5
            )

        proc.send_signal(signal.SIGTERM)
        started = time.monotonic()
        stdout, stderr = proc.communicate(timeout=10)
        elapsed = time.monotonic() - started
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.communicate()

    assert proc.returncode == 0, stderr
    assert elapsed < 5  # 2 s drain + 2 s telemetry flush at most; idle it is well under a second
    assert stderr == ""  # nothing noisy (no exporter retries) with telemetry off
    records = [json.loads(line) for line in stdout.splitlines()]  # every line is valid JSON
    messages = [r["message"] for r in records]
    assert all(set(r) >= {"timestamp", "severity", "name", "message"} for r in records)
    assert any("telemetry off, profiling off" in m for m in messages)
    assert any('quote"d@example.com' in m for m in messages)
    assert "Received SIGTERM, shutting down" in messages
    assert messages[-1] == "EmailService stopped"


def test_exits_0_on_sigterm_with_otlp_and_pyroscope_pointed_at_a_collector_that_never_answers(app_dir):
    """Chart defaults set both OTEL_EXPORTER_OTLP_ENDPOINT and PYROSCOPE_ADDR; behind a dropping NetworkPolicy the
    collector accepts nothing useful. The process used to hang in interpreter finalization until SIGKILL (exit 137)."""
    import grpc

    with socket.socket() as blackhole:  # accepts connections (backlog), never reads or answers
        blackhole.bind(("127.0.0.1", 0))
        blackhole.listen(32)
        address = f"http://127.0.0.1:{blackhole.getsockname()[1]}"
        port = _free_port()
        proc = subprocess.Popen(
            [sys.executable, "src/server.py"],
            cwd=app_dir,
            env=_clean_env(
                PORT=str(port),
                PYTHONUNBUFFERED="1",
                OTEL_EXPORTER_OTLP_ENDPOINT=address,
                PYROSCOPE_ADDR=address,
            ),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            with grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
                grpc.channel_ready_future(channel).result(timeout=15)
            time.sleep(3)  # let the exporters connect and start waiting for answers
            proc.send_signal(signal.SIGTERM)
            started = time.monotonic()
            proc.communicate(timeout=10)
            elapsed = time.monotonic() - started
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.communicate()
    assert proc.returncode == 0
    assert elapsed < 5
