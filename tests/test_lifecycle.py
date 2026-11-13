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
    "LOG_LEVEL",
    "SERVICE_VERSION",
    "SERVICE_COMMIT",
    "PYROSCOPE_ADDR",
    "ENABLE_PROFILING",
)


# ---------------------------------------------------------------------------
# Contract (pure functions)
# ---------------------------------------------------------------------------


EP = "http://alloy:4317"
OTLP_CASES = [
    # (env, expected endpoint)
    ({}, None),
    ({"OTEL_EXPORTER_OTLP_ENDPOINT": ""}, None),
    ({"OTEL_EXPORTER_OTLP_ENDPOINT": "   "}, None),
    ({"OTEL_EXPORTER_OTLP_ENDPOINT": EP}, EP),
    ({"OTEL_EXPORTER_OTLP_ENDPOINT": f" {EP} "}, EP),
    ({"OTEL_EXPORTER_OTLP_ENDPOINT": "https://otel.example.com:4317"}, "https://otel.example.com:4317"),
    # malformed: off, never a crash
    ({"OTEL_EXPORTER_OTLP_ENDPOINT": "alloy:4317"}, None),
    ({"OTEL_EXPORTER_OTLP_ENDPOINT": "alloy"}, None),
    ({"OTEL_EXPORTER_OTLP_ENDPOINT": "http://"}, None),
    ({"OTEL_EXPORTER_OTLP_ENDPOINT": "ftp://alloy:4317"}, None),
    ({"OTEL_EXPORTER_OTLP_ENDPOINT": "http://alloy:notaport"}, None),
    ({"OTEL_EXPORTER_OTLP_ENDPOINT": "http://alloy:99999"}, None),
    ({"OTEL_EXPORTER_OTLP_ENDPOINT": "not a url"}, None),
    # OTEL_SDK_DISABLED=true (trimmed, case-insensitive) forces off; anything else does not
    ({"OTEL_EXPORTER_OTLP_ENDPOINT": EP, "OTEL_SDK_DISABLED": "true"}, None),
    ({"OTEL_EXPORTER_OTLP_ENDPOINT": EP, "OTEL_SDK_DISABLED": "TRUE"}, None),
    ({"OTEL_EXPORTER_OTLP_ENDPOINT": EP, "OTEL_SDK_DISABLED": " True "}, None),
    ({"OTEL_EXPORTER_OTLP_ENDPOINT": EP, "OTEL_SDK_DISABLED": "false"}, EP),
    ({"OTEL_EXPORTER_OTLP_ENDPOINT": EP, "OTEL_SDK_DISABLED": ""}, EP),
    # the legacy switches are gone: they neither turn telemetry on nor off
    ({"COLLECTOR_SERVICE_ADDR": "alloy:4317"}, None),
    ({"COLLECTOR_SERVICE_ADDR": "alloy:4317", "OTEL_EXPORTER_OTLP_ENDPOINT": EP}, EP),
    ({"OTEL_EXPORTER_OTLP_ENDPOINT": EP, "ENABLE_TRACING": "0"}, EP),
]

PY = "http://pyroscope:4040"
PYROSCOPE_CASES = [
    # (env, expected address)
    ({}, None),
    ({"PYROSCOPE_ADDR": ""}, None),
    ({"PYROSCOPE_ADDR": PY}, PY),
    ({"PYROSCOPE_ADDR": PY, "ENABLE_PROFILING": "1"}, PY),
    ({"ENABLE_PROFILING": "1"}, None),  # no default address
    ({"PYROSCOPE_ADDR": PY, "ENABLE_PROFILING": "0"}, None),
    ({"PYROSCOPE_ADDR": "pyroscope:4040"}, None),  # malformed
    ({"PYROSCOPE_ADDR": "http://"}, None),
    ({"PYROSCOPE_ADDR": PY, "OTEL_SDK_DISABLED": "true"}, None),  # "forces everything off"
    ({"PYROSCOPE_ADDR": PY, "OTEL_SDK_DISABLED": "false"}, PY),
]


@pytest.mark.parametrize(("env", "expected"), OTLP_CASES)
def test_otlp_endpoint_matrix(env, expected):
    assert telemetry.otlp_endpoint(env) == expected


@pytest.mark.parametrize(("env", "expected"), PYROSCOPE_CASES)
def test_pyroscope_address_matrix(env, expected):
    assert telemetry.pyroscope_address(env) == expected


def test_resource_attributes_defaults():
    attrs = telemetry.resource_attributes({"HOSTNAME": "pod-1"})
    assert attrs == {
        "service.name": "email-service",
        "service.version": "dev",
        "service.namespace": "everly",
        "deployment.environment": "local",
        "service.instance.id": "pod-1",
    }


def test_resource_attributes_from_env():
    env = {
        "OTEL_SERVICE_NAME": "x",
        "SERVICE_VERSION": " 1.2.3 ",
        "DEPLOYMENT_ENVIRONMENT": "dev",
        "HOSTNAME": "pod-2",
    }
    attrs = telemetry.resource_attributes(env)
    assert (attrs["service.name"], attrs["service.version"]) == ("x", "1.2.3")
    assert (attrs["deployment.environment"], attrs["service.instance.id"]) == ("dev", "pod-2")


def test_commit_default_and_env():
    assert telemetry.service_commit({}) == "unknown"
    assert telemetry.service_commit({"SERVICE_COMMIT": "abc1234"}) == "abc1234"


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
            str(ROOT / "proto" / "mailbox.proto"),
        ],
        check=True,
    )
    for name in ("email", "mailbox"):  # the same import fix the Dockerfile applies with sed
        stub = gen / f"{name}_pb2_grpc.py"
        stub.write_text(stub.read_text().replace(f"import {name}_pb2 as", f"from . import {name}_pb2 as"))
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
                email_pb2.SendOrderConfirmationRequest(
                    email="jane.doe@example.com", order=email_pb2.OrderResult(order_id='ord"1')
                ),
                timeout=5,
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
    assert all(set(r) >= {"timestamp", "severity", "message", "service", "version"} for r in records)
    assert all(r["timestamp"].endswith("Z") and "T" in r["timestamp"] for r in records)  # RFC3339 UTC
    assert all(r["severity"] in ("debug", "info", "warning", "error") for r in records)
    assert all("trace_id" not in r and "span_id" not in r for r in records)  # no span with telemetry off
    starting = next(r for r in records if r["message"] == "starting")
    assert starting["version"] == "dev" and starting["commit"] == "unknown"
    assert "telemetry off, profiling off" in starting["telemetry"]
    assert starting["service"] == "email-service"
    assert any('order=ord"1' in m for m in messages)  # the quote survived as valid JSON
    assert "jane.doe@example.com" not in stdout  # no recipient (PII) in logs
    assert "Received SIGTERM, shutting down" in messages
    assert messages[-1] == "EmailService stopped"


@pytest.mark.parametrize("endpoint", ["", "not-a-url", "alloy:4317", "http://"])
def test_malformed_or_empty_endpoint_means_off_not_a_crash(endpoint):
    code = "import telemetry; print(telemetry.TELEMETRY_ENABLED, telemetry.describe())"
    proc = subprocess.run(
        [sys.executable, "-c", code],
        cwd=ROOT / "src",
        env=_clean_env(OTEL_EXPORTER_OTLP_ENDPOINT=endpoint, PYROSCOPE_ADDR="garbage"),
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout.strip() == "False telemetry off, profiling off"


def test_uncaught_startup_error_is_one_json_line(app_dir, tmp_path):
    """Kafka on with a mailbox database that cannot be opened is fatal: exit 1 and a JSON line, no traceback."""
    proc = subprocess.run(
        [sys.executable, "src/server.py"],
        cwd=app_dir,
        env=_clean_env(
            PORT=str(_free_port()),
            PYTHONUNBUFFERED="1",
            KAFKA_BROKERS="127.0.0.1:1",
            MAILBOX_DB=str(tmp_path / "missing-dir" / "mailbox.db"),
        ),
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert proc.returncode == 1
    assert "Traceback" not in proc.stdout + proc.stderr
    records = [json.loads(line) for line in proc.stdout.splitlines()]
    assert records[-1]["severity"] == "error"
    assert records[-1]["message"] == "uncaught exception"
    assert records[-1]["error"]


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
