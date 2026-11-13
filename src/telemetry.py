"""Telemetry bootstrap. Import this module first: when telemetry is on it patches grpc before the server is built.

Contract (same as the Go services, OBS-01/02/04):
- Traces, metrics and logs are exported over OTLP/gRPC only when OTEL_EXPORTER_OTLP_ENDPOINT is a valid absolute
  http(s) URL. An empty or malformed value means off (never a crash). OTEL_SDK_DISABLED=true (trimmed, any case)
  forces everything off, profiling included. With telemetry off nothing is exported and the metric instruments below
  are OpenTelemetry API no-ops. The exporters are asynchronous (batch processors with bounded queues), startup never
  waits for the collector.
- Profiling (Pyroscope) is on only when PYROSCOPE_ADDR is a valid absolute http(s) URL; ENABLE_PROFILING=0 forces it
  off, ENABLE_PROFILING=1 alone starts nothing (there is no default address).
- Resource attributes on every signal: service.name, service.version, service.namespace=everly,
  deployment.environment, service.instance.id. Pyroscope tags: service_name, version, environment.
"""

import atexit
import contextlib
import logging
import os
import socket
import threading
import time
from collections.abc import Mapping
from urllib.parse import urlparse

from opentelemetry import metrics

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SERVICE_NAMESPACE = "everly"
UNKNOWN = "unknown"


def service_name(env: Mapping[str, str]) -> str:
    return env.get("OTEL_SERVICE_NAME", "").strip() or "email-service"


def service_version(env: Mapping[str, str]) -> str:
    """SERVICE_VERSION, else "dev" (this service has no package version and no build info of its own)."""
    return env.get("SERVICE_VERSION", "").strip() or "dev"


def service_commit(env: Mapping[str, str]) -> str:
    """SERVICE_COMMIT (set by the build/deploy), else "unknown"."""
    return env.get("SERVICE_COMMIT", "").strip() or UNKNOWN


def deployment_environment(env: Mapping[str, str]) -> str:
    return env.get("DEPLOYMENT_ENVIRONMENT", "").strip() or "local"


def instance_id(env: Mapping[str, str]) -> str:
    # Distinguishes replicas: in Kubernetes the hostname is the pod name.
    return env.get("HOSTNAME", "").strip() or socket.gethostname()


def resource_attributes(env: Mapping[str, str]) -> dict[str, str]:
    """The resource attributes of every signal (OBS-02)."""
    return {
        "service.name": service_name(env),
        "service.version": service_version(env),
        "service.namespace": SERVICE_NAMESPACE,
        "deployment.environment": deployment_environment(env),
        "service.instance.id": instance_id(env),
    }


def _valid_http_url(value: str) -> bool:
    try:
        parsed = urlparse(value)
        _ = parsed.port  # raises ValueError for a port that is not a number or out of range
    except ValueError:
        return False
    return parsed.scheme in ("http", "https") and bool(parsed.hostname)


def sdk_disabled(env: Mapping[str, str]) -> bool:
    return env.get("OTEL_SDK_DISABLED", "").strip().lower() == "true"


def otlp_endpoint(env: Mapping[str, str]) -> str | None:
    """OTLP/gRPC endpoint (scheme included) or None when telemetry is off."""
    if sdk_disabled(env):
        return None
    endpoint = env.get("OTEL_EXPORTER_OTLP_ENDPOINT", "").strip()
    return endpoint if _valid_http_url(endpoint) else None


def pyroscope_address(env: Mapping[str, str]) -> str | None:
    """Pyroscope server address or None when profiling is off."""
    addr = env.get("PYROSCOPE_ADDR", "").strip()
    if sdk_disabled(env) or env.get("ENABLE_PROFILING", "").strip() == "0":
        return None
    return addr if _valid_http_url(addr) else None


_SERVICE_NAME = service_name(os.environ)
_SERVICE_VERSION = service_version(os.environ)
_SERVICE_COMMIT = service_commit(os.environ)
_ENVIRONMENT = deployment_environment(os.environ)

OTLP_ENDPOINT = otlp_endpoint(os.environ)
PYROSCOPE_ADDR = pyroscope_address(os.environ)
TELEMETRY_ENABLED = OTLP_ENDPOINT is not None
PROFILING_ENABLED = PYROSCOPE_ADDR is not None

_providers = []  # SDK providers to flush on shutdown


def _setup_otel(endpoint: str) -> None:
    # Imported lazily so a service with telemetry off never loads the SDK or the exporters.
    from opentelemetry import trace
    from opentelemetry._logs import set_logger_provider
    from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
    from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    from opentelemetry.instrumentation.grpc import GrpcInstrumentorServer
    from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
    from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics._internal.aggregation import ExplicitBucketHistogramAggregation
    from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
    from opentelemetry.sdk.metrics.view import View
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    insecure = endpoint.startswith("http://")
    export_interval_ms = int(os.environ.get("OTEL_METRIC_EXPORT_INTERVAL", "15000"))

    resource = Resource.create(resource_attributes(os.environ))

    # Traces (OTLP gRPC → Alloy → Tempo)
    tracer_provider = TracerProvider(resource=resource, shutdown_on_exit=False)
    tracer_provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint, insecure=insecure)))
    trace.set_tracer_provider(tracer_provider)

    # Metrics (OTLP gRPC push → Alloy → Mimir)
    duration_view = View(
        instrument_name="rpc_server_duration",
        aggregation=ExplicitBucketHistogramAggregation(
            boundaries=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10],
        ),
    )
    meter_provider = MeterProvider(
        resource=resource,
        metric_readers=[
            PeriodicExportingMetricReader(
                OTLPMetricExporter(endpoint=endpoint, insecure=insecure),
                export_interval_millis=export_interval_ms,
            )
        ],
        views=[duration_view],
        shutdown_on_exit=False,
    )
    metrics.set_meter_provider(meter_provider)

    # Logs (OTLP gRPC → Alloy → Loki). The service logger keeps writing JSON to stdout;
    # this handler additionally ships records (with trace/span ids) over OTLP.
    logger_provider = LoggerProvider(resource=resource, shutdown_on_exit=False)
    logger_provider.add_log_record_processor(
        BatchLogRecordProcessor(OTLPLogExporter(endpoint=endpoint, insecure=insecure))
    )
    set_logger_provider(logger_provider)
    logging.getLogger(_SERVICE_NAME).addHandler(LoggingHandler(level=logging.INFO, logger_provider=logger_provider))

    # gRPC auto-instrumentation (patches grpc before the server starts)
    GrpcInstrumentorServer().instrument()

    _providers.extend([tracer_provider, meter_provider, logger_provider])


def _setup_pyroscope(address: str) -> None:
    # Pyroscope SDK push protocol (not OTLP); PYROSCOPE_ADDR may point at Alloy.
    import pyroscope

    pyroscope.configure(
        application_name=_SERVICE_NAME,
        server_address=address,
        tags={"service_name": _SERVICE_NAME, "version": _SERVICE_VERSION, "environment": _ENVIRONMENT},
    )


if TELEMETRY_ENABLED:
    _setup_otel(OTLP_ENDPOINT)
if PROFILING_ENABLED:
    _setup_pyroscope(PYROSCOPE_ADDR)


def describe() -> str:
    """One line for the startup log."""
    tel = f"telemetry on (OTLP {OTLP_ENDPOINT})" if TELEMETRY_ENABLED else "telemetry off"
    prof = f"profiling on (Pyroscope {PYROSCOPE_ADDR})" if PROFILING_ENABLED else "profiling off"
    return f"{tel}, {prof}"


# Upper bound for the whole telemetry shutdown. The SDK's own exit hooks are disabled (shutdown_on_exit=False)
# because with an unreachable collector their retries kept the process alive for ~20 s after SIGTERM.
SHUTDOWN_TIMEOUT_SECONDS = 2.0
_shutdown_lock = threading.Lock()
_shutdown_done = False


def shutdown(timeout_seconds: float = SHUTDOWN_TIMEOUT_SECONDS) -> None:
    """Flush and stop the exporters and the profiler, waiting at most `timeout_seconds`. Idempotent.

    A no-op with telemetry and profiling off. Also registered with atexit, so the last batch goes out on any exit.
    """
    global _shutdown_done
    with _shutdown_lock:
        if _shutdown_done:
            return
        _shutdown_done = True
    if not _providers and not PROFILING_ENABLED:
        return

    def _run():
        # Best effort: a failing exporter must not turn a clean shutdown into a crash.
        for provider in _providers:  # shutdown() exports what is still buffered
            with contextlib.suppress(Exception):
                provider.shutdown()
        if PROFILING_ENABLED:
            with contextlib.suppress(Exception):
                import pyroscope

                pyroscope.shutdown()

    worker = threading.Thread(target=_run, name="telemetry-shutdown", daemon=True)
    worker.start()
    worker.join(timeout_seconds)


atexit.register(shutdown)


# ---------------------------------------------------------------------------
# Custom gRPC server metrics (no-op instruments when telemetry is off)
# ---------------------------------------------------------------------------

meter = metrics.get_meter("email-service-grpc")

rpc_server_duration = meter.create_histogram(
    name="rpc_server_duration",
    description="Duration of inbound gRPC calls in seconds",
    unit="s",
)

rpc_server_requests_total = meter.create_counter(
    name="rpc_server_requests_total",
    description="Total number of inbound gRPC calls",
)

rpc_server_active_requests = meter.create_up_down_counter(
    name="rpc_server_active_requests",
    description="Number of in-flight gRPC calls",
)


def start_rpc_metrics(method: str, service: str = "hipstershop.EmailService"):
    """
    Call at the start of each gRPC handler.
    Returns a callable: end(grpc_status_code_name) to record metrics.
    """
    start_time = time.monotonic()
    attrs = {
        "rpc_system": "grpc",
        "rpc_service": service,
        "rpc_method": method,
    }
    rpc_server_active_requests.add(1, attrs)

    def end(grpc_status_code: str):
        elapsed = time.monotonic() - start_time
        final_attrs = {**attrs, "rpc_grpc_status_code": grpc_status_code}
        rpc_server_duration.record(elapsed, final_attrs)
        rpc_server_requests_total.add(1, final_attrs)
        rpc_server_active_requests.add(-1, attrs)

    return end
