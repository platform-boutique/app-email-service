"""Log shape (OBS-30), trace correlation of logs, and Kafka trace-header propagation (OBS-61).

The real OpenTelemetry API and SDK are used with an in-memory span exporter; the global tracer provider is not
touched (it can only be set once per process), spans come from a provider created per test.
"""

import io
import json
import logging
import re
import threading
from concurrent import futures
from types import SimpleNamespace

import grpc
import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

import kafka_trace
import logsetup
from tests.conftest import event_bytes
from tests.test_consumer import FakeKafka, make_consumer, run_until

TRACE_ID = re.compile(r"^[0-9a-f]{32}$")
SPAN_ID = re.compile(r"^[0-9a-f]{16}$")
RFC3339_MS = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$")


@pytest.fixture()
def tracing():
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    yield SimpleNamespace(provider=provider, tracer=provider.get_tracer("test"), exporter=exporter)
    provider.shutdown()


@pytest.fixture()
def logs():
    """A logger wired like server.py, writing to a buffer; lines() parses what was written."""
    stream = io.StringIO()
    logger = logging.getLogger("email-service-obs-test")
    logger.handlers = [logsetup.make_handler("email-service", "1.2.3", stream)]
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    yield SimpleNamespace(logger=logger, lines=lambda: [json.loads(x) for x in stream.getvalue().splitlines()])
    logger.handlers = []


# ---------------------------------------------------------------------------
# log shape
# ---------------------------------------------------------------------------


def test_golden_line_outside_a_span_has_no_trace_ids():
    record = logging.LogRecord("email-service", logging.WARNING, __file__, 1, "skipping record %s", ("a[0]@1",), None)
    record.created = 1_700_000_000.123456  # injected clock
    line = logsetup.JsonFormatter("email-service", "1.2.3").format(record)
    assert json.loads(line) == {
        "timestamp": "2023-11-14T22:13:20.123Z",
        "severity": "warning",
        "message": "skipping record a[0]@1",
        "service": "email-service",
        "version": "1.2.3",
    }
    assert "\n" not in line


@pytest.mark.parametrize(
    ("level", "severity"),
    [
        (logging.DEBUG, "debug"),
        (logging.INFO, "info"),
        (logging.WARNING, "warning"),
        (logging.ERROR, "error"),
        (logging.CRITICAL, "error"),
    ],
)
def test_severity_is_lowercase_and_one_of_four(logs, level, severity):
    logs.logger.log(level, "x")
    rec = logs.lines()[0]
    assert rec["severity"] == severity and RFC3339_MS.match(rec["timestamp"])


def test_error_field_from_extra_and_from_exception(logs):
    logs.logger.error("failed", extra={"error": "boom"})
    try:
        raise ValueError("bad value")
    except ValueError:
        logs.logger.error("failed too", exc_info=True)
    first, second = logs.lines()
    assert first["error"] == "boom"
    assert second["error"] == "ValueError: bad value"
    assert "Traceback" not in json.dumps(second)


def test_fields_extra_is_merged_and_quotes_stay_valid_json(logs):
    logs.logger.info('say "hi"\nnow', extra={"fields": {"commit": "abc"}})
    rec = logs.lines()[0]
    assert rec["message"] == 'say "hi"\nnow' and rec["commit"] == "abc"


def test_trace_ids_are_present_inside_a_span_and_absent_outside(logs, tracing):
    logs.logger.info("before")
    with tracing.tracer.start_as_current_span("work") as span:
        logs.logger.info("inside")
        ctx = span.get_span_context()
    logs.logger.info("after")
    before, inside, after = logs.lines()
    assert "trace_id" not in before and "span_id" not in before
    assert "trace_id" not in after and "span_id" not in after
    assert inside["trace_id"] == trace.format_trace_id(ctx.trace_id) and TRACE_ID.match(inside["trace_id"])
    assert inside["span_id"] == trace.format_span_id(ctx.span_id) and SPAN_ID.match(inside["span_id"])
    assert set(inside["trace_id"]) != {"0"}


def test_non_recording_invalid_span_writes_no_ids(logs):
    with trace.use_span(trace.NonRecordingSpan(trace.INVALID_SPAN_CONTEXT)):
        logs.logger.info("x")
    assert "trace_id" not in logs.lines()[0]


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, logging.INFO),
        ("", logging.INFO),
        ("debug", logging.DEBUG),
        ("INFO", logging.INFO),
        (" Warning ", logging.WARNING),
        ("warn", logging.WARNING),
        ("error", logging.ERROR),
        ("verbose", logging.INFO),
    ],
)
def test_log_level_from_env(value, expected):
    env = {} if value is None else {"LOG_LEVEL": value}
    assert logsetup.log_level(env) == expected


# ---------------------------------------------------------------------------
# a log line written while a gRPC call is handled carries that call's trace
# ---------------------------------------------------------------------------


def test_grpc_handler_logs_carry_the_incoming_trace_id(logs, tracing):
    from opentelemetry.instrumentation.grpc import GrpcInstrumentorServer

    def echo(request, context):
        logs.logger.info("handling")
        return request

    handler = grpc.method_handlers_generic_handler(
        "t.Echo", {"Say": grpc.unary_unary_rpc_method_handler(echo, lambda b: b, lambda b: b)}
    )
    instrumentor = GrpcInstrumentorServer()
    instrumentor.instrument(tracer_provider=tracing.provider)
    try:
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
        server.add_generic_rpc_handlers((handler,))
        port = server.add_insecure_port("127.0.0.1:0")
        server.start()
        try:
            traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
            with grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
                call = channel.unary_unary(
                    "/t.Echo/Say", request_serializer=lambda b: b, response_deserializer=lambda b: b
                )
                assert call(b"hi", metadata=(("traceparent", traceparent),), timeout=10) == b"hi"
        finally:
            server.stop(0).wait()
    finally:
        instrumentor.uninstrument()
    line = logs.lines()[0]
    assert line["trace_id"] == "0af7651916cd43dd8448eb211c80319c"
    assert SPAN_ID.match(line["span_id"]) and line["span_id"] != "b7ad6b7169203331"  # the server span, a child
    (span,) = tracing.exporter.get_finished_spans()
    assert span.kind == SpanKind.SERVER and trace.format_span_id(span.context.span_id) == line["span_id"]


# ---------------------------------------------------------------------------
# Kafka headers
# ---------------------------------------------------------------------------


def _producer_headers(tracer):
    """What checkout does: inject the current context into the record headers as (key, bytes)."""
    carrier: dict[str, str] = {}
    with tracer.start_as_current_span("orders.placed publish", kind=SpanKind.PRODUCER) as span:
        TraceContextTextMapPropagator().inject(carrier)
        ctx = span.get_span_context()
    return [(k, v.encode()) for k, v in carrier.items()], ctx


def _record(headers=None, **kw):
    return SimpleNamespace(
        topic="orders.placed", partition=2, offset=7, value=event_bytes(), headers=headers, leader_epoch=None, **kw
    )


def test_round_trip_producer_header_to_consumer_span(tracing):
    headers, producer = _producer_headers(tracing.tracer)
    assert [k for k, _ in headers][0] == "traceparent"
    with kafka_trace.consumer_span(_record(headers), "email-service", tracer=tracing.tracer) as span:
        assert trace.get_current_span() is span
    consumer = tracing.exporter.get_finished_spans()[-1]
    assert consumer.context.trace_id == producer.trace_id  # ONE trace
    assert consumer.parent.span_id == producer.span_id  # child of the producer span
    assert consumer.kind == SpanKind.CONSUMER
    assert consumer.name == "orders.placed process"
    expected = {
        "messaging.system": "kafka",
        "messaging.destination.name": "orders.placed",
        "messaging.operation": "process",
        "messaging.consumer.group.name": "email-service",
    }
    assert expected.items() <= dict(consumer.attributes).items()


@pytest.mark.parametrize(
    "headers",
    [
        None,
        [],
        [("event-type", b"order.placed")],  # other headers only
        [("traceparent", b"garbage")],
        [("traceparent", b"00-00000000000000000000000000000000-0000000000000000-01")],  # all zero
        [("traceparent", b"\xff\xfe")],  # not UTF-8
        [("traceparent", None)],  # kafka allows a null header value
        ["not-a-pair"],
        [("traceparent", b"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"), "junk"],
    ],
)
def test_missing_or_malformed_headers_never_break_consumption(tracing, headers):
    with kafka_trace.consumer_span(_record(headers), "g", tracer=tracing.tracer):
        pass
    (span,) = tracing.exporter.get_finished_spans()
    assert span.kind == SpanKind.CONSUMER
    if not any(isinstance(h, tuple) and h[1] and h[1].startswith(b"00-0af7") for h in headers or []):
        assert span.parent is None  # a new root trace


def test_header_keys_are_case_insensitive(tracing):
    headers, producer = _producer_headers(tracing.tracer)
    upper = [(k.upper(), v) for k, v in headers]
    with kafka_trace.consumer_span(_record(upper), "g", tracer=tracing.tracer):
        pass
    assert tracing.exporter.get_finished_spans()[-1].context.trace_id == producer.trace_id


def test_consumer_run_processes_in_the_producer_trace_and_logs_its_ids(store, tracing, monkeypatch):
    headers, producer = _producer_headers(tracing.tracer)
    monkeypatch.setattr(kafka_trace.trace, "get_tracer", lambda *a, **k: tracing.tracer)
    stream = io.StringIO()
    service_logger = logging.getLogger("email-service")
    handler = logsetup.make_handler("email-service", "t", stream)
    service_logger.addHandler(handler)
    service_logger.setLevel(logging.INFO)
    try:
        stop = threading.Event()
        fake = FakeKafka([event_bytes("A")])
        fake.queue[0].headers = headers
        run_until(make_consumer(store, fake, stop), lambda: fake.commits)
    finally:
        service_logger.removeHandler(handler)
    assert store.count() == 1 and fake.commits == [{0: 1}]
    process = [s for s in tracing.exporter.get_finished_spans() if s.kind == SpanKind.CONSUMER]
    assert len(process) == 1 and process[0].context.trace_id == producer.trace_id
    assert dict(process[0].attributes)["email.outcome"] == "stored"
    sending = [json.loads(x) for x in stream.getvalue().splitlines() if "has been received" in x]
    assert sending[0]["trace_id"] == trace.format_trace_id(producer.trace_id)
    assert sending[0]["span_id"] == trace.format_span_id(process[0].context.span_id)
