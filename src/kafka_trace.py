"""W3C trace context from Kafka record headers (OBS-61).

The checkout service injects `traceparent` (and `tracestate`) into the headers of every `orders.placed` record.
kafka-python hands them over as `record.headers`, a list of (key, bytes) tuples. consumer_span() extracts them and
starts a CONSUMER span that is a child of the producer span, so one order stays ONE trace from the browser to this
service. Missing, malformed or undecodable headers never break consumption: the span then simply starts a new trace.
Only the OpenTelemetry API is used (no-op spans when telemetry is off).
"""

import contextlib
import logging
from collections.abc import Iterable, Iterator

from opentelemetry import context as otel_context
from opentelemetry import propagate, trace
from opentelemetry.trace import SpanKind

logger = logging.getLogger("email-service")

TRACER_NAME = "email-service.kafka"


def headers_to_carrier(headers: Iterable[tuple[str, bytes | str | None]] | None) -> dict[str, str]:
    """Header list -> dict with lower-cased keys. Non-UTF-8 values are dropped; the last duplicate wins."""
    carrier: dict[str, str] = {}
    for item in headers or ():
        try:
            key, value = item
            if isinstance(value, (bytes, bytearray)):
                value = bytes(value).decode("utf-8")
            if isinstance(key, str) and isinstance(value, str):
                carrier[key.lower()] = value
        except (TypeError, ValueError):  # not a pair, or not UTF-8: ignore this header
            continue
    return carrier


def extract_context(headers) -> otel_context.Context:
    """The context carried by the record headers (an empty context when there is none or it is invalid)."""
    try:
        return propagate.extract(headers_to_carrier(headers))
    except Exception as err:  # never let tracing break consumption
        logger.debug("ignoring unreadable trace headers", extra={"error": type(err).__name__})
        return otel_context.Context()


@contextlib.contextmanager
def consumer_span(record, group: str, tracer: trace.Tracer | None = None) -> Iterator[trace.Span]:
    """Run the body inside a CONSUMER span for `record` (child of the producer span when the headers allow it)."""
    tracer = tracer or trace.get_tracer(TRACER_NAME)
    attributes = {
        "messaging.system": "kafka",
        "messaging.destination.name": record.topic,
        "messaging.operation": "process",
        "messaging.operation.type": "process",
        "messaging.consumer.group.name": group,
        "messaging.kafka.consumer.group": group,
        "messaging.destination.partition.id": str(record.partition),
        "messaging.kafka.message.offset": record.offset,
    }
    with tracer.start_as_current_span(
        f"{record.topic} process",
        context=extract_context(getattr(record, "headers", None)),
        kind=SpanKind.CONSUMER,
        attributes=attributes,
    ) as span:
        yield span
