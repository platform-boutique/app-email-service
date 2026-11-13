"""Structured logs (OBS-30): one JSON object per line on stdout, the same field set as the Go services.

Fields: timestamp (RFC3339 UTC, fractional seconds), severity (debug, info, warning, error), message, service,
version, trace_id and span_id (hex, only while a valid span is active), error (string, on errors when known),
plus the keys of an optional extra={"fields": {...}}.
The ids are read from the current OpenTelemetry span when the line is formatted, so a line logged while a request
(gRPC server span) or a Kafka message (consumer span) is handled carries that trace. Only the OpenTelemetry API is
used here: with telemetry off the current span is never valid and the ids are simply absent.

Uncaught exceptions (also at import time or in a thread) are logged as JSON too, see install_excepthooks().
"""

import json
import logging
import sys
import threading
from collections.abc import Mapping
from datetime import UTC, datetime

from opentelemetry import trace

DEFAULT_LEVEL = "info"
_LEVELS = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warn": logging.WARNING,
    "warning": logging.WARNING,
    "error": logging.ERROR,
}
_SEVERITY = {
    logging.DEBUG: "debug",
    logging.INFO: "info",
    logging.WARNING: "warning",
    logging.ERROR: "error",
    logging.CRITICAL: "error",
}


def log_level(env: Mapping[str, str]) -> int:
    """LOG_LEVEL (debug, info, warning, error; case-insensitive), default info. An unknown value means info."""
    return _LEVELS.get(env.get("LOG_LEVEL", "").strip().lower(), logging.INFO)


def rfc3339(epoch_seconds: float) -> str:
    return datetime.fromtimestamp(epoch_seconds, UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _severity(levelno: int) -> str:
    return _SEVERITY.get(levelno) or ("error" if levelno > logging.ERROR else "debug")


class JsonFormatter(logging.Formatter):
    def __init__(self, service: str, version: str):
        super().__init__()
        self.service, self.version = service, version

    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "timestamp": rfc3339(record.created),
            "severity": _severity(record.levelno),
            "message": record.getMessage(),
            "service": self.service,
            "version": self.version,
        }
        ctx = trace.get_current_span().get_span_context()
        if ctx.is_valid:  # all-zero ids (no span) are never written
            entry["trace_id"] = trace.format_trace_id(ctx.trace_id)
            entry["span_id"] = trace.format_span_id(ctx.span_id)
        entry.update(getattr(record, "fields", None) or {})  # logger.info("msg", extra={"fields": {...}})
        error = getattr(record, "error", None)
        if error is None and record.exc_info and record.exc_info[1] is not None:
            error = f"{type(record.exc_info[1]).__name__}: {record.exc_info[1]}"
        if error:
            entry["error"] = str(error)
        return json.dumps(entry, ensure_ascii=False, separators=(",", ":"))


def make_handler(service: str, version: str, stream=None) -> logging.Handler:
    handler = logging.StreamHandler(stream or sys.stdout)
    handler.setFormatter(JsonFormatter(service, version))
    return handler


def log_startup_failure(logger: logging.Logger, exc: BaseException, what: str = "startup failed") -> None:
    logger.error(what, extra={"error": f"{type(exc).__name__}: {exc}"})


def install_excepthooks(logger: logging.Logger) -> None:
    """Uncaught exceptions in the main thread (also while importing) and in other threads become one JSON line.

    No traceback on stdout (normal-path rule); the exit code stays 1 for the main thread."""

    def _main(exc_type, exc, _tb):
        if issubclass(exc_type, KeyboardInterrupt):
            return
        log_startup_failure(logger, exc, "uncaught exception")
        sys.stdout.flush()

    def _thread(args):
        log_startup_failure(logger, args.exc_value, f"uncaught exception in thread {args.thread.name}")

    sys.excepthook = _main
    threading.excepthook = _thread
