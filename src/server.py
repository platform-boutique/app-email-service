# telemetry must be imported first to patch grpc before anything else
import logging
import os
import signal
import sqlite3
import sys
import threading
from concurrent import futures

import grpc
from grpc_health.v1 import health_pb2, health_pb2_grpc
from jinja2 import TemplateError
from pythonjsonlogger.json import JsonFormatter

import telemetry
from generated import email_pb2, email_pb2_grpc, mailbox_pb2_grpc
from telemetry import start_rpc_metrics  # noqa: F401, E402

# ---------------------------------------------------------------------------
# Logger
# ---------------------------------------------------------------------------

logger = logging.getLogger("email-service")
handler = logging.StreamHandler(sys.stdout)
# One valid JSON object per line (quotes and newlines in messages are escaped); same keys as before.
handler.setFormatter(
    JsonFormatter(
        ["created", "levelname", "name", "message"],
        rename_fields={"created": "timestamp", "levelname": "severity"},
    )
)
logger.addHandler(handler)
logger.setLevel(logging.INFO)
logger.propagate = False

# The Kafka client's own messages (connection problems, rebalances) go through the same JSON handler; it never logs
# record payloads. Without this they would end up on stderr in a different format.
kafka_logger = logging.getLogger("kafka")
kafka_logger.addHandler(handler)
kafka_logger.setLevel(logging.WARNING)
kafka_logger.propagate = False
# Connection churn while the broker is away (one ERROR/WARNING pair per reconnect attempt) is reported by the
# consumer itself as "kafka connection lost" / "restored" at info level.
logging.getLogger("kafka.net").setLevel(logging.CRITICAL)

# Imported after the logger exists: these modules log through it.
from consumer import OrdersPlacedConsumer  # noqa: E402
from mailbox_service import MailboxServicer  # noqa: E402
from mailbox_store import MailboxStore  # noqa: E402
from rendering import render_html  # noqa: E402

# ---------------------------------------------------------------------------
# gRPC service implementation
# ---------------------------------------------------------------------------


class EmailServiceServicer(email_pb2_grpc.EmailServiceServicer):
    """Dummy email service — logs the confirmation instead of sending real email."""

    def SendOrderConfirmation(self, request, context):
        end_metrics = start_rpc_metrics("SendOrderConfirmation")
        try:
            email = request.email
            order = request.order

            # Render the confirmation template
            try:
                render_html(order)
            except TemplateError as err:
                logger.error(f"Template rendering failed: {err}")
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details("An error occurred when preparing the confirmation mail.")
                end_metrics("13")  # INTERNAL
                return email_pb2.Empty()

            logger.info(f"A request to send order confirmation email to {email} has been received.")
            end_metrics("0")  # OK
            return email_pb2.Empty()

        except Exception as err:
            logger.error(f"SendOrderConfirmation failed: {err}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("Failed to send order confirmation.")
            end_metrics("13")  # INTERNAL
            return email_pb2.Empty()


class HealthServicer(health_pb2_grpc.HealthServicer):
    def Check(self, request, context):
        return health_pb2.HealthCheckResponse(
            status=health_pb2.HealthCheckResponse.SERVING,
        )


# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------


# Seconds in-flight RPCs get to finish after SIGTERM before they are cancelled. Drain (2 s) plus the telemetry
# flush (at most 2 s) stays inside the charts' terminationGracePeriodSeconds: 5.
SHUTDOWN_GRACE_SECONDS = 2

DEFAULT_MAILBOX_DB = "/data/mailbox.db"


def install_signal_handlers(stop_event: threading.Event) -> None:
    """SIGTERM/SIGINT set stop_event. Needed because PID 1 in a container ignores signals without a handler."""

    def _handler(signum, _frame):
        logger.info(f"Received {signal.Signals(signum).name}, shutting down")
        stop_event.set()

    signal.signal(signal.SIGTERM, _handler)
    signal.signal(signal.SIGINT, _handler)


def shutdown(server, consumer=None, grace: float = SHUTDOWN_GRACE_SECONDS) -> None:
    """Stop accepting RPCs, let in-flight ones finish for up to `grace` seconds, then flush telemetry.

    The consumer (already told to stop through stop_event) finishes its current message, commits and closes in
    parallel; it gets the same `grace`, after that the daemon thread is abandoned (its message is redelivered).
    """
    stopped = server.stop(grace)
    if consumer is not None and not consumer.join(grace):
        logger.warning("the kafka consumer did not stop in time, leaving it (its message will be redelivered)")
    stopped.wait()
    logger.info("EmailService stopped")
    telemetry.shutdown()


def create_server(store: MailboxStore) -> grpc.Server:
    """The gRPC server with every service on one port: EmailService, MailboxService and Health (not bound yet).

    Health is static SERVING and does not look at Kafka: a broker outage must not take the pod out of rotation.
    """
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    email_pb2_grpc.add_EmailServiceServicer_to_server(EmailServiceServicer(), server)
    mailbox_pb2_grpc.add_MailboxServiceServicer_to_server(MailboxServicer(store), server)
    health_pb2_grpc.add_HealthServicer_to_server(HealthServicer(), server)
    return server


def kafka_brokers(environ=os.environ) -> list[str]:
    """KAFKA_BROKERS is a comma separated list; empty or unset means Kafka is off."""
    return [b.strip() for b in environ.get("KAFKA_BROKERS", "").split(",") if b.strip()]


def open_store(path: str, kafka_on: bool) -> MailboxStore:
    """Open the mailbox. With Kafka on an unusable MAILBOX_DB is fatal (the consumer could not store anything).
    With Kafka off the service must behave as before, so it falls back to an empty in-memory mailbox."""
    try:
        return MailboxStore(path)
    except (OSError, sqlite3.Error) as err:
        if kafka_on:
            logger.error(f"cannot open the mailbox database {path}: {err}")
            raise
        logger.warning(
            f"cannot open the mailbox database {path} ({err}); Kafka is off, using an empty in-memory mailbox"
        )
        return MailboxStore(":memory:")


def main() -> int:
    port = os.environ.get("PORT", "8080")
    brokers = kafka_brokers()
    stop_event = threading.Event()
    install_signal_handlers(stop_event)

    store = open_store(os.environ.get("MAILBOX_DB", DEFAULT_MAILBOX_DB), kafka_on=bool(brokers))
    server = create_server(store)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    logger.info(f"EmailService gRPC server started on port {port} ({telemetry.describe()})")

    consumer = None
    if brokers:
        group = os.environ.get("KAFKA_CONSUMER_GROUP") or "email-service"
        consumer = OrdersPlacedConsumer(brokers, group, store, stop_event)
        consumer.start()
        logger.info(f"consuming orders.placed from {','.join(brokers)} as group {group}")
    else:
        logger.info("KAFKA_BROKERS is empty: the orders.placed consumer is off")

    stop_event.wait()
    shutdown(server, consumer)
    store.close()
    return 0


if __name__ == "__main__":
    code = main()
    # Everything is flushed and stopped by now. Leave without interpreter finalization: with the OTLP exporter and
    # the Pyroscope profiler both pointed at a collector that does not answer, finalization blocks until SIGKILL.
    logging.shutdown()
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(code)
