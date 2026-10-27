# telemetry must be imported first to patch grpc before anything else
import logging
import os
import signal
import sys
import threading
from concurrent import futures

import grpc
from grpc_health.v1 import health_pb2, health_pb2_grpc
from jinja2 import Environment, FileSystemLoader, TemplateError, select_autoescape
from pythonjsonlogger.json import JsonFormatter

import telemetry
from generated import email_pb2, email_pb2_grpc
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

# ---------------------------------------------------------------------------
# Email template
# ---------------------------------------------------------------------------

env = Environment(
    loader=FileSystemLoader(os.path.join(os.path.dirname(__file__), "..", "templates")),
    autoescape=select_autoescape(["html", "xml"]),
)
template = env.get_template("confirmation.html")

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
                template.render(order=order)
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


def install_signal_handlers(stop_event: threading.Event) -> None:
    """SIGTERM/SIGINT set stop_event. Needed because PID 1 in a container ignores signals without a handler."""

    def _handler(signum, _frame):
        logger.info(f"Received {signal.Signals(signum).name}, shutting down")
        stop_event.set()

    signal.signal(signal.SIGTERM, _handler)
    signal.signal(signal.SIGINT, _handler)


def shutdown(server, grace: float = SHUTDOWN_GRACE_SECONDS) -> None:
    """Stop accepting RPCs, let in-flight ones finish for up to `grace` seconds, then flush telemetry."""
    server.stop(grace).wait()
    logger.info("EmailService stopped")
    telemetry.shutdown()


def main() -> int:
    port = os.environ.get("PORT", "8080")
    stop_event = threading.Event()
    install_signal_handlers(stop_event)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    email_pb2_grpc.add_EmailServiceServicer_to_server(EmailServiceServicer(), server)
    health_pb2_grpc.add_HealthServicer_to_server(HealthServicer(), server)

    server.add_insecure_port(f"[::]:{port}")
    server.start()
    logger.info(f"EmailService gRPC server started on port {port} ({telemetry.describe()})")

    stop_event.wait()
    shutdown(server)
    return 0


if __name__ == "__main__":
    sys.exit(main())
