"""orders.placed consumer: renders the order-confirmation mail, stores it in the mailbox, commits the offset.

Rules: consumer group, earliest offset for a new group, offsets committed by hand and
only after the mail is stored (at-least-once; the store is idempotent by event_id), a topic or broker that is not
there yet is waited for, malformed JSON / unsupported schema are skipped with a warning (never the payload in the
log), a transient failure (the database) is retried with backoff and never skipped, SIGTERM (stop_event) finishes the
current message, commits, closes the consumer and returns.

The Kafka client is kafka-python (pure Python: there is no musllinux wheel for confluent-kafka and the image is
python:3.12-alpine). The consumer runs in its own thread; kafka-python's heartbeats run in a background thread of
its own, so a slow message does not cost the group membership.
"""

import json
import logging
import sqlite3
import threading
import time
from dataclasses import dataclass

from jinja2 import TemplateError

from kafka_trace import consumer_span
from mailbox_store import MailboxStore
from rendering import EventError, order_view_from_event, render_html, render_text, subject_for

logger = logging.getLogger("email-service")

TOPIC = "orders.placed"
EVENT_TYPE = "order.placed"
SUPPORTED_SCHEMA_VERSION = 1

# Outcomes of handle_message (also what the tests assert on)
STORED = "stored"
DUPLICATE = "duplicate"
SKIPPED = "skipped"


class RepeatFilter(logging.Filter):
    """Let an identical message through at most once per `window` seconds.

    The Kafka client repeats "Topic x not found in cluster metadata" and "Marking the coordinator dead" several
    times a second for as long as the topic or the broker is missing (normal until the first order is placed)."""

    def __init__(self, window: float = 60.0, clock=time.monotonic):
        super().__init__()
        self.window, self._clock, self._seen = window, clock, {}

    def filter(self, record: logging.LogRecord) -> bool:
        key, now = (record.name, record.getMessage()), self._clock()
        last = self._seen.get(key)
        if last is not None and now - last < self.window:
            return False
        if len(self._seen) > 1000:
            self._seen = {k: t for k, t in self._seen.items() if now - t < self.window}
        self._seen[key] = now
        return True


class TransientError(Exception):
    """Handling failed for a reason that may pass (database busy/unavailable). The message is retried, never skipped."""


@dataclass(frozen=True)
class Position:
    topic: str
    partition: int
    offset: int

    def __str__(self) -> str:
        return f"{self.topic}[{self.partition}]@{self.offset}"


def parse_event(value: bytes | None) -> tuple[dict | None, str]:
    """Return (event, "") for a usable event, else (None, reason). The reason never contains payload data."""
    if not value:
        return None, "empty value"
    try:
        event = json.loads(value.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        return None, "not valid JSON"
    if not isinstance(event, dict):
        return None, "not a JSON object"
    version = event.get("schema_version")
    if isinstance(version, bool) or not isinstance(version, int):
        return None, "missing or invalid schema_version"
    if version != SUPPORTED_SCHEMA_VERSION:
        return None, f"unsupported schema_version {version}"
    if event.get("event_type") != EVENT_TYPE:
        return None, "unsupported event_type"
    for key in ("event_id", "order_id", "email"):
        if not isinstance(event.get(key), str) or not event[key]:
            return None, f"missing {key}"
    return event, ""


def handle_message(store: MailboxStore, pos: Position, value: bytes | None) -> str:
    """Handle one record. Returns STORED, DUPLICATE or SKIPPED; raises TransientError when it must be retried."""
    event, reason = parse_event(value)
    if event is None:
        logger.warning(f"skipping record {pos}: {reason}")
        return SKIPPED
    order_id = event["order_id"]
    try:
        order = order_view_from_event(event)
        html = render_html(order)
        text = render_text(order)
    except (EventError, TemplateError, TypeError, AttributeError) as err:
        # deterministic: the same record would fail the same way, so retrying only blocks the partition
        logger.error(f"skipping record {pos} order={order_id}: cannot render the confirmation ({type(err).__name__})")
        return SKIPPED
    try:
        mail_id = store.add(event["event_id"], order_id, event["email"], subject_for(order_id), html, text)
    except sqlite3.Error as err:
        raise TransientError(f"mailbox: {type(err).__name__}: {err}") from err
    if mail_id is None:
        logger.info(f"duplicate event for order={order_id} at {pos}, mail already stored")
        return DUPLICATE
    # The recipient is deliberately not logged (contract: never log the payload); the order id identifies the mail.
    logger.info(f"A request to send order confirmation email for order={order_id} has been received ({pos}).")
    return STORED


class Backoff:
    """1 s, 2 s, 4 s ... capped at `maximum`; reset() after a success."""

    def __init__(self, initial: float = 1.0, maximum: float = 30.0):
        self.initial, self.maximum, self._next = initial, maximum, initial

    def next(self) -> float:
        delay = self._next
        self._next = min(self._next * 2, self.maximum)
        return delay

    def reset(self) -> None:
        self._next = self.initial


class OrdersPlacedConsumer:
    def __init__(
        self,
        brokers: list[str],
        group_id: str,
        store: MailboxStore,
        stop_event: threading.Event,
        topic: str = TOPIC,
        consumer_factory=None,
        backoff_initial: float = 1.0,
        backoff_max: float = 30.0,
        poll_timeout_ms: int = 500,
    ):
        self.brokers, self.group_id, self.store, self.stop_event, self.topic = (
            brokers,
            group_id,
            store,
            stop_event,
            topic,
        )
        self._factory = consumer_factory or self._kafka_consumer
        self._backoff_initial, self._backoff_max = backoff_initial, backoff_max
        self._poll_timeout_ms = poll_timeout_ms
        self._thread: threading.Thread | None = None
        self.processed = 0
        self.skipped = 0
        self.failed_attempts = 0

    # -- lifecycle ---------------------------------------------------------

    def start(self) -> None:
        self._thread = threading.Thread(target=self.run, name="orders-placed-consumer", daemon=True)
        self._thread.start()

    def join(self, timeout: float | None = None) -> bool:
        """Wait for the thread to finish; True when it is gone."""
        if self._thread is None:
            return True
        self._thread.join(timeout)
        return not self._thread.is_alive()

    # -- internals ---------------------------------------------------------

    def _kafka_consumer(self):
        from kafka import KafkaConsumer  # imported here: the service does not need the library with Kafka off

        return KafkaConsumer(
            bootstrap_servers=self.brokers,
            group_id=self.group_id,
            client_id="email-service",
            enable_auto_commit=False,
            auto_offset_reset="earliest",  # a new group starts at the beginning of the topic
            allow_auto_create_topics=True,
            max_poll_records=1,  # one message at a time: handle, store, commit
            session_timeout_ms=10000,
            heartbeat_interval_ms=3000,
            metadata_max_age_ms=30000,  # notice a topic that appears later within 30 s
            request_timeout_ms=15000,
            bootstrap_timeout_ms=2000,  # the constructor blocks this long when no broker answers; then _connect retries
        )

    def _wait(self, seconds: float) -> bool:
        """Sleep that wakes up on shutdown. True when we should stop."""
        return self.stop_event.wait(seconds)

    def _connect(self):
        backoff = Backoff(self._backoff_initial, self._backoff_max)
        attempt = 0
        while not self.stop_event.is_set():
            attempt += 1
            try:
                consumer = self._factory()
                consumer.subscribe([self.topic])
                logger.info(f"kafka consumer started group={self.group_id} topic={self.topic}")
                return consumer
            except Exception as err:  # broker not there (yet): keep trying, never crash-loop
                if attempt == 1 or attempt % 10 == 0:
                    logger.info(f"kafka not reachable yet ({type(err).__name__}), retrying (attempt {attempt})")
                if self._wait(backoff.next()):
                    break
        return None

    def _commit(self, consumer, pos: Position, record) -> bool:
        """Commit pos+1 after the message was handled. True when committed (or it can only be redelivered)."""
        from kafka.errors import CommitFailedError
        from kafka.structs import OffsetAndMetadata, TopicPartition

        backoff = Backoff(self._backoff_initial, self._backoff_max)
        meta = OffsetAndMetadata(pos.offset + 1, "", getattr(record, "leader_epoch", None) or -1)
        while True:
            try:
                consumer.commit({TopicPartition(pos.topic, pos.partition): meta})
                return True
            except CommitFailedError:
                # The group was rebalanced under us: the partition is re-read from the last commit (idempotent).
                logger.warning(f"commit of {pos} failed after a rebalance, the record will be redelivered")
                return True
            except Exception as err:
                logger.warning(f"commit of {pos} failed ({type(err).__name__}), retrying")
                if self._wait(backoff.next()):
                    return False  # shutting down uncommitted: redelivered at the next start

    def _process(self, consumer, record) -> bool:
        """Handle one record with retry on transient failures, then commit. False when we stopped before the commit.

        The whole thing runs in a CONSUMER span, a child of the producer span when the record carries `traceparent`
        (OBS-61); the log lines written meanwhile carry its trace_id/span_id."""
        with consumer_span(record, self.group_id) as span:
            pos = Position(record.topic, record.partition, record.offset)
            backoff = Backoff(self._backoff_initial, self._backoff_max)
            while True:
                try:
                    outcome = handle_message(self.store, pos, record.value)
                    break
                except TransientError as err:
                    self.failed_attempts += 1
                    delay = backoff.next()
                    span.record_exception(err)
                    logger.error(f"handling {pos} failed, retrying in {delay:.0f}s", extra={"error": str(err)})
                    if self._wait(delay):
                        return False
            span.set_attribute("email.outcome", outcome)
            if outcome == SKIPPED:
                self.skipped += 1
            else:
                self.processed += 1
            return self._commit(consumer, pos, record)

    def run(self) -> None:
        consumer = self._connect()
        if consumer is None:
            return
        topic_warned = False
        try:
            while not self.stop_event.is_set():
                try:
                    batches = consumer.poll(timeout_ms=self._poll_timeout_ms, max_records=1)
                except Exception as err:  # broker went away etc.: the client reconnects on its own
                    logger.warning(f"kafka poll failed ({type(err).__name__}), retrying")
                    if self._wait(1.0):
                        break
                    continue
                if not batches and not topic_warned:
                    try:
                        if consumer.partitions_for_topic(self.topic) is None:
                            logger.info(f"topic {self.topic} does not exist yet, waiting for it")
                            topic_warned = True
                    except Exception as err:  # only a hint for the log; the poll loop must not stop for it
                        logger.debug(f"topic lookup failed ({type(err).__name__})")
                for records in batches.values():
                    for record in records:
                        topic_warned = False
                        if not self._process(consumer, record):
                            return
        finally:
            try:
                consumer.close(autocommit=False)
            except Exception as err:
                logger.warning(f"closing the kafka consumer failed ({type(err).__name__})")
            logger.info("kafka consumer stopped")
