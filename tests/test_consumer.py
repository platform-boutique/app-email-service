"""The orders.placed consumer: parse/skip rules, idempotent store, retry, shutdown. No broker needed (fake client)."""

import json
import logging
import signal
import sqlite3
import subprocess
import sys
import threading
import time
from collections import namedtuple
from types import SimpleNamespace

import pytest

from consumer import (
    DUPLICATE,
    SKIPPED,
    STORED,
    OrdersPlacedConsumer,
    Position,
    TransientError,
    handle_message,
    parse_event,
    topic_name,
)
from tests.conftest import event_bytes, make_event
from tests.test_lifecycle import _clean_env, _free_port, app_dir  # noqa: F401  (app_dir is a fixture)

POS = Position("orders.placed", 1, 42)


# ---------------------------------------------------------------------------
# parsing / skip rules
# ---------------------------------------------------------------------------


class TestParse:
    def test_valid_event(self):
        event, reason = parse_event(event_bytes())
        assert reason == "" and event["order_id"] == "ORD-123"

    def test_unknown_fields_are_ignored(self):
        event, _ = parse_event(event_bytes(brand_new={"x": 1}))
        assert event is not None

    @pytest.mark.parametrize(
        "value",
        [None, b"", b"not json", b"\xff\xfe", b"[1,2]", b'"str"', b"null"],
    )
    def test_malformed_is_skipped(self, value):
        event, reason = parse_event(value)
        assert event is None and reason

    @pytest.mark.parametrize("version", [2, 99, 0, -1, "1", None, True, 1.5])
    def test_unsupported_or_invalid_schema_version_is_skipped(self, version):
        event, reason = parse_event(event_bytes(schema_version=version))
        assert event is None and "schema_version" in reason

    def test_missing_schema_version_is_skipped(self):
        raw = make_event()
        del raw["schema_version"]
        assert parse_event(json.dumps(raw).encode())[0] is None

    def test_other_event_type_is_skipped(self):
        assert parse_event(event_bytes(event_type="order.cancelled"))[0] is None

    @pytest.mark.parametrize("key", ["event_id", "order_id", "email"])
    def test_missing_identity_is_skipped(self, key):
        assert parse_event(event_bytes(**{key: ""}))[0] is None
        raw = make_event()
        del raw[key]
        assert parse_event(json.dumps(raw).encode())[0] is None


class TestHandle:
    def test_stores_one_mail_and_logs_the_sending_line_without_the_recipient(self, store, caplog):
        with caplog.at_level(logging.INFO, logger="email-service"):
            assert handle_message(store, POS, event_bytes()) == STORED
        assert store.count() == 1
        mail = store.list_for_orders(["ORD-123"])[0]
        assert mail.to_addr == "customer@example.com" and "ORD-123" in mail.subject
        full = store.get(mail.id, "ORD-123")
        assert (
            "<!DOCTYPE html>" in full.html_body and "Linen shirt" in full.html_body and "Linen shirt" in full.text_body
        )
        assert "PROD-001" not in full.html_body and "PROD-001" not in full.text_body  # ids are not customer text
        log = "\n".join(r.getMessage() for r in caplog.records)
        assert "order confirmation email" in log and "order=ORD-123" in log and "orders.placed[1]@42" in log
        assert "customer@example.com" not in log and "123 Main St" not in log  # never the payload

    def test_replay_stores_nothing_new(self, store):
        assert handle_message(store, POS, event_bytes()) == STORED
        assert handle_message(store, Position("orders.placed", 2, 7), event_bytes()) == DUPLICATE
        assert store.count() == 1

    def test_republish_of_the_same_order_with_the_same_event_id_is_one_mail(self, store):
        handle_message(store, POS, event_bytes(occurred_at="2026-10-31T18:00:01Z"))
        handle_message(store, POS, event_bytes(occurred_at="2026-10-31T18:00:09Z"))
        assert store.count() == 1

    def test_skips_log_warn_with_position_but_no_payload(self, store, caplog):
        with caplog.at_level(logging.WARNING, logger="email-service"):
            assert handle_message(store, POS, b'{"email": "secret@example.com", "schema_version": 2}') == SKIPPED
        assert store.count() == 0
        warn = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warn) == 1 and "orders.placed[1]@42" in warn[0].getMessage()
        assert "secret@example.com" not in warn[0].getMessage()

    def test_undigestible_event_is_skipped_not_retried(self, store):
        assert handle_message(store, POS, event_bytes(items="not a list")) == SKIPPED
        assert store.count() == 0

    def test_database_error_is_transient(self):
        class Broken:
            def add(self, *a):
                raise sqlite3.OperationalError("database is locked")

        with pytest.raises(TransientError):
            handle_message(Broken(), POS, event_bytes())


# ---------------------------------------------------------------------------
# consumer loop with a fake Kafka client
# ---------------------------------------------------------------------------


# Tests wait for what the consumer did by waiting on this condition (notified on every commit, close and idle poll),
# never by sleeping for a guessed time. The timeout of wait_for is only the failsafe against a hung test.
PROGRESS = threading.Condition()


class FakeKafka:
    """Delivers the queued records one per poll, records commits and close()."""

    def __init__(self, values, stop_after=None, stop_event=None):
        self.queue = [
            SimpleNamespace(topic="orders.placed", partition=0, offset=i, value=v, leader_epoch=None)
            for i, v in enumerate(values)
        ]
        self.commits, self.closed, self.subscribed = [], False, None
        self.idle_polls = 0
        self._stop_after, self._stop_event = stop_after, stop_event
        self._yield = threading.Event()  # never set: wait(0.01) only keeps an idle poll from spinning

    def subscribe(self, topics):
        self.subscribed = topics

    def poll(self, timeout_ms=0, max_records=None):
        if self.queue:
            rec = self.queue.pop(0)
            if self._stop_after is not None and rec.offset == self._stop_after:
                self._stop_event.set()  # SIGTERM arrives while this record is being handled
            return {("orders.placed", 0): [rec]}
        self._yield.wait(0.01)
        with PROGRESS:
            self.idle_polls += 1
            PROGRESS.notify_all()
        return {}

    def partitions_for_topic(self, topic):
        return {0}

    def commit(self, offsets):
        with PROGRESS:
            self.commits.append({tp.partition: m.offset for tp, m in offsets.items()})
            PROGRESS.notify_all()

    def close(self, autocommit=True, timeout_ms=None):
        with PROGRESS:
            self.closed = True
            PROGRESS.notify_all()


def make_consumer(store, fake, stop_event, **kw):
    return OrdersPlacedConsumer(
        ["x:1"], "g", store, stop_event, consumer_factory=lambda: fake, backoff_initial=0.01, backoff_max=0.05, **kw
    )


def run_until(consumer, condition, timeout=60):
    """Start the consumer, wait until `condition()` holds (re-checked on every PROGRESS notification), stop it."""
    consumer.start()
    with PROGRESS:
        PROGRESS.wait_for(condition, timeout)
    consumer.stop_event.set()
    assert consumer.join(30), "consumer thread did not stop"


def test_commits_the_next_offset_after_each_stored_message(store):
    stop = threading.Event()
    fake = FakeKafka([event_bytes("A"), b"garbage", event_bytes("B")])
    c = make_consumer(store, fake, stop)
    run_until(c, lambda: len(fake.commits) == 3)
    assert fake.commits == [{0: 1}, {0: 2}, {0: 3}]  # the skipped record is committed too
    assert fake.subscribed == ["orders.placed"] and fake.closed
    assert store.count() == 2 and c.processed == 2 and c.skipped == 1


def test_transient_store_failure_is_retried_never_skipped_and_commits_only_after_success(store):
    stop = threading.Event()
    fake = FakeKafka([event_bytes("A")])
    real_add, calls = store.add, []

    def flaky(*args):
        calls.append(1)
        if len(calls) < 4:
            raise sqlite3.OperationalError("database is locked")
        return real_add(*args)

    store.add = flaky
    c = make_consumer(store, fake, stop)
    run_until(c, lambda: fake.commits)
    assert len(calls) == 4 and c.failed_attempts == 3
    assert store.count() == 1 and fake.commits == [{0: 1}]


def test_a_failing_commit_is_retried(store):
    stop = threading.Event()
    fake = FakeKafka([event_bytes("A")])
    real_commit, attempts = fake.commit, []

    def flaky_commit(offsets):
        attempts.append(1)
        if len(attempts) < 3:
            raise RuntimeError("broker gone")
        real_commit(offsets)

    fake.commit = flaky_commit
    run_until(make_consumer(store, fake, stop), lambda: fake.commits)
    assert fake.commits == [{0: 1}] and len(attempts) == 3


def test_stop_while_the_database_is_down_returns_quickly_without_commit_or_skip(store):
    stop = threading.Event()
    fake = FakeKafka([event_bytes("A")])

    failed = threading.Event()

    def down(*args):
        failed.set()
        raise sqlite3.OperationalError("disk I/O error")

    store.add = down
    c = OrdersPlacedConsumer(
        ["x:1"], "g", store, stop, consumer_factory=lambda: fake, backoff_initial=3600, backoff_max=3600
    )
    c.start()
    assert failed.wait(60)  # the consumer is at (or about to enter) its retry wait
    stop.set()
    assert c.join(30)  # a one hour backoff does not delay the shutdown: it wakes on the stop event
    assert fake.commits == [] and fake.closed  # never committed: redelivered at the next start


def test_stop_finishes_the_current_message_commits_and_closes(store):
    stop = threading.Event()
    fake = FakeKafka([event_bytes("A"), event_bytes("B")], stop_after=0, stop_event=stop)
    c = make_consumer(store, fake, stop)
    c.start()
    assert c.join(10)
    assert fake.commits == [{0: 1}]  # message A (in flight when the stop came) is stored and committed
    assert store.count() == 1 and fake.closed  # B is not started


def test_broker_not_there_yet_is_waited_for(store, caplog):
    stop = threading.Event()
    fake = FakeKafka([event_bytes("A")])
    attempts = []

    def factory():
        attempts.append(1)
        if len(attempts) < 4:
            raise OSError("no brokers available")
        return fake

    c = OrdersPlacedConsumer(
        ["x:1"], "g", store, stop, consumer_factory=factory, backoff_initial=0.01, backoff_max=0.02
    )
    with caplog.at_level(logging.INFO, logger="email-service"):
        run_until(c, lambda: fake.commits)
    assert len(attempts) == 4 and store.count() == 1
    assert any("not reachable yet" in r.getMessage() and r.levelno == logging.INFO for r in caplog.records)


def test_stop_while_the_broker_is_missing_returns_quickly(store):
    stop = threading.Event()

    tried = threading.Event()

    def factory():
        tried.set()
        raise OSError("no brokers available")

    c = OrdersPlacedConsumer(
        ["x:1"], "g", store, stop, consumer_factory=factory, backoff_initial=3600, backoff_max=3600
    )
    c.start()
    assert tried.wait(60)
    stop.set()
    assert c.join(30)  # the one hour backoff wakes on the stop event


def test_missing_topic_is_logged_at_info_once(store, caplog):
    stop = threading.Event()
    fake = FakeKafka([])
    fake.partitions_for_topic = lambda topic: None
    c = make_consumer(store, fake, stop)
    with caplog.at_level(logging.INFO, logger="email-service"):
        c.start()
        with PROGRESS:
            PROGRESS.wait_for(lambda: fake.idle_polls >= 5, 60)  # several empty polls after the first warning
        stop.set()
        assert c.join(30)
    msgs = [r.getMessage() for r in caplog.records if "does not exist yet" in r.getMessage()]
    assert len(msgs) == 1


# ---------------------------------------------------------------------------
# real process: Kafka configured but unreachable
# ---------------------------------------------------------------------------


class LogTail:
    """Reads a process's stdout on a thread, keeps the lines, and lets a test wait for a message by event."""

    def __init__(self, stream):
        self.lines = []
        self._waiters = []  # (text, Event)
        self._lock = threading.Lock()
        self._thread = threading.Thread(target=self._read, args=(stream,), daemon=True)
        self._thread.start()

    def _read(self, stream):
        for line in stream:
            with self._lock:
                self.lines.append(line)
                for text, event in self._waiters:
                    if text in line:
                        event.set()

    def wait_for(self, text, timeout=120):
        event = threading.Event()
        with self._lock:
            if any(text in line for line in self.lines):
                return True
            self._waiters.append((text, event))
        return event.wait(timeout)

    def finish(self, timeout=60):
        self._thread.join(timeout)
        return list(self.lines)


def test_kafka_down_keeps_health_serving_and_sigterm_exits_0_quickly(app_dir, tmp_path):  # noqa: F811
    import grpc
    from grpc_health.v1 import health_pb2, health_pb2_grpc

    port = _free_port()
    proc = subprocess.Popen(
        [sys.executable, "src/server.py"],
        cwd=app_dir,
        env=_clean_env(
            PORT=str(port),
            PYTHONUNBUFFERED="1",
            KAFKA_BROKERS=f"127.0.0.1:{_free_port()}",  # nothing listens there
            MAILBOX_DB=str(tmp_path / "mailbox.db"),
        ),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    tail = LogTail(proc.stdout)
    try:
        with grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
            grpc.channel_ready_future(channel).result(timeout=120)
            # the consumer is in its connect/backoff loop once it says so (no guessing with a sleep)
            assert tail.wait_for("not reachable yet"), "the consumer never reported the missing broker"
            health = health_pb2_grpc.HealthStub(channel).Check(health_pb2.HealthCheckRequest(), timeout=30)
            assert health.status == health_pb2.HealthCheckResponse.SERVING
        proc.send_signal(signal.SIGTERM)
        started = time.monotonic()
        proc.wait(timeout=30)
        elapsed = time.monotonic() - started
        stdout = "".join(tail.finish())
        stderr = proc.stderr.read()
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()
    assert proc.returncode == 0, stderr
    assert elapsed < 10  # CODE-04: exit within 10 s of SIGTERM even though the broker is gone
    records = [json.loads(line) for line in stdout.splitlines()]  # all JSON, kafka client noise included
    messages = [r["message"] for r in records]
    assert any("not reachable yet" in m for m in messages)
    assert messages[-1] == "EmailService stopped"


def test_repeat_filter_lets_an_identical_message_through_once_per_window():
    from consumer import RepeatFilter

    now = [100.0]
    flt = RepeatFilter(window=60, clock=lambda: now[0])

    def rec(msg, name="kafka.cluster"):
        return logging.LogRecord(name, logging.ERROR, "f.py", 1, msg, None, None)

    assert flt.filter(rec("Topic x not found")) is True
    assert flt.filter(rec("Topic x not found")) is False  # repeated within the window
    assert flt.filter(rec("Topic y not found")) is True  # another message
    now[0] += 61
    assert flt.filter(rec("Topic x not found")) is True  # window over


# ---------------------------------------------------------------------------
# metrics: consumer lag and events by outcome (OBS-20)
# ---------------------------------------------------------------------------


TopicPartition = namedtuple("TopicPartition", "topic partition")


class LagKafka(FakeKafka):
    """A FakeKafka that also answers assignment/highwater/position like the real client (one partition)."""

    def __init__(self, values, end_offset):
        super().__init__(values)
        self.end_offset = end_offset

    def assignment(self):
        return {TopicPartition("orders.placed", 0)}

    def highwater(self, tp):
        return self.end_offset

    def position(self, tp):
        return self.commits[-1][0] if self.commits else 0  # the committed offset is the next one to read


def _meter():
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import InMemoryMetricReader

    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    return provider, reader, provider.get_meter("test")


def _points(reader, name):
    out = []
    data = reader.get_metrics_data()  # None while nothing has been recorded
    for resource_metrics in data.resource_metrics if data else []:
        for scope in resource_metrics.scope_metrics:
            for metric in scope.metrics:
                if metric.name == name:
                    out += [(dict(p.attributes), p.value) for p in metric.data.data_points]
    return out


def test_lag_is_highwater_minus_position_and_goes_away_with_the_consumer(store):
    stop = threading.Event()
    fake = LagKafka([], end_offset=7)  # 7 records on the broker, none read yet
    c = make_consumer(store, fake, stop)
    provider, reader, meter = _meter()
    c.register_metrics(meter)
    c.start()
    with PROGRESS:
        PROGRESS.wait_for(lambda: fake.idle_polls >= 2, 60)  # the lag was refreshed after a poll
    assert c.lag_snapshot() == {("orders.placed", 0): 7}
    assert _points(reader, "kafka_consumer_lag") == [({"group": "g", "topic": "orders.placed", "partition": "0"}, 7)]
    stop.set()
    assert c.join(30)
    assert c.lag_snapshot() == {} and _points(reader, "kafka_consumer_lag") == []  # no stale series after stop
    provider.shutdown()


def test_lag_shrinks_as_records_are_committed(store):
    stop = threading.Event()
    fake = LagKafka([event_bytes("A"), event_bytes("B")], end_offset=2)
    c = make_consumer(store, fake, stop)
    c.start()
    with PROGRESS:
        PROGRESS.wait_for(lambda: len(fake.commits) == 2 and fake.idle_polls >= 2, 60)
    snapshot = c.lag_snapshot()
    stop.set()
    assert c.join(30)
    assert snapshot == {("orders.placed", 0): 0}


def test_lag_of_a_client_that_cannot_tell_is_left_out_and_does_not_stop_the_loop(store):
    stop = threading.Event()
    fake = FakeKafka([event_bytes("A")])  # no assignment(): AttributeError inside the refresh
    c = make_consumer(store, fake, stop)
    run_until(c, lambda: fake.commits)
    assert c.lag_snapshot() == {} and store.count() == 1


def test_events_are_counted_by_outcome(store):
    stop = threading.Event()
    fake = FakeKafka([event_bytes("A"), event_bytes("A"), b"garbage"])  # stored, duplicate, skipped
    c = make_consumer(store, fake, stop)
    provider, reader, meter = _meter()
    c.register_metrics(meter)
    run_until(c, lambda: len(fake.commits) == 3)
    counted = {attrs["outcome"]: value for attrs, value in _points(reader, "email_events_processed_total")}
    assert counted == {"stored": 1, "duplicate": 1, "skipped": 1}
    provider.shutdown()


def test_outcome_series_start_at_zero(store):
    c = make_consumer(store, FakeKafka([]), threading.Event())
    provider, reader, meter = _meter()
    c.register_metrics(meter)
    counted = {attrs["outcome"]: value for attrs, value in _points(reader, "email_events_processed_total")}
    assert counted == {"stored": 0, "duplicate": 0, "skipped": 0}
    provider.shutdown()


# ---------------------------------------------------------------------------
# KAFKA_TOPIC_PREFIX (FUT-03): the same name checkout-service publishes to and order-service consumes
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "prefix, want",
    [
        (None, "orders.placed"),
        ("", "orders.placed"),
        ("   ", "orders.placed"),
        ("dev.", "dev.orders.placed"),
        (" dev. ", "dev.orders.placed"),
        ("stage_1-.", "stage_1-.orders.placed"),
    ],
)
def test_topic_name(prefix, want):
    assert topic_name(prefix) == want


@pytest.mark.parametrize("prefix", ["dev/x", "de v", "dev:", "\u00fc", "a" * 240])
def test_topic_name_rejects_a_prefix_kafka_cannot_have(prefix):
    with pytest.raises(ValueError, match="KAFKA_TOPIC_PREFIX"):
        topic_name(prefix)


def test_topic_name_longest_allowed_prefix():
    assert len(topic_name("a" * (249 - len("orders.placed")))) == 249


def test_consumer_subscribes_to_the_given_topic(store):
    stop, fake = threading.Event(), FakeKafka([])
    c = make_consumer(store, fake, stop, topic=topic_name("dev."))
    run_until(c, lambda: fake.subscribed is not None)
    assert fake.subscribed == ["dev.orders.placed"]
    assert make_consumer(store, FakeKafka([]), threading.Event()).topic == "orders.placed"  # default unchanged


def _start_server(app_dir, tmp_path, **env):  # noqa: F811
    return subprocess.Popen(
        [sys.executable, "src/server.py"],
        cwd=app_dir,
        env=_clean_env(
            PORT=str(_free_port()),
            PYTHONUNBUFFERED="1",
            KAFKA_BROKERS=f"127.0.0.1:{_free_port()}",  # nothing listens there
            MAILBOX_DB=str(tmp_path / "mailbox.db"),
            **env,
        ),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def test_invalid_topic_prefix_stops_the_start_with_a_json_error(app_dir, tmp_path):  # noqa: F811
    proc = _start_server(app_dir, tmp_path, KAFKA_TOPIC_PREFIX="dev/x")
    try:
        stdout, stderr = proc.communicate(timeout=120)
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()
    assert proc.returncode == 1, stderr
    records = [json.loads(line) for line in stdout.splitlines()]
    assert records and records[-1]["severity"] == "error" and "KAFKA_TOPIC_PREFIX" in records[-1]["error"]


def test_topic_prefix_is_used_by_the_started_consumer(app_dir, tmp_path):  # noqa: F811
    proc = _start_server(app_dir, tmp_path, KAFKA_TOPIC_PREFIX="dev.")
    tail = LogTail(proc.stdout)
    try:
        assert tail.wait_for("consuming dev.orders.placed from"), "".join(tail.lines)
    finally:
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=30)
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait()
