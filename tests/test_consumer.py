"""The orders.placed consumer: parse/skip rules, idempotent store, retry, shutdown. No broker needed (fake client)."""

import json
import logging
import signal
import sqlite3
import subprocess
import sys
import threading
import time
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
        assert "<!DOCTYPE html>" in full.html_body and "PROD-001" in full.html_body and "Linen shirt" in full.text_body
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


class FakeKafka:
    """Delivers the queued records one per poll, records commits and close()."""

    def __init__(self, values, stop_after=None, stop_event=None):
        self.queue = [
            SimpleNamespace(topic="orders.placed", partition=0, offset=i, value=v, leader_epoch=None)
            for i, v in enumerate(values)
        ]
        self.commits, self.closed, self.subscribed = [], False, None
        self._stop_after, self._stop_event = stop_after, stop_event

    def subscribe(self, topics):
        self.subscribed = topics

    def poll(self, timeout_ms=0, max_records=None):
        if self.queue:
            rec = self.queue.pop(0)
            if self._stop_after is not None and rec.offset == self._stop_after:
                self._stop_event.set()  # SIGTERM arrives while this record is being handled
            return {("orders.placed", 0): [rec]}
        time.sleep(0.01)
        return {}

    def partitions_for_topic(self, topic):
        return {0}

    def commit(self, offsets):
        self.commits.append({tp.partition: m.offset for tp, m in offsets.items()})

    def close(self, autocommit=True, timeout_ms=None):
        self.closed = True


def make_consumer(store, fake, stop_event, **kw):
    return OrdersPlacedConsumer(
        ["x:1"], "g", store, stop_event, consumer_factory=lambda: fake, backoff_initial=0.01, backoff_max=0.05, **kw
    )


def run_until(consumer, condition, timeout=30):
    consumer.start()
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline and not condition():
        time.sleep(0.02)
    consumer.stop_event.set()
    assert consumer.join(10), "consumer thread did not stop"


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

    def down(*args):
        raise sqlite3.OperationalError("disk I/O error")

    store.add = down
    c = OrdersPlacedConsumer(
        ["x:1"], "g", store, stop, consumer_factory=lambda: fake, backoff_initial=30, backoff_max=30
    )
    c.start()
    time.sleep(0.3)
    started = time.monotonic()
    stop.set()
    assert c.join(5)
    assert time.monotonic() - started < 2  # the 30 s backoff does not delay the shutdown
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

    def factory():
        raise OSError("no brokers available")

    c = OrdersPlacedConsumer(["x:1"], "g", store, stop, consumer_factory=factory, backoff_initial=30, backoff_max=30)
    c.start()
    time.sleep(0.2)
    stop.set()
    assert c.join(3)


def test_missing_topic_is_logged_at_info_once(store, caplog):
    stop = threading.Event()
    fake = FakeKafka([])
    fake.partitions_for_topic = lambda topic: None
    c = make_consumer(store, fake, stop)
    with caplog.at_level(logging.INFO, logger="email-service"):
        c.start()
        time.sleep(0.3)
        stop.set()
        assert c.join(5)
    msgs = [r.getMessage() for r in caplog.records if "does not exist yet" in r.getMessage()]
    assert len(msgs) == 1


# ---------------------------------------------------------------------------
# real process: Kafka configured but unreachable
# ---------------------------------------------------------------------------


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
    try:
        with grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
            grpc.channel_ready_future(channel).result(timeout=30)
            time.sleep(3)  # the consumer is in its connect/backoff loop by now
            health = health_pb2_grpc.HealthStub(channel).Check(health_pb2.HealthCheckRequest(), timeout=5)
            assert health.status == health_pb2.HealthCheckResponse.SERVING
        proc.send_signal(signal.SIGTERM)
        started = time.monotonic()
        stdout, stderr = proc.communicate(timeout=15)
        elapsed = time.monotonic() - started
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.communicate()
    assert proc.returncode == 0, stderr
    assert elapsed < 5
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
