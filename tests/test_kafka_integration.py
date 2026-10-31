"""Consumer against a real Kafka broker (a throwaway apache/kafka container). NOT part of the normal run.

    KAFKA_INTEGRATION=1 pytest tests/test_kafka_integration.py

The test starts and removes its own container (`d3-kafka-<pid>`, 768 MB, port 127.0.0.1:19093) with docker, so it needs
a docker CLI and the image `apache/kafka:4.3.2`. The host clock of the dev machine jumps: every wait has a generous
30 s deadline and nothing relies on tight sleeps.
"""

import json
import os
import subprocess
import threading
import time
import uuid

import grpc
import pytest

from consumer import OrdersPlacedConsumer
from tests.conftest import make_event

pytestmark = pytest.mark.skipif(
    not os.environ.get("KAFKA_INTEGRATION"), reason="set KAFKA_INTEGRATION=1 (needs docker)"
)

PORT = 19093
BROKERS = [f"127.0.0.1:{PORT}"]
IMAGE = "apache/kafka:4.3.2"
DEADLINE = 90  # seconds


class Broker:
    def __init__(self):
        self.name = f"d3-kafka-{os.getpid()}"

    def start(self):
        subprocess.run(
            [
                "docker", "run", "-d", "--rm", "--name", self.name, "--hostname", self.name, "--memory", "768m",
                "-e", "KAFKA_NODE_ID=1",
                "-e", "KAFKA_PROCESS_ROLES=broker,controller",
                "-e", "KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093",
                "-e", f"KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:{PORT}",
                "-e", "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
                "-e", "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
                "-e", "KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093",
                "-e", "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
                "-e", "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
                "-e", "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1",
                "-e", "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0",
                "-e", "KAFKA_NUM_PARTITIONS=3",
                "-e", "KAFKA_HEAP_OPTS=-Xms256m -Xmx384m",
                "-p", f"127.0.0.1:{PORT}:9092",
                IMAGE,
            ],
            check=True, capture_output=True,
        )  # fmt: skip

    def wait_ready(self):
        from kafka import KafkaProducer

        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            try:
                KafkaProducer(bootstrap_servers=BROKERS, bootstrap_timeout_ms=3000).close()
                return
            except Exception:
                time.sleep(2)
        raise RuntimeError("broker did not come up")

    def stop(self):
        subprocess.run(["docker", "rm", "-f", self.name], capture_output=True)


@pytest.fixture(scope="module")
def broker():
    b = Broker()
    yield b
    b.stop()


def produce(topic, event_dicts):
    from kafka import KafkaProducer

    p = KafkaProducer(bootstrap_servers=BROKERS, acks="all", bootstrap_timeout_ms=30000)
    for event in event_dicts:
        p.send(
            topic,
            key=event["order_id"].encode(),
            value=json.dumps(event).encode(),
            headers=[("event-type", b"order.placed"), ("content-type", b"application/json"), ("schema-version", b"1")],
        ).get(timeout=30)
    p.flush()
    p.close()


def wait_for(condition, what):
    deadline = time.monotonic() + DEADLINE
    while time.monotonic() < deadline:
        if condition():
            return
        time.sleep(0.5)
    raise AssertionError(f"timed out waiting for {what}")


def new_consumer(store, topic, group, stop=None):
    stop = stop or threading.Event()
    return OrdersPlacedConsumer(BROKERS, group, store, stop, topic=topic, backoff_initial=0.5, backoff_max=2)


def stop_consumer(c):
    c.stop_event.set()
    assert c.join(15), "consumer did not stop"


@pytest.fixture()
def uniq():
    return uuid.uuid4().hex[:8]


def test_late_broker_the_consumer_waits_and_then_catches_up(broker, store, uniq):
    """The service starts first, the broker (and the topic) do not exist yet."""
    topic, group = f"orders.placed.late{uniq}", f"g-late{uniq}"
    c = new_consumer(store, topic, group)
    c.start()
    time.sleep(8)  # nothing to connect to
    assert c._thread.is_alive() and store.count() == 0
    broker.start()
    broker.wait_ready()
    produce(topic, [make_event("LATE-1")])
    wait_for(lambda: store.count() == 1, "the mail after the broker came up")
    stop_consumer(c)


def test_event_becomes_one_mail_listed_through_the_grpc_servicer(broker, store, stubs, uniq):
    import server

    topic, group = f"orders.placed.e2e{uniq}", f"g-e2e{uniq}"
    c = new_consumer(store, topic, group)
    c.start()  # the topic does not exist yet: it is auto-created / waited for
    produce(topic, [make_event(f"E2E-{uniq}")])
    wait_for(lambda: store.count() == 1, "the mail")

    srv = server.create_server(store)
    port = srv.add_insecure_port("127.0.0.1:0")
    srv.start()
    try:
        with grpc.insecure_channel(f"127.0.0.1:{port}") as ch:
            stub = stubs.mailbox_pb2_grpc.MailboxServiceStub(ch)
            listed = stub.ListOrderEmails(stubs.mailbox_pb2.ListOrderEmailsRequest(order_ids=[f"E2E-{uniq}"])).emails
            assert len(listed) == 1 and listed[0].html_body == "" and listed[0].to == "customer@example.com"
            full = stub.GetOrderEmail(
                stubs.mailbox_pb2.GetOrderEmailRequest(email_id=listed[0].id, order_id=f"E2E-{uniq}")
            )
            assert "<!DOCTYPE html>" in full.html_body and f"E2E-{uniq}" in full.text_body
    finally:
        srv.stop(0).wait()
        stop_consumer(c)


def test_the_same_event_twice_is_one_mail(broker, store, uniq):
    topic, group = f"orders.placed.dup{uniq}", f"g-dup{uniq}"
    event = make_event(f"DUP-{uniq}")
    c = new_consumer(store, topic, group)
    c.start()
    produce(topic, [event, event, make_event(f"OTHER-{uniq}")])
    wait_for(lambda: c.processed == 3, "all three records")
    assert store.count() == 2
    stop_consumer(c)


def test_restart_resumes_after_the_committed_offset(broker, store, uniq):
    topic, group = f"orders.placed.resume{uniq}", f"g-resume{uniq}"
    first = new_consumer(store, topic, group)
    first.start()
    produce(topic, [make_event(f"R-{uniq}-1"), make_event(f"R-{uniq}-2")])
    wait_for(lambda: store.count() == 2, "the first two mails")
    wait_for(lambda: first.processed == 2, "the commits")
    stop_consumer(first)

    produce(topic, [make_event(f"R-{uniq}-3")])  # while no consumer runs
    second = new_consumer(store, topic, group)
    second.start()
    wait_for(lambda: store.count() == 3, "the mail produced while the consumer was down")
    time.sleep(3)
    assert second.processed == 1  # it did NOT start from the beginning again
    stop_consumer(second)


def test_new_group_starts_from_the_earliest_offset(broker, store, uniq):
    topic = f"orders.placed.early{uniq}"
    produce(topic, [make_event(f"EARLY-{uniq}-1"), make_event(f"EARLY-{uniq}-2")])
    c = new_consumer(store, topic, f"g-early{uniq}")
    c.start()
    wait_for(lambda: store.count() == 2, "both pre-existing events")
    stop_consumer(c)


def test_malformed_and_unsupported_records_are_skipped_and_the_rest_continues(broker, store, uniq):
    from kafka import KafkaProducer

    topic = f"orders.placed.bad{uniq}"
    p = KafkaProducer(bootstrap_servers=BROKERS, bootstrap_timeout_ms=30000)
    p.send(topic, key=b"k", value=b"{not json", partition=0).get(timeout=30)
    p.send(topic, key=b"k", value=json.dumps(make_event("V2", schema_version=2)).encode(), partition=0).get(timeout=30)
    p.send(topic, key=b"k", value=json.dumps(make_event(f"GOOD-{uniq}")).encode(), partition=0).get(timeout=30)
    p.close()
    c = new_consumer(store, topic, f"g-bad{uniq}")
    c.start()
    wait_for(lambda: store.count() == 1, "the good record behind two bad ones")
    assert c.skipped == 2
    stop_consumer(c)
