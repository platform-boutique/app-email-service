"""MailboxService through a real in-process gRPC server (the same create_server() main() uses)."""

import grpc
import pytest

from tests.conftest import make_event


@pytest.fixture()
def api(stubs, store):
    import server

    srv = server.create_server(store)
    port = srv.add_insecure_port("127.0.0.1:0")
    srv.start()
    channel = grpc.insecure_channel(f"127.0.0.1:{port}")
    grpc.channel_ready_future(channel).result(timeout=30)
    yield stubs, stubs.mailbox_pb2_grpc.MailboxServiceStub(channel), channel
    channel.close()
    srv.stop(0).wait()


def _add(store, n, order_id="ORD-1", **kw):
    return store.add(f"evt-{order_id}-{n}", order_id, "a@example.com", f"subject {n}", f"<p>{n}</p>", f"text {n}")


def test_list_has_no_bodies_and_is_newest_first(api, store):
    stubs, mailbox, _ = api
    _add(store, 1)
    _add(store, 2, "ORD-2")
    resp = mailbox.ListOrderEmails(stubs.mailbox_pb2.ListOrderEmailsRequest(order_ids=["ORD-1", "ORD-2", "ORD-9"]))
    assert [e.subject for e in resp.emails] == ["subject 2", "subject 1"]
    assert all(e.html_body == "" and e.text_body == "" for e in resp.emails)
    assert resp.emails[0].to == "a@example.com" and resp.emails[0].order_id == "ORD-2"
    assert resp.emails[0].created_at.seconds > 1_700_000_000


def test_list_of_unknown_orders_and_empty_request_is_empty(api, store):
    stubs, mailbox, _ = api
    _add(store, 1)
    req = stubs.mailbox_pb2.ListOrderEmailsRequest
    assert list(mailbox.ListOrderEmails(req(order_ids=["nope"])).emails) == []
    assert list(mailbox.ListOrderEmails(req()).emails) == []


def test_list_caps_at_50_ids(api):
    stubs, mailbox, _ = api
    req = stubs.mailbox_pb2.ListOrderEmailsRequest
    mailbox.ListOrderEmails(req(order_ids=[f"o{i}" for i in range(50)]))  # exactly 50 is fine
    with pytest.raises(grpc.RpcError) as err:
        mailbox.ListOrderEmails(req(order_ids=[f"o{i}" for i in range(51)]))
    assert err.value.code() == grpc.StatusCode.INVALID_ARGUMENT


def test_get_returns_bodies(api, store):
    stubs, mailbox, _ = api
    mail_id = _add(store, 1)
    mail = mailbox.GetOrderEmail(stubs.mailbox_pb2.GetOrderEmailRequest(email_id=mail_id, order_id="ORD-1"))
    assert mail.id == mail_id and mail.html_body == "<p>1</p>" and mail.text_body == "text 1"


@pytest.mark.parametrize("email_id,order_id", [("known", "ORD-2"), ("unknown", "ORD-1"), ("", "ORD-1"), ("known", "")])
def test_get_with_wrong_order_or_unknown_id_is_not_found(api, store, email_id, order_id):
    stubs, mailbox, _ = api
    mail_id = _add(store, 1)
    email_id = mail_id if email_id == "known" else email_id
    with pytest.raises(grpc.RpcError) as err:
        mailbox.GetOrderEmail(stubs.mailbox_pb2.GetOrderEmailRequest(email_id=email_id, order_id=order_id))
    assert err.value.code() == grpc.StatusCode.NOT_FOUND
    assert err.value.details() == "email not found"  # the same answer for every reason


def test_send_order_confirmation_still_works_and_does_not_write_to_the_mailbox(api, store):
    stubs, _, channel = api
    pb = stubs.email_pb2
    stub = stubs.email_pb2_grpc.EmailServiceStub(channel)
    stub.SendOrderConfirmation(
        pb.SendOrderConfirmationRequest(email="x@example.com", order=pb.OrderResult(order_id="ORD-7")), timeout=10
    )
    assert store.count() == 0


def test_health_is_serving(api):
    from grpc_health.v1 import health_pb2, health_pb2_grpc

    _, _, channel = api
    resp = health_pb2_grpc.HealthStub(channel).Check(health_pb2.HealthCheckRequest())
    assert resp.status == health_pb2.HealthCheckResponse.SERVING


def test_a_broken_database_is_unavailable_not_a_crash(api, store):
    stubs, mailbox, _ = api
    store.close()
    with pytest.raises(grpc.RpcError) as err:
        mailbox.ListOrderEmails(stubs.mailbox_pb2.ListOrderEmailsRequest(order_ids=["ORD-1"]))
    assert err.value.code() == grpc.StatusCode.UNAVAILABLE


def test_mail_from_an_event_is_listed_with_its_subject(api, store):
    from consumer import STORED, Position, handle_message

    stubs, mailbox, _ = api
    assert (
        handle_message(store, Position("orders.placed", 0, 5), __import__("json").dumps(make_event()).encode())
        == STORED
    )
    resp = mailbox.ListOrderEmails(stubs.mailbox_pb2.ListOrderEmailsRequest(order_ids=["ORD-123"]))
    assert len(resp.emails) == 1 and "ORD-123" in resp.emails[0].subject
