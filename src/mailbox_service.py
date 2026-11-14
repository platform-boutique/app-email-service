"""hipstershop.mailbox.MailboxService: read access to the mailbox (see proto/mailbox.proto)."""

import grpc
from google.protobuf.timestamp_pb2 import Timestamp

from generated import mailbox_pb2, mailbox_pb2_grpc
from mailbox_store import MailboxStore
from telemetry import start_rpc_metrics

MAX_ORDER_IDS = 50
SERVICE_NAME = "hipstershop.mailbox.MailboxService"


def _to_proto(mail, with_bodies: bool):
    created = Timestamp()
    created.FromDatetime(mail.created_at)
    return mailbox_pb2.OrderEmail(
        id=mail.id,
        order_id=mail.order_id,
        to=mail.to_addr,
        subject=mail.subject,
        html_body=mail.html_body if with_bodies else "",
        text_body=mail.text_body if with_bodies else "",
        created_at=created,
    )


class MailboxServicer(mailbox_pb2_grpc.MailboxServiceServicer):
    def __init__(self, store: MailboxStore):
        self._store = store

    def ListOrderEmails(self, request, context):
        end = start_rpc_metrics("ListOrderEmails", SERVICE_NAME)
        # Empty and repeated ids are dropped; an empty request answers with an empty list (the contract's 1-50
        # is the useful range, an empty list is not worth an error for the caller).
        ids = list(dict.fromkeys(i for i in request.order_ids if i))
        if len(ids) > MAX_ORDER_IDS:
            end("3")
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"at most {MAX_ORDER_IDS} order ids per request")
        try:
            mails = self._store.list_for_orders(ids)
        except Exception as err:  # sqlite3.Error and anything unexpected: the caller sees UNAVAILABLE/INTERNAL
            end("14")
            context.abort(grpc.StatusCode.UNAVAILABLE, f"mailbox unavailable: {type(err).__name__}")
        end("0")
        return mailbox_pb2.ListOrderEmailsResponse(emails=[_to_proto(m, False) for m in mails])

    def GetOrderEmail(self, request, context):
        end = start_rpc_metrics("GetOrderEmail", SERVICE_NAME)
        try:
            mail = (
                self._store.get(request.email_id, request.order_id) if request.email_id and request.order_id else None
            )
        except Exception as err:
            end("14")
            context.abort(grpc.StatusCode.UNAVAILABLE, f"mailbox unavailable: {type(err).__name__}")
        if mail is None:  # unknown id, empty ids and "belongs to another order" all give the same answer
            end("5")
            context.abort(grpc.StatusCode.NOT_FOUND, "email not found")
        end("0")
        return _to_proto(mail, True)
