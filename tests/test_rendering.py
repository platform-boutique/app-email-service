"""Rendering shared by the gRPC path and the consumer: same HTML for the same order."""

import pytest

from rendering import EventError, order_view_from_event, render_html, render_text, subject_for
from tests.conftest import make_event


def _proto_order(stubs):
    pb = stubs.email_pb2
    return pb.OrderResult(
        order_id="ORD-123",
        shipping_tracking_id="TRACK-456",
        shipping_cost=pb.Money(currency_code="USD", units=5, nanos=0),
        shipping_address=pb.Address(
            street_address="123 Main St", city="New York", state="NY", country="US", zip_code=10001
        ),
        items=[
            pb.OrderItem(
                item=pb.CartItem(product_id="PROD-001", quantity=2),
                cost=pb.Money(currency_code="USD", units=12, nanos=990000000),
            )
        ],
    )


def test_event_renders_the_same_html_as_the_grpc_order(stubs):
    """Parity: the old gRPC path renders template.render(order=<OrderResult proto>); the consumer must match it."""
    from_proto = render_html(_proto_order(stubs))
    from_event = render_html(order_view_from_event(make_event()))
    assert from_event == from_proto
    assert "#ORD-123" in from_event and "PROD-001" in from_event and "12.99 USD" in from_event


def test_html_is_what_the_old_server_template_produced():
    """rendering.render_html is the unchanged template: compare with a directly built Jinja environment."""
    import os

    from jinja2 import Environment, FileSystemLoader, select_autoescape

    root = os.path.join(os.path.dirname(__file__), "..", "templates")
    old = Environment(loader=FileSystemLoader(root), autoescape=select_autoescape(["html", "xml"]))
    order = order_view_from_event(make_event())
    assert render_html(order) == old.get_template("confirmation.html").render(order=order)


def test_html_escapes_event_data():
    html = render_html(order_view_from_event(make_event(order_id='<script>alert("x")</script>')))
    assert "<script>" not in html and "&lt;script&gt;" in html


def test_text_body_has_order_items_and_total():
    text = render_text(order_view_from_event(make_event()))
    assert "#ORD-123" in text and "Linen shirt" in text and "x 2" in text
    assert "12.99 USD" in text and "Total: 30.98 USD" in text
    assert "<" not in text  # plain text


def test_text_body_is_not_html_escaped():
    text = render_text(
        order_view_from_event(make_event(items=[{"product_id": "A&B", "name": "Salt & Pepper", "quantity": 1}]))
    )
    assert "Salt & Pepper" in text


def test_subject():
    assert "ORD-123" in subject_for("ORD-123")


def test_unknown_fields_are_ignored_and_missing_ones_default():
    event = make_event(brand_new_field={"a": 1})
    for key in ("shipping_address", "items", "shipping_cost", "total", "shipping_tracking_id"):
        event.pop(key)
    html = render_html(order_view_from_event(event))
    assert "#ORD-123" in html


@pytest.mark.parametrize(
    "overrides",
    [{"items": "nope"}, {"items": [1]}, {"shipping_cost": []}, {"total": {"units": "abc"}}, {"shipping_address": "x"}],
)
def test_wrong_types_are_an_event_error(overrides):
    with pytest.raises(EventError):
        order_view_from_event(make_event(**overrides))


def test_int64_as_string_is_accepted():
    order = order_view_from_event(make_event(total={"currency_code": "USD", "units": "7", "nanos": 0}))
    assert order.total.units == 7
