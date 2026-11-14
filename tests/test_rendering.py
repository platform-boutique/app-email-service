"""Rendering shared by the gRPC path and the consumer: same HTML for the same order."""

import re

import pytest

from rendering import EventError, order_view_from_event, render_html, render_text, subject_for
from tests.conftest import make_event


def _proto_order(stubs, product_id="PROD-001"):
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
                item=pb.CartItem(product_id=product_id, quantity=2),
                cost=pb.Money(currency_code="USD", units=12, nanos=990000000),
            )
        ],
    )


def _cells(html):
    """The text of every table cell, in order (tags and padding stripped)."""
    return [c.strip() for c in re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", html, re.S)]


def test_event_html_has_the_expected_content():
    html = render_html(order_view_from_event(make_event()))
    assert "Order ID: #ORD-123" in html and "Tracking ID: #TRACK-456" in html
    assert "123 Main St, New York, NY, US 10001" in html  # street and state are there
    assert _cells(html) == [
        "Item", "Quantity", "Price", "Total",
        "Linen shirt", "2", "$12.99", "$25.98",
        "Shipping", "$5.00",
        "Total", "$30.98",
    ]  # fmt: skip
    assert "PROD-001" not in html  # the product id is not customer text
    assert html.count("Order ID") == 1 and html.count("Tracking ID") == 1


def test_grpc_order_renders_through_the_same_template_and_falls_back_to_the_id(stubs):
    """The legacy path has no names and no total: the id is shown, the total is items + shipping."""
    html = render_html(_proto_order(stubs))
    assert "Order ID: #ORD-123" in html and "123 Main St, New York, NY, US 10001" in html
    assert _cells(html) == [
        "Item", "Quantity", "Price", "Total",
        "PROD-001", "2", "$12.99", "$25.98",
        "Shipping", "$5.00",
        "Total", "$30.98",
    ]  # fmt: skip


def test_event_and_grpc_agree_apart_from_the_item_label(stubs):
    """Parity: with the name equal to the id, the event renders byte for byte what the gRPC order renders."""
    event = make_event()
    event["items"][0]["name"] = "PROD-001"
    assert render_html(order_view_from_event(event)) == render_html(_proto_order(stubs))
    assert render_text(order_view_from_event(event)) == render_text(_proto_order(stubs))


def test_html_is_inline_styled_and_loads_nothing():
    html = render_html(order_view_from_event(make_event()))
    assert "<script" not in html and "<link" not in html and "<style" not in html and "http" not in html
    assert 'style="' in html


@pytest.mark.parametrize(
    "units,nanos,code,expected",
    [
        (8, 990000000, "USD", "$8.99"),
        (19, 990000000, "USD", "$19.99"),
        (5, 250000000, "USD", "$5.25"),
        (0, 5000000, "USD", "$0.01"),  # 0.005 rounds half up to the cent
        (0, 4999999, "USD", "$0.00"),
        (5, 0, "USD", "$5.00"),
        (1234, 560000000, "USD", "$1,234.56"),
        (5, 250000000, "EUR", "5.25 EUR"),
        (7, 0, "JPY", "7.00 JPY"),
        (3, 500000000, "", "3.50"),
        (-2, -500000000, "USD", "-$2.50"),
    ],
)
def test_money_formatting(units, nanos, code, expected):
    event = make_event(shipping_cost={"currency_code": code, "units": units, "nanos": nanos})
    cells = _cells(render_html(order_view_from_event(event)))
    assert cells[cells.index("Shipping") + 1] == expected


def test_line_total_is_unit_price_times_quantity():
    event = make_event()
    event["items"][0].update(quantity=3, unit_price={"currency_code": "USD", "units": 0, "nanos": 335000000})
    cells = _cells(render_html(order_view_from_event(event)))
    assert cells[cells.index("Linen shirt") + 2 : cells.index("Linen shirt") + 4] == ["$0.34", "$1.01"]  # .335 / 1.005


def test_event_total_is_used_not_recomputed():
    event = make_event(total={"currency_code": "USD", "units": 99, "nanos": 0})
    assert _cells(render_html(order_view_from_event(event)))[-1] == "$99.00"


def test_missing_event_total_is_computed():
    event = make_event()
    del event["total"]
    assert _cells(render_html(order_view_from_event(event)))[-1] == "$30.98"


def test_address_skips_empty_parts():
    event = make_event(
        shipping_address={
            "street_address": "",
            "city": "Mountain View",
            "state": "",
            "country": "United States",
            "zip_code": 94043,
        }
    )
    assert '<p style="margin: 0;">Mountain View, United States 94043</p>' in render_html(order_view_from_event(event))
    assert ", ," not in render_html(order_view_from_event(event))


def test_html_escapes_event_data_everywhere():
    event = make_event(order_id='<script>alert("x")</script>', shipping_tracking_id="<img src=x onerror=1>")
    event["items"][0]["name"] = "<script>alert(1)</script> & <b>bold</b>"
    event["items"][0]["unit_price"]["currency_code"] = "<i>"
    event["shipping_address"].update(street_address="<u>1</u>", city="<b>c</b>", state="<s>", country="<em>")
    html = render_html(order_view_from_event(event))
    for raw in ("<script>", "<img", "<b>", "<i>", "<u>", "<s>", "<em>"):
        assert raw not in html
    assert "&lt;script&gt;alert(1)&lt;/script&gt; &amp; &lt;b&gt;bold&lt;/b&gt;" in html  # inert, escaped exactly once
    assert "&lt;i&gt;" in html and "&amp;amp;" not in html


def test_text_body_has_the_same_content():
    text = render_text(order_view_from_event(make_event()))
    assert text == (
        "Your Order Confirmation\n\nThanks for shopping with us!\n\n"
        "Order ID: #ORD-123\nTracking ID: #TRACK-456\n\n"
        "Shipping address:\n123 Main St, New York, NY, US 10001\n\n"
        "Items:\n- Linen shirt: 2 x $12.99 = $25.98\n\n"
        "Shipping: $5.00\nTotal: $30.98\n"
    )
    assert "PROD-001" not in text


def test_text_body_is_not_html_escaped():
    event = make_event()
    event["items"][0]["name"] = "Salt & Pepper <mix>"
    assert "Salt & Pepper <mix>" in render_text(order_view_from_event(event))


def test_html_is_what_the_old_server_template_produced():
    """rendering.render_html is the template itself: compare with a directly built Jinja environment."""
    import os

    from jinja2 import Environment, FileSystemLoader, select_autoescape

    root = os.path.join(os.path.dirname(__file__), "..", "templates")
    old = Environment(loader=FileSystemLoader(root), autoescape=select_autoescape(["html", "xml"]))
    order = order_view_from_event(make_event())
    assert render_html(order) == old.get_template("confirmation.html").render(order=order)


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
