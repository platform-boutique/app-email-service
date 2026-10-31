"""Order-confirmation rendering, shared by the gRPC SendOrderConfirmation path and the orders.placed consumer.

The HTML template is the one the service always had (templates/confirmation.html). It reads an order with the
shape of the `OrderResult` proto message (order.items[].item.product_id, .item.quantity, .cost, ...); the consumer
adapts the JSON event into that shape (`order_view_from_event`), so both paths render the same HTML.
"""

import os
from types import SimpleNamespace

from jinja2 import Environment, FileSystemLoader, select_autoescape

env = Environment(
    loader=FileSystemLoader(os.path.join(os.path.dirname(__file__), "..", "templates")),
    autoescape=select_autoescape(["html", "xml"]),
)
html_template = env.get_template("confirmation.html")
# The .txt template is not autoescaped (select_autoescape only covers html/xml): it is plain text.
text_template = env.get_template("confirmation.txt")


def render_html(order) -> str:
    """Render the confirmation HTML for an OrderResult-shaped order. Raises jinja2.TemplateError."""
    return html_template.render(order=order)


def render_text(order) -> str:
    """Plain-text twin of the confirmation, for mail clients (and the mailbox) without HTML."""
    return text_template.render(order=order).strip() + "\n"


def subject_for(order_id: str) -> str:
    return f"Your Everly order confirmation (#{order_id})"


# ---------------------------------------------------------------------------
# JSON event -> OrderResult-shaped view
# ---------------------------------------------------------------------------


class EventError(ValueError):
    """The event is not usable (wrong type, missing key). Deterministic: retrying will not help."""


def _text(value, default="") -> str:
    if value is None:
        return default
    if isinstance(value, str):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return str(value)
    raise EventError(f"expected a string, got {type(value).__name__}")


def _number(value) -> int:
    # proto3 JSON writes int64 as a string, Go's encoding/json as a number: accept both
    if value is None or value == "":
        return 0
    if isinstance(value, bool):
        raise EventError("expected a number, got a bool")
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value == int(value):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError as err:
            raise EventError("expected an integer string") from err
    raise EventError(f"expected a number, got {type(value).__name__}")


def _obj(value, what: str) -> dict:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise EventError(f"{what} must be an object")
    return value


def _money(value, what: str) -> SimpleNamespace:
    m = _obj(value, what)
    return SimpleNamespace(
        currency_code=_text(m.get("currency_code")), units=_number(m.get("units")), nanos=_number(m.get("nanos"))
    )


def order_view_from_event(event: dict) -> SimpleNamespace:
    """Adapt an order.placed event (already a dict) to what the template expects. Unknown fields are ignored.

    Raises EventError on wrong types. The shape is OrderResult plus `name` per item and an optional `total`.
    """
    addr = _obj(event.get("shipping_address"), "shipping_address")
    items = event.get("items")
    if items is None:
        items = []
    if not isinstance(items, list):
        raise EventError("items must be an array")
    view_items = []
    for raw in items:
        it = _obj(raw, "item")
        if not it:
            raise EventError("item must be an object")
        view_items.append(
            SimpleNamespace(
                # the product id is not customer-facing text; the template falls back to it only on the gRPC path
                name=_text(it.get("name")) or "Item",
                item=SimpleNamespace(product_id=_text(it.get("product_id")), quantity=_number(it.get("quantity"))),
                cost=_money(it.get("unit_price"), "unit_price"),
            )
        )
    view = SimpleNamespace(
        order_id=_text(event.get("order_id")),
        shipping_tracking_id=_text(event.get("shipping_tracking_id")),
        shipping_cost=_money(event.get("shipping_cost"), "shipping_cost"),
        shipping_address=SimpleNamespace(
            street_address=_text(addr.get("street_address")),
            city=_text(addr.get("city")),
            state=_text(addr.get("state")),
            country=_text(addr.get("country")),
            zip_code=_number(addr.get("zip_code")),
        ),
        items=view_items,
    )
    # An event without a total (the contract always sends one) gets it computed by the template, like the gRPC path.
    if _obj(event.get("total"), "total"):
        view.total = _money(event.get("total"), "total")
    return view
