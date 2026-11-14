"""
Unit tests for the Jinja2 confirmation.html template.

These tests exercise the template in isolation — no gRPC, no telemetry.
They verify that the rendered HTML contains the expected content for all
data shapes defined in the proto (single item, multiple items, empty items,
edge-case money values).
"""

import os

import pytest
from jinja2 import Environment, FileSystemLoader, select_autoescape

# ---------------------------------------------------------------------------
# Template fixture — loads the real file from the repo
# ---------------------------------------------------------------------------

TEMPLATES_DIR = os.path.join(os.path.dirname(__file__), "..", "templates")


@pytest.fixture(scope="module")
def jinja_env():
    return Environment(
        loader=FileSystemLoader(TEMPLATES_DIR),
        autoescape=select_autoescape(["html", "xml"]),
    )


@pytest.fixture(scope="module")
def confirmation_template(jinja_env):
    return jinja_env.get_template("confirmation.html")


def render(template, order):
    return template.render(order=order)


# ---------------------------------------------------------------------------
# Basic structure
# ---------------------------------------------------------------------------


class TestTemplateStructure:
    def test_renders_html_doctype(self, confirmation_template, make_order):
        html = render(confirmation_template, make_order())
        assert "<!DOCTYPE html>" in html

    def test_contains_order_id(self, confirmation_template, make_order):
        order = make_order(order_id="ORD-XYZ-789")
        html = render(confirmation_template, order)
        assert "ORD-XYZ-789" in html

    def test_contains_tracking_id(self, confirmation_template, make_order):
        order = make_order(shipping_tracking_id="TRK-ABCDEF")
        html = render(confirmation_template, order)
        assert "TRK-ABCDEF" in html

    def test_contains_items_table(self, confirmation_template, make_order):
        html = render(confirmation_template, make_order())
        # Table header row
        assert ">Item<" in html  # was "Item No." while the column showed product ids
        assert "Quantity" in html
        assert "Price" in html
        assert ">Total<" in html


# ---------------------------------------------------------------------------
# Shipping cost formatting
# ---------------------------------------------------------------------------


class TestShippingCostFormatting:
    def test_shipping_cost_units_present(self, confirmation_template, make_order, make_money):
        order = make_order(shipping_cost=make_money(units=12, nanos=500000000, currency_code="EUR"))
        html = render(confirmation_template, order)
        assert "12" in html

    def test_shipping_cost_currency_code_present(self, confirmation_template, make_order, make_money):
        order = make_order(shipping_cost=make_money(currency_code="GBP", units=7, nanos=0))
        html = render(confirmation_template, order)
        assert "GBP" in html

    def test_shipping_cost_zero_nanos(self, confirmation_template, make_order, make_money):
        """nanos=0 should render as '00' via the format filter."""
        order = make_order(shipping_cost=make_money(units=5, nanos=0))
        html = render(confirmation_template, order)
        assert "00" in html

    def test_shipping_cost_nanos_rounded(self, confirmation_template, make_order, make_money):
        """nanos=990000000 → 99 cents via // 10000000."""
        order = make_order(shipping_cost=make_money(units=5, nanos=990000000))
        html = render(confirmation_template, order)
        assert "99" in html


# ---------------------------------------------------------------------------
# Order items
# ---------------------------------------------------------------------------


class TestOrderItems:
    def test_single_item_product_id_present(self, confirmation_template, make_order, make_money):
        from tests.conftest import _OrderItem

        items = [_OrderItem(product_id="UNIQUE-PROD-42", quantity=1)]
        html = render(confirmation_template, make_order(items=items))
        assert "UNIQUE-PROD-42" in html

    def test_single_item_quantity_present(self, confirmation_template, make_order):
        from tests.conftest import _OrderItem

        items = [_OrderItem(product_id="P1", quantity=7)]
        html = render(confirmation_template, make_order(items=items))
        assert "7" in html

    def test_multiple_items_all_product_ids_present(self, confirmation_template, make_order):
        from tests.conftest import _Money, _OrderItem

        items = [
            _OrderItem(product_id="AAA-111", quantity=2, cost=_Money(units=5)),
            _OrderItem(product_id="BBB-222", quantity=1, cost=_Money(units=10)),
            _OrderItem(product_id="CCC-333", quantity=4, cost=_Money(units=3)),
        ]
        html = render(confirmation_template, make_order(items=items))
        assert "AAA-111" in html
        assert "BBB-222" in html
        assert "CCC-333" in html

    def test_empty_items_renders_no_product_rows(self, confirmation_template, make_order):
        html = render(confirmation_template, make_order(items=[]))
        # Table headers still present, but no product IDs
        assert ">Item<" in html
        # No product cells — crude check that the for loop produced nothing
        assert "#PROD" not in html

    def test_item_cost_currency_shown(self, confirmation_template, make_order):
        from tests.conftest import _Money, _OrderItem

        items = [
            _OrderItem(
                product_id="X",
                quantity=1,
                cost=_Money(currency_code="JPY", units=100, nanos=0),
            )
        ]
        html = render(confirmation_template, make_order(items=items))
        assert "JPY" in html


# ---------------------------------------------------------------------------
# XSS / autoescape
# ---------------------------------------------------------------------------


class TestAutoescape:
    def test_order_id_xss_is_escaped(self, confirmation_template, make_order):
        order = make_order(order_id='<script>alert("xss")</script>')
        html = render(confirmation_template, order)
        assert "<script>" not in html
        assert "&lt;script&gt;" in html

    def test_email_fields_escaped_if_rendered(self, confirmation_template, make_order, make_address):
        addr = make_address(city="<b>Gotham</b>")
        order = make_order(shipping_address=addr)
        html = render(confirmation_template, order)
        assert "<b>Gotham</b>" not in html
