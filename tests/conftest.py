"""
Shared pytest fixtures for the email-service test suite.

All fixtures are auto-discovered by pytest — no imports needed in test files.
We use plain Python objects instead of generated protobuf stubs so the unit
tests can run without a grpc compilation step.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

# ---------------------------------------------------------------------------
# Lightweight proto message stand-ins
# ---------------------------------------------------------------------------


class _Money:
    def __init__(self, currency_code="USD", units=10, nanos=990000000):
        self.currency_code = currency_code
        self.units = units
        self.nanos = nanos


class _Address:
    def __init__(
        self,
        street_address="123 Main St",
        city="New York",
        state="NY",
        country="US",
        zip_code=10001,
    ):
        self.street_address = street_address
        self.city = city
        self.state = state
        self.country = country
        self.zip_code = zip_code


class _CartItem:
    def __init__(self, product_id="PROD-001", quantity=2):
        self.product_id = product_id
        self.quantity = quantity


class _OrderItem:
    def __init__(self, product_id="PROD-001", quantity=2, cost=None):
        self.item = _CartItem(product_id=product_id, quantity=quantity)
        self.cost = cost or _Money()


class _OrderResult:
    def __init__(
        self,
        order_id="ORD-123",
        shipping_tracking_id="TRACK-456",
        shipping_cost=None,
        shipping_address=None,
        items=None,
    ):
        self.order_id = order_id
        self.shipping_tracking_id = shipping_tracking_id
        self.shipping_cost = shipping_cost or _Money(units=5, nanos=990000000)
        self.shipping_address = shipping_address or _Address()
        self.items = items if items is not None else [_OrderItem()]


class _SendOrderConfirmationRequest:
    def __init__(self, email="customer@example.com", order=None):
        self.email = email
        self.order = order or _OrderResult()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def valid_request():
    """Well-formed SendOrderConfirmationRequest with a single order item."""
    return _SendOrderConfirmationRequest()


@pytest.fixture()
def multi_item_request():
    """Request carrying multiple order items."""
    items = [
        _OrderItem(product_id="PROD-001", quantity=1, cost=_Money(units=9, nanos=990000000)),
        _OrderItem(product_id="PROD-002", quantity=3, cost=_Money(units=24, nanos=970000000)),
    ]
    order = _OrderResult(order_id="ORD-999", items=items)
    return _SendOrderConfirmationRequest(email="bulk@example.com", order=order)


@pytest.fixture()
def empty_items_request():
    """Request whose order contains no line items."""
    return _SendOrderConfirmationRequest(order=_OrderResult(items=[]))


@pytest.fixture()
def mock_grpc_context():
    """Minimal mock of a gRPC ServicerContext."""
    ctx = MagicMock()
    ctx.set_code = MagicMock()
    ctx.set_details = MagicMock()
    return ctx


# Expose the builder classes as fixtures so tests can construct custom objects.
@pytest.fixture()
def make_money():
    return _Money


@pytest.fixture()
def make_address():
    return _Address


@pytest.fixture()
def make_order():
    return _OrderResult


@pytest.fixture()
def make_request():
    return _SendOrderConfirmationRequest


# ---------------------------------------------------------------------------
# Real proto stubs (compiled into a temp dir, like the Dockerfile does) and helpers for the mailbox tests
# ---------------------------------------------------------------------------

import importlib  # noqa: E402
import json  # noqa: E402
import subprocess  # noqa: E402
import sys  # noqa: E402
from pathlib import Path  # noqa: E402

_ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(scope="session")
def _stub_dir(tmp_path_factory):
    pytest.importorskip("grpc_tools")
    out = tmp_path_factory.mktemp("stubs")
    subprocess.run(
        [
            sys.executable,
            "-m",
            "grpc_tools.protoc",
            f"-I{_ROOT / 'proto'}",
            f"--python_out={out}",
            f"--grpc_python_out={out}",
            str(_ROOT / "proto" / "email.proto"),
            str(_ROOT / "proto" / "mailbox.proto"),
        ],
        check=True,
    )
    for name in ("email", "mailbox"):  # the import fix the Dockerfile applies with sed
        stub = out / f"{name}_pb2_grpc.py"
        stub.write_text(stub.read_text().replace(f"import {name}_pb2 as", f"from . import {name}_pb2 as"))
    return out


@pytest.fixture()
def stubs(_stub_dir):
    """The generated stubs as `generated.email_pb2`, `generated.mailbox_pb2` ... (src/generated holds none in git).

    The compiled files are appended to the `generated` package path; works whether or not locally compiled stubs
    already exist in src/generated, and again after another test removed `generated` from sys.modules."""
    generated = importlib.import_module("generated")
    if str(_stub_dir) not in generated.__path__:
        generated.__path__.append(str(_stub_dir))
    return SimpleNamespace(
        email_pb2=importlib.import_module("generated.email_pb2"),
        email_pb2_grpc=importlib.import_module("generated.email_pb2_grpc"),
        mailbox_pb2=importlib.import_module("generated.mailbox_pb2"),
        mailbox_pb2_grpc=importlib.import_module("generated.mailbox_pb2_grpc"),
    )


def make_event(order_id="ORD-123", **overrides):
    """An order.placed event as the contract defines it (section 1)."""
    event = {
        "event_id": f"evt-{order_id}",
        "event_type": "order.placed",
        "schema_version": 1,
        "occurred_at": "2026-10-31T18:00:00.123Z",
        "order_id": order_id,
        "user_id": "",
        "email": "customer@example.com",
        "shipping_address": {
            "street_address": "123 Main St",
            "city": "New York",
            "state": "NY",
            "country": "US",
            "zip_code": 10001,
        },
        "items": [
            {
                "product_id": "PROD-001",
                "name": "Linen shirt",
                "quantity": 2,
                "unit_price": {"currency_code": "USD", "units": 12, "nanos": 990000000},
                "picture": "/static/img/x.jpg",
            }
        ],
        "shipping_cost": {"currency_code": "USD", "units": 5, "nanos": 0},
        "total": {"currency_code": "USD", "units": 30, "nanos": 980000000},
        "shipping_tracking_id": "TRACK-456",
        "payment_transaction_id": "TX-1",
    }
    event.update(overrides)
    return event


def event_bytes(order_id="ORD-123", **overrides) -> bytes:
    return json.dumps(make_event(order_id, **overrides)).encode()


@pytest.fixture()
def store():
    from mailbox_store import MailboxStore

    s = MailboxStore(":memory:")
    yield s
    s.close()
