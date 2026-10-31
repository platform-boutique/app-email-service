# Develop & test

## Running locally

Requirements: Python 3.13 (see `.python-version`; 3.12 also works), Docker, [`grpcurl`](https://github.com/fullstorydev/grpcurl).

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt -r requirements-dev.txt

# generate the gRPC stubs (what the Dockerfile does) — required before running from source
mkdir -p src/generated && python -m grpc_tools.protoc -I proto \
  --python_out=src/generated --grpc_python_out=src/generated proto/email.proto proto/mailbox.proto
touch src/generated/__init__.py
sed -i 's/^import email_pb2 as email__pb2/from . import email_pb2 as email__pb2/' src/generated/email_pb2_grpc.py
sed -i 's/^import mailbox_pb2 as mailbox__pb2/from . import mailbox_pb2 as mailbox__pb2/' src/generated/mailbox_pb2_grpc.py

python src/server.py          # run (telemetry and profiling off unless their variables are set)
pytest                        # tests (pytest.ini sets pythonpath=src)
# tests/test_lifecycle.py compiles the proto into a temp dir itself (needs grpcio-tools from requirements.txt), starts
# the real server with every telemetry variable unset, checks the JSON log lines and that SIGTERM exits 0
# the mailbox tests compile both protos into a temp dir too (the `stubs` fixture), so `pytest` needs no generated stubs
# integration tests against a real broker are skipped unless asked for; they start and remove their own
# apache/kafka:4.3.2 container on 127.0.0.1:19093 (needs docker and the image):
KAFKA_INTEGRATION=1 pytest tests/test_kafka_integration.py
ruff check . && ruff format --check .      # lint (line length 120)

docker build -t email-service .
docker run --rm -p 8080:8080 email-service
# with a mailbox volume and a broker (KAFKA_BROKERS empty = Kafka off)
docker run --rm -p 8080:8080 -v email-mailbox:/data -e KAFKA_BROKERS=host.docker.internal:9092 email-service
```

To regenerate pinned deps: `uv pip compile requirements.in -o requirements.txt` (the lockfile header shows `uv` was used).

### Manual gRPC testing (no reflection — pass the proto)

```bash
grpcurl -plaintext -proto proto/email.proto -d '{
  "email": "test@example.com",
  "order": {
    "order_id": "ord-123",
    "shipping_tracking_id": "track-456",
    "shipping_cost": {"currency_code": "USD", "units": 8, "nanos": 990000000},
    "shipping_address": {"street_address": "1 Main St", "city": "Brooklyn", "state": "NY", "country": "US", "zip_code": 11201},
    "items": [{"item": {"product_id": "OLJCESPC7Z", "quantity": 1}, "cost": {"currency_code": "USD", "units": 19, "nanos": 990000000}}]
  }
}' localhost:8080 hipstershop.EmailService/SendOrderConfirmation

grpcurl -plaintext -proto proto/health.proto localhost:8080 grpc.health.v1.Health/Check

# the mailbox (needs a stored mail, i.e. Kafka on and an order.placed event consumed)
grpcurl -plaintext -proto proto/mailbox.proto -d '{"order_ids": ["ord-123"]}' localhost:8080 hipstershop.mailbox.MailboxService/ListOrderEmails
grpcurl -plaintext -proto proto/mailbox.proto -d '{"email_id": "<id from the list>", "order_id": "ord-123"}' localhost:8080 hipstershop.mailbox.MailboxService/GetOrderEmail
```

## Project structure

```
├── src/
│   ├── server.py        # gRPC server (10-thread pool), EmailService + MailboxService + Health on one port, JSON logs, SIGTERM
│   ├── rendering.py     # Jinja2 rendering shared by the gRPC path and the consumer, event -> template view
│   ├── consumer.py      # orders.placed consumer (kafka-python): parse/skip rules, retry, manual commit
│   ├── mailbox_store.py # SQLite mailbox (stdlib sqlite3, WAL, one lock), idempotent by event_id
│   ├── mailbox_service.py # MailboxService servicer
│   ├── telemetry.py     # telemetry contract, OTel traces/metrics/logs, Pyroscope, bounded shutdown; imported first
│   ├── generated/       # protoc output (git: only __init__.py; real stubs are built in Docker)
│   └── __init__.py
├── templates/confirmation.html     # Jinja2 order-confirmation template (inline styles only)
├── templates/confirmation.txt      # plain-text twin, stored with the mail
├── templates/_macros.jinja         # money / address / total macros shared by both
├── proto/               # email.proto, mailbox.proto, health.proto
├── requirements.in / requirements.txt / requirements-dev.txt
├── pyproject.toml / pytest.ini / .python-version / .trivyignore
├── Dockerfile / .dockerignore
└── docs/                # empty placeholder
```
