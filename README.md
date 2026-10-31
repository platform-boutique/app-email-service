# email-service

A gRPC service that handles order-confirmation emails for the platform-demo e-commerce platform. **It still sends no real email** (no SMTP, no provider). It does two things:

1. **Kafka path (new, optional):** with `KAFKA_BROKERS` set it consumes `order.placed` events from the topic `orders.placed`, renders the order-confirmation template, logs that a confirmation was "sent" and **stores the mail in a SQLite mailbox** (`MAILBOX_DB`). The mailbox is readable through a second gRPC service, `hipstershop.mailbox.MailboxService`, on the same port (the frontend shows it under `/emails`).
2. **Direct path (unchanged):** `EmailService/SendOrderConfirmation`, used by `checkout-service` while Kafka is off: renders the template to prove the order data is valid, logs the request, writes nothing to the mailbox.

With `KAFKA_BROKERS` empty (the default) the service behaves as before; the mailbox is then simply empty.

Part of the [platform-demo](https://github.com/platform-boutique) platform — microservices, observability (LGTM stack), GitOps (Argo CD), policy enforcement (Kyverno), infrastructure provisioning (Crossplane) and an internal developer portal (Backstage). See the master `README.md` in the platform-demo root for the whole-system view.

## At a glance

| | |
|---|---|
| **Language / runtime** | Python 3.12 in the image (3.13 pinned for local dev), `grpcio`, Jinja2, `kafka-python` (pure Python), stdlib `sqlite3` |
| **Image** | `mladenovskistefan/email-service` |
| **Protocol / port** | gRPC (HTTP/2, plaintext) on `8080` |
| **Metrics** | Pushed over OTLP/gRPC to Alloy → Mimir every 15 s (no scrape endpoint). Dashboard names: `rpc_server_duration_seconds_{bucket,count,sum}`, `rpc_server_requests_total`, `rpc_server_active_requests` |
| **Health** | `grpc.health.v1.Health/Check` on `8080` — static `SERVING` |
| **State** | SQLite mailbox file `MAILBOX_DB` (default `/data/mailbox.db`, WAL; a volume in compose). Empty and unused while Kafka is off |
| **Depends on** | Optional: Kafka (`KAFKA_BROKERS`, topic `orders.placed`), OTLP collector, Pyroscope. The service starts, answers and stays healthy without any of them |
| **Called by** | `checkout-service` (`SendOrderConfirmation`, only while Kafka is off), `frontend` (`MailboxService`). Consumes `orders.placed` (published by checkout) |
| **gRPC reflection** | Not enabled (as before; the contract's "reflection as today" means none) — pass `-proto` to `grpcurl` |

## API

Package `hipstershop`, service `EmailService` ([`proto/email.proto`](proto/email.proto)); health in [`proto/health.proto`](proto/health.proto).

| Method | Request | Response | Behaviour |
|---|---|---|---|
| `SendOrderConfirmation` | `{ string email, OrderResult order }` | `Empty` | Renders `templates/confirmation.html` with the order (autoescaped); on success logs "A request to send order confirmation email to <email> has been received."; on template or unexpected error sets gRPC `INTERNAL` |

The rendered HTML is discarded. The template used to read `street_address_1` / `street_address_2` (not in the `Address` message) and printed money as `8. 99 USD`; it now reads `street_address` and `state` and shows **`$8.99`** for USD and `5.25 EUR` (amount, then code) for other currencies: always 2 decimals, nanos rounded half up, thousands separated. This path has only product ids and no total, so the item column falls back to the id and the total is items + shipping. This path does **not** write to the mailbox.

### The confirmation mail (HTML and plain text)

Both bodies come from `templates/confirmation.html` / `confirmation.txt` (shared macros in `templates/_macros.jinja`) and show: order id and tracking id once, the shipping address without empty parts (`street, city, state, country zip`), an items table (**name**, quantity, unit price, line total; the product id is never shown for an event), a Shipping row and a Total row (`event.total`; computed for the gRPC path). The HTML uses inline styles only, no `<style>`, `<link>`, script or external resource (it is shown in a sandboxed iframe); every value is HTML-escaped exactly once (a test renders a `<script>` product name inert).

### MailboxService (same port)

Package `hipstershop.mailbox`, service `MailboxService` ([`proto/mailbox.proto`](proto/mailbox.proto)); additive to the existing API.

| Method | Request | Response | Behaviour |
|---|---|---|---|
| `ListOrderEmails` | `{ repeated string order_ids }` | `{ repeated OrderEmail emails }` | The mails of the given orders, **newest first, without bodies** (`html_body` / `text_body` empty). Unknown ids give nothing; an empty request gives an empty list; empty and repeated ids are ignored; **more than 50 distinct ids = `INVALID_ARGUMENT`**. The caller (the frontend) must pass only order ids the visitor owns |
| `GetOrderEmail` | `{ email_id, order_id }` | `OrderEmail` | One mail **with** bodies. Unknown id, empty ids and "the mail belongs to another order" all answer `NOT_FOUND` / `email not found` |

Database errors answer `UNAVAILABLE`. The RPC metrics carry `rpc_service="hipstershop.mailbox.MailboxService"`.

### Kafka consumer (`orders.placed`)

Active only when `KAFKA_BROKERS` is non-empty. Event format: `CONTRACT-kafka.md` section 1 (JSON, `schema_version` 1, `event_id` = UUID v5 of the order id).

| Situation | Behaviour |
|---|---|
| New consumer group | Starts at the **earliest** offset |
| Valid event | Render HTML (the same template and code as the gRPC path) and a plain-text twin, store the mail, log `A request to send order confirmation email for order=<id> has been received (orders.placed[<partition>]@<offset>).`, **then** commit the offset (manual commit, one record at a time) |
| Same `event_id` again (republish, redelivery, rebalance) | `INSERT OR IGNORE` on the UNIQUE `event_id`: nothing new is stored, offset committed, `info` log "duplicate event" |
| Malformed JSON, not an object, `schema_version` missing/invalid/not 1, other `event_type`, missing `event_id` / `order_id` / `email` | `warn` log with topic/partition/offset and the reason (never the payload), record skipped and committed |
| Event whose fields have the wrong types or cannot be rendered | `error` log with order id and offset, skipped (retrying cannot help) |
| Database failure (locked, disk full, ...) | Retried in place with backoff 1 s .. 30 s, `error` log per attempt; **never skipped**, nothing committed until stored |
| Commit fails | Retried with backoff; after a rebalance the record is simply redelivered (idempotent) |
| Broker not reachable at start / later | The client is created in a loop (2 s bootstrap timeout, backoff 1 s .. 30 s, `info` log on the first and every 10th attempt) and reconnects on its own; gRPC and health are unaffected (health stays `SERVING`) |
| Topic does not exist yet | `info` log once, waits; the topic is picked up at the next metadata refresh (`metadata_max_age_ms` = 30 s) |
| `SIGTERM` | The current message is finished and committed, the consumer leaves the group and closes, the process exits `0` (see shutdown below) |

The consumer never logs the recipient or the address; the **gRPC path still logs the recipient** as it always did. The Kafka client's own log lines go through the same JSON format, identical lines are rate limited to one per minute. There is no lag/processed/failed counter: this service has no `/metrics` (metrics are OTLP-pushed RPC metrics only); the counts are logs.

## Configuration

| Variable | Default | Description |
|---|---|---|
| `PORT` | `8080` | gRPC listen port |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | unset (telemetry off) | OTLP/**gRPC** endpoint for **traces, metrics and logs** (scheme required), e.g. `http://alloy.monitoring.svc.cluster.local:4317`. Legacy fallback: `COLLECTOR_SERVICE_ADDR` (`host:port`). Telemetry is on only when one of them is set and non-empty |
| `OTEL_SDK_DISABLED` | — | `true` forces telemetry off even with an endpoint set (the legacy `ENABLE_TRACING=0` does the same) |
| `OTEL_METRIC_EXPORT_INTERVAL` | `15000` | Milliseconds between metric pushes |
| `OTEL_SERVICE_NAME` | `email-service` | `service.name` — becomes the Prometheus `job` and the Loki `service_name`; keep the default, dashboards depend on it |
| `SERVICE_VERSION` | `1.0.0` | `service.version` resource attribute |
| `PYROSCOPE_ADDR` | unset (profiling off) | Profiles use the Pyroscope SDK push protocol (**not** OTLP). Point it at Alloy (`http://alloy.monitoring.svc.cluster.local:4040`), which forwards to Pyroscope, or directly at Pyroscope. Profiling is on only when this is set and non-empty |
| `ENABLE_PROFILING` | — | `0` forces profiling off even with `PYROSCOPE_ADDR` set; any other value (or unset) leaves the decision to `PYROSCOPE_ADDR` |
| `KAFKA_BROKERS` | empty (Kafka off) | Comma separated bootstrap servers, e.g. `kafka:9092`. Empty or unset = the `orders.placed` consumer is off and the service behaves as before |
| `KAFKA_CONSUMER_GROUP` | `email-service` | Consumer group of the consumer |
| `MAILBOX_DB` | `/data/mailbox.db` | SQLite file of the mailbox (the image creates `/data` for uid 100 and declares it a `VOLUME`); `:memory:` is allowed (tests). With Kafka on, a database that cannot be opened is fatal (exit 1); with Kafka off the service logs a warning and uses an empty in-memory mailbox instead |

Notes:

- No secrets and no mail-server settings exist. Kafka runs without authentication or TLS (`PLAINTEXT`, local demo).
- Telemetry is **pushed**, not scraped: traces, metrics and logs share one OTLP/gRPC endpoint (Alloy); profiles go to Pyroscope (via Alloy). `service.instance.id` is the pod hostname, so replicas stay distinct in Mimir.
- The former Prometheus endpoint (`METRICS_PORT`) no longer exists. Logs go to stdout as JSON lines and, with telemetry on, are additionally exported over OTLP with trace ids.
- With everything unset the service starts with telemetry and profiling off: the OTel SDK and exporters are not even imported, the RPC metric instruments are API no-ops, and the only startup line is `EmailService gRPC server started on port 8080 (telemetry off, profiling off)` (verified from source and in the image).
- With an endpoint set but no reachable collector, the exporters log retry errors to stderr and the service keeps serving.

## Container image

Multi-stage [`Dockerfile`](Dockerfile):

| Stage | Base | What happens |
|---|---|---|
| builder | `python:3.12-alpine` | installs `g++`/`linux-headers`, `pip install -r requirements.txt` (fully pinned), **compiles the protos** (`email.proto`, `mailbox.proto`) with `grpc_tools.protoc` into `generated/` and patches the relative import in `email_pb2_grpc.py` and `mailbox_pb2_grpc.py` (both import their `_pb2` module absolutely) |
| runtime | `python:3.12-alpine` | copies site-packages, generated stubs (`/app/src/generated`), `src/`, `templates/`; creates `/data` owned by `100:101` (`VOLUME /data`, the mailbox); runs as non-root |

- **Entrypoint:** `python src/server.py` (working dir `/app`; `src/` is on `sys.path`, so `from generated import …` resolves).
- **User:** numeric `USER 100:101` (`appuser`/`appgroup`, created with these fixed ids; `id` in the container prints `uid=100(appuser) gid=101(appgroup)`). Matches the chart's `runAsUser: 100` / `runAsGroup: 101`.
- **EXPOSE:** `8080` only.
- **Docker `HEALTHCHECK`:** Python `grpc.channel_ready_future` against `localhost:${PORT:-8080}` (honours the default).
- **Image size:** 184 MB uncompressed (`docker images`, local build of this branch on 2026-10-31, was 177 MB before `kafka-python`; the earlier ~41 MB figure was not re-measured and is probably the compressed registry size).
- **Kafka client:** `kafka-python` 3.0.11 (pure Python, no dependencies). `confluent-kafka` was rejected because PyPI has no `musllinux` wheel for it (checked with `pip download --only-binary=:all: --platform musllinux_1_2_x86_64`; only `manylinux`), so it would need a source build against `librdkafka` on `python:3.12-alpine`. `kafka-python` 3.0.12 is newer but was released 2026-10-26, less than 7 days before the pin.
- **Reproducibility:** the image installs from the fully pinned `requirements.txt` (compiled from `requirements.in` with `uv`), so transitive versions no longer float between builds. The lock was not regenerated in this change, so its transitive pins are older than what the unpinned build used to pull (e.g. `wrapt 1.17.3` instead of `2.5.1`).
- **Build context:** [`.dockerignore`](.dockerignore) keeps `.git`, tests, docs, caches and locally compiled stubs out of the context.
- **Volume:** the image declares `VOLUME /data`; a named volume inherits the `100:101` ownership (verified: the mailbox file survived a container restart and was writable as uid 100). A bind mount or a Kubernetes volume must be writable for uid 100 / gid 101.
- The generated stubs are **not** in git; they only exist after the Docker build (see local dev below).

## Kubernetes deployment notes

**Workload:** with Kafka off, stateless (scale freely). **With Kafka on it holds state:** every replica has its own SQLite mailbox and the consumer group splits the 3 partitions between them, so with more than one replica a mail lives only in the pod that consumed its order, and `ListOrderEmails` / `GetOrderEmail` (balanced across pods) would answer inconsistently. Run **one replica** with a persistent or emptyDir `/data` until the mailbox moves to shared storage. The Helm chart (2 replicas, HPA, read-only root filesystem, no volume) is **not updated**; that is Deferred work.

**Ports to declare**

| Name | Port | Protocol | Expose via Service? |
|---|---|---|---|
| `grpc` | `8080` | TCP (HTTP/2) | Yes — `checkout-service` and `frontend` dial `email-service:8080` |

**Service DNS name expected by callers:** `email-service` (`checkout-service`: `EMAIL_SERVICE_ADDR=email-service:8080`). `recommendation-service` also listens on `8080` — different Service, no clash.

**Probes**

```yaml
readinessProbe:
  grpc: { port: 8080 }
  initialDelaySeconds: 3
  periodSeconds: 5
livenessProbe:
  grpc: { port: 8080 }
  periodSeconds: 10
  failureThreshold: 3
```

**Graceful shutdown:** `SIGTERM` (and `SIGINT`) is handled even as PID 1: the Kafka consumer (if on) finishes its current message, commits and closes (bounded at 2 s, in parallel with the drain; a stuck consumer thread is abandoned and its message is redelivered), the server stops accepting new RPCs, gives in-flight ones up to 2 s (`server.stop(2)`), flushes telemetry when it is on (bounded at 2 s, also when the collector is unreachable) and exits `0`. The worst case (4 s) fits the chart's `terminationGracePeriodSeconds: 5`. Measured: `docker stop` on the idle container returned in 0.36 s with exit code 0; running from source against a stub OTLP receiver, the final traces, metrics and logs batches arrived after `SIGTERM` and the process exited 0; with nothing listening on the endpoint it exited 0 after the 2 s flush bound (a test covers this). With the consumer on (this branch): `docker stop` returned with exit code 0 in about 0.6 s with Kafka up, and the log shows the consumer stopped within 20 ms of `SIGTERM` with Kafka down; a test starts the real server with an unreachable broker and checks health `SERVING`, exit 0 and under 5 s.

**Security context**

```yaml
securityContext:
  runAsNonRoot: true
  runAsUser: 100
  runAsGroup: 101
  allowPrivilegeEscalation: false
  capabilities: { drop: ["ALL"] }
  readOnlyRootFilesystem: true   # verified in-cluster before the mailbox existed; with Kafka on, /data must be a writable volume (not yet in the chart)
```

**Resources** — requests sized from measured idle usage in the test stack (see the chart's `values.yaml` for limits):

```yaml
resources:
  requests: { cpu: 100m, memory: 64Mi }
  limits:   { cpu: "500m", memory: 192Mi }
```

**Config / secrets:** `KAFKA_BROKERS`, `KAFKA_CONSUMER_GROUP`, `MAILBOX_DB` (no secrets; Kafka is plaintext without auth). If this ever becomes a real mailer, SMTP credentials belong in a `Secret`. The mailbox holds recipient addresses and order contents in clear text on the volume.

**Telemetry:** pushed, not scraped — OTLP/gRPC to Alloy (`global.monitoring.otlpEndpoint`), profiles to Alloy `:4040`. Metrics carry `job="email-service"` and `instance=<pod>`; no scrape annotations or `ServiceMonitor` are needed.

**Helm chart:** [`platform-demo-helm/email-service`](https://github.com/platform-boutique/platform-demo-helm/tree/main/email-service) (built on the shared `platform-common` library chart). It ships with HA defaults — 2 replicas,
HPA, PodDisruptionBudget, topology spread, NetworkPolicy, restricted pod security context, probes and the full env contract (see Configuration) —
and was verified under the `restricted` Pod Security Standard on the earlier local 3-node kind cluster (since retired). The platform now targets AWS EKS, where this chart has not been deployed or verified yet.

**Deployment and admission policies:** Argo CD deploys the chart from `platform-demo-helm` (`envs/<env>/values/email-service.yaml` pins the image digest; promotion is a pull request). In the cluster Kyverno enforces, for this service: pods named `app.kubernetes.io/name: email-service` that run under the ServiceAccount of the same name (Cilium derives the network identity from that label), an image from this repository pinned by digest and signed by our release pipeline, a read-only root filesystem, resource requests and a memory limit, readiness and liveness probes and no API token. The Helm CI evaluates the rendered chart against these policies before merge; see [`platform-demo-helm/policies`](https://github.com/platform-boutique/platform-demo-helm/tree/main/policies).

## Observability

| Signal | Implementation | Destination |
|---|---|---|
| **Traces** | OpenTelemetry SDK, `GrpcInstrumentorServer`, OTLP/gRPC exporter (`BatchSpanProcessor`); only when telemetry is on | Alloy `:4317` → Tempo |
| **Metrics** | Hand-rolled RPC instruments, `PeriodicExportingMetricReader` over OTLP/gRPC every 15 s (replaces the Prometheus reader) | Alloy → Mimir |
| **Logs** | Python `logging` with `python-json-logger` to stdout (always) **and** an OTel `LoggingHandler` (OTLP/gRPC, with trace ids; only when telemetry is on) | Alloy → Loki |
| **Profiles** | `pyroscope-io`, tagged with `version`; only when profiling is on | Alloy `:4040` → Pyroscope |

Metric names: `rpc_server_duration_seconds_{bucket,count,sum}` (buckets 1 ms … 10 s; the unit `s` yields `_seconds`), `rpc_server_requests_total`, `rpc_server_active_requests` (labels `rpc_system`, `rpc_service`, `rpc_method`, `rpc_grpc_status_code`; `rpc_service` is `hipstershop.EmailService` or `hipstershop.mailbox.MailboxService`). There is no Kafka lag/processed/failed metric (no `/metrics` here); the consumer reports through logs: one `info` line per stored mail (`order=<id>`, `orders.placed[<partition>]@<offset>`), `warn` for skipped records, `error` for retried failures. The payload (email, address) is never logged by the consumer. Each stdout line is one valid JSON object with the keys `timestamp` (epoch seconds), `severity`, `name`, `message` (quotes and newlines in messages are escaped; a test asserts this).

Every series carries `job="email-service"` (from `service.name`) and `instance=<pod name>` (`service.instance.id`), which is what the Grafana dashboards select on. Logs land in Loki with `service_name="email-service"` and trace/span ids, so a log line links to its trace.

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

## Verification status (what was run, what was not)

Run on this branch (2026-10-31, Python 3.12.3 venv with the pinned `requirements.txt`; the 3.13 venv was only used for the lifecycle tests, see below):

| What | Result |
|---|---|
| `ruff check .` and `ruff format --check .` | clean |
| `pytest` (no generated stubs in `src/generated`, as in CI) | 146 passed, 6 skipped (the Kafka integration tests); the pre-existing tests are unchanged except the two column-header asserts in `tests/test_template.py` ("Item No." is now "Item") and the stub compilation in the `app_dir` fixture of `tests/test_lifecycle.py`, which now includes `mailbox.proto` |
| `KAFKA_INTEGRATION=1 pytest tests/test_kafka_integration.py` against a throwaway `apache/kafka:4.3.2` | 6 passed: broker started **after** the consumer (it waits, then catches up), event to one mail listed/fetched through the real gRPC servicer, the same event twice = one mail, restart resumes after the committed offset (the second consumer processed exactly the one new record), a new group reads from the earliest offset, malformed + `schema_version` 2 records skipped and the good one behind them stored |
| `docker build -t email-service:local .` and a container smoke test (run before the template rewrite of 2026-10-31 evening; the image was not rebuilt after it) on a private docker network with a throwaway Kafka and a named volume | image 184 MB; service started before the broker was up, recovered, consumed an event (1 mail), a duplicate was ignored; `ListOrderEmails` / `GetOrderEmail` via `grpcurl -proto` returned the mail (with bodies only in `GetOrderEmail`), wrong `order_id` = `NOT_FOUND`, old `SendOrderConfirmation` still `{}`, health `SERVING` also with the broker stopped; the mailbox survived a container restart and the restart did not re-read old offsets; `docker stop` exit code 0 (about 0.6 s with Kafka up, consumer stopped within 20 ms of `SIGTERM` with Kafka down); the service restarted while the broker was gone, and a broker created afterwards was picked up (second event stored, about 40 s after the produce) |

Not run:

- Kubernetes / Helm / Argo / Kyverno (the chart has no `/data` volume and runs 2 replicas, see Kubernetes), AWS, the CI pipeline, Trivy.
- Telemetry on together with the consumer (OTLP/Pyroscope with Kafka on): only the telemetry-off container and the existing telemetry tests were exercised.
- More than one replica / partition rebalancing between two live consumers, a `kill -9` of the process between store and commit (the idempotency is covered by the replay tests, the crash itself was not), a database failure in the container (covered with a fake client and a broken store in the unit tests only), and the real frontend and checkout against this service.
- A topic that is created later than the consumer was only seen in the integration run (the consumer logs/waits), not measured for latency.
- The Kafka client under load or with a large backlog.
- The lifecycle tests (`tests/test_lifecycle.py`, existing) are timing based and flaky on this loaded WSL2 host with a jumping clock, on the unchanged baseline too (on Python 3.13 one or two of them fail in most runs and pass when run again; on 3.12 the full run passed).

## Known limitations

- Does not send email; the mailbox is the only place a "sent" mail exists.
- The template has no logo or web font (no external resources: the mail is shown in a sandboxed iframe); the legacy gRPC path renders ids and a computed total because that request carries no names or total.
- The gRPC path logs the recipient address (as before); the consumer path never logs email or address.
- The mailbox is one SQLite file per replica: **run one replica** with Kafka on (see Kubernetes). No retention or pruning: it grows with every order.
- The mailbox stores recipients and bodies in clear text; access control is the caller's job (`ListOrderEmails` trusts the order ids it is given).
- Delivery is at-least-once: after a crash between the store and the commit the event is read again and ignored by the unique `event_id`. A mail's `created_at` is the time it was stored, not the event's `occurred_at`.
- A record whose fields have the wrong types, or that cannot be rendered, is skipped (logged at `error`), not retried.
- No consumer lag metric; no Kafka authentication/TLS.
- `ListOrderEmails` rejects more than 50 distinct order ids (`INVALID_ARGUMENT`) instead of truncating.
- Health is static `SERVING`: it does not look at Kafka (a broker outage must not remove the pod) and stays `SERVING` during the 2 s shutdown drain; the pod is taken out of the Service by Kubernetes on termination, not by the probe.
- `requirements.txt` was compiled for Python 3.13 (`--python-version 3.13`) while the image runs 3.12; it installs and runs there (verified by the image build and the local test run on Python 3.12), but the lock was not recompiled for 3.12.
