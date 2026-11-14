# Configuration

| Variable | Default | Description |
|---|---|---|
| `PORT` | `8080` | gRPC listen port |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | unset (telemetry off) | OTLP/**gRPC** endpoint for **traces, metrics and logs**: an absolute `http(s)://host:port` URL, e.g. `http://alloy.monitoring.svc.cluster.local:4317`. Telemetry is on only when this is a valid URL; empty or malformed means off (never a crash). The old `COLLECTOR_SERVICE_ADDR` and `ENABLE_TRACING` are no longer read |
| `OTEL_SDK_DISABLED` | — | `true` (trimmed, any case) forces everything off: telemetry and profiling, even with an endpoint set |
| `OTEL_METRIC_EXPORT_INTERVAL` | `15000` | Milliseconds between metric pushes |
| `OTEL_SERVICE_NAME` | `email-service` | `service.name` — becomes the Prometheus `job` and the Loki `service_name`; keep the default, dashboards depend on it |
| `SERVICE_VERSION` | `dev` | `service.version` resource attribute, the `version` log field and the Pyroscope `version` tag |
| `SERVICE_COMMIT` | `unknown` | Commit id, logged once at start (`message: "starting"`); set it at deploy time |
| `DEPLOYMENT_ENVIRONMENT` | `local` | `deployment.environment` resource attribute and the Pyroscope `environment` tag |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warning` (or `warn`) or `error`, case-insensitive; an unknown value means `info` |
| `PYROSCOPE_ADDR` | unset (profiling off) | Profiles use the Pyroscope SDK push protocol (**not** OTLP): an absolute `http(s)://` URL, e.g. `http://pyroscope:4040`, or Alloy. There is no default address; profiling is on only when this is a valid URL. Tags: `service_name`, `version`, `environment` |
| `ENABLE_PROFILING` | — | `0` forces profiling off even with `PYROSCOPE_ADDR` set; `1` alone starts nothing |
| `KAFKA_BROKERS` | empty (Kafka off) | Comma separated bootstrap servers, e.g. `kafka:9092`. Empty or unset = the `orders.placed` consumer is off and the service behaves as before |
| `KAFKA_CONSUMER_GROUP` | `email-service` | Consumer group of the consumer |
| `MAILBOX_DB` | `/data/mailbox.db` | SQLite file of the mailbox (the image creates `/data` for uid 100 and declares it a `VOLUME`); `:memory:` is allowed (tests). With Kafka on, a database that cannot be opened is fatal (exit 1); with Kafka off the service logs a warning and uses an empty in-memory mailbox instead |

Notes:

- No secrets and no mail-server settings exist. Kafka runs without authentication or TLS (`PLAINTEXT`, local demo).
- Telemetry is **pushed**, not scraped: traces, metrics and logs share one OTLP/gRPC endpoint (Alloy); profiles go to Pyroscope (via Alloy). `service.instance.id` is the pod hostname, so replicas stay distinct in Mimir.
- The former Prometheus endpoint (`METRICS_PORT`) no longer exists. Logs go to stdout as JSON lines and, with telemetry on, are additionally exported over OTLP with trace ids.
- With everything unset the service starts with telemetry and profiling off: the OTel SDK and exporters are not even imported, the RPC metric instruments are API no-ops, and the startup line is `{"message":"starting", ... "telemetry":"telemetry off, profiling off"}` (tests run the real server and check it; not re-checked in the image).
- With an endpoint set but no reachable collector, the exporters log retry errors to stderr and the service keeps serving. Resource attributes on every signal: `service.name`, `service.version`, `service.namespace=everly`, `deployment.environment`, `service.instance.id` (hostname).
