# Operations

## Health

`grpc.health.v1.Health/Check` on the gRPC port (`8080`) is a static `SERVING`. It deliberately does not look at Kafka, so a broker outage does not remove the pod, and it stays `SERVING` during the short shutdown drain. The image `HEALTHCHECK` (exec form, no shell) uses `grpc.channel_ready_future` against `localhost:$PORT`.

```bash
grpcurl -plaintext localhost:8080 grpc.health.v1.Health/Check
grpcurl -plaintext localhost:8080 list
```

## Shutdown

`SIGTERM` and `SIGINT` are handled, also as PID 1. The Kafka consumer (if on) finishes and commits its current message and closes (bounded at 2 s, in parallel with the drain; a stuck consumer thread is abandoned and its message is redelivered). The server stops accepting RPCs and gives in-flight ones up to 2 s (`server.stop(2)`). Telemetry is flushed, bounded at 2 s, also when the collector is unreachable, and the process exits 0. The worst case is 4 s, which fits a termination grace period of 5 s. A `docker stop` of the idle container was measured by the author on an earlier build (0.36 s, exit 0); it was not repeated for this page.

## Scaling and replicas

With Kafka off the service is stateless. **With Kafka on it holds state**: each replica has its own SQLite file, and the consumer group splits the topic's partitions between the replicas, so with more than one replica a mail lives only in the pod that consumed its order and the mailbox RPCs answer inconsistently. Run **one replica** with a persistent volume at `/data` ([ADR 1](adr/0001-sqlite-mailbox.md)). The mailbox has no retention or pruning and grows with every order. Whether the Helm chart has that volume was not checked for this page; the README says it had none.

## Failure behaviour

| Failure | Behaviour |
|---|---|
| Kafka down, unreachable or topic missing | the client reconnects on its own; gRPC and health are unaffected; the topic is picked up at the next metadata refresh (30 s) |
| `MAILBOX_DB` unusable at start | Kafka on: the process exits (the consumer could not store anything). Kafka off: warning, empty in-memory mailbox |
| Database error while storing | retried in place with backoff 1 s to 30 s, never skipped, nothing committed until stored |
| Commit fails | retried with backoff; after a rebalance the record is redelivered and ignored as a duplicate |
| Malformed event, wrong `schema_version`, other `event_type`, missing fields | warning log with topic, partition, offset and the reason (never the payload), record skipped and committed |
| Event that cannot be rendered | error log with order id and offset, skipped (retrying cannot help) |
| `ListOrderEmails` with more than 50 distinct ids | `INVALID_ARGUMENT` |
| Unknown mail, or a mail of another order | `NOT_FOUND` (`email not found`) |
| Database error in a mailbox RPC | `UNAVAILABLE` |
| Collector unreachable | the exporters log retry errors to stderr and the service keeps serving |

## Runbook

| Symptom | Check |
|---|---|
| No mails in the frontend's `/emails` | is `KAFKA_BROKERS` set? With Kafka off nothing is stored. Then the consumer logs and the order id of the event |
| Mails missing for some orders | checkout logs `EVENT NOT PUBLISHED order=<id>` when the publish failed; nothing re-publishes it |
| Different mails on each refresh | more than one replica; scale to one |
| Same event twice | by design one mail: the unique `event_id` ignores the duplicate (`duplicate event` info log) |
| Mailbox file grows | there is no retention; remove the volume or the rows by hand |
| Process exits at start | with Kafka on: the mailbox file cannot be opened, or its schema version is newer than this release supports (the log line names the path). With Kafka off the service logs a warning and uses an empty in-memory mailbox instead |

There is no dashboard or alert rule for this service in the repository yet (the dashboards are an open task).
