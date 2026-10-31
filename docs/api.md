# API

Package `hipstershop`, service `EmailService` ([`proto/email.proto`](https://github.com/platform-boutique/app-email-service/blob/main/proto/email.proto)); health in [`proto/health.proto`](https://github.com/platform-boutique/app-email-service/blob/main/proto/health.proto).

| Method | Request | Response | Behaviour |
|---|---|---|---|
| `SendOrderConfirmation` | `{ string email, OrderResult order }` | `Empty` | Renders `templates/confirmation.html` with the order (autoescaped); on success logs "A request to send order confirmation email to <email> has been received."; on template or unexpected error sets gRPC `INTERNAL` |

The rendered HTML is discarded. The template references `street_address_1` / `street_address_2`, which do not exist in the `Address` message, so those render as empty strings. This path does **not** write to the mailbox.

### MailboxService (same port)

Package `hipstershop.mailbox`, service `MailboxService` ([`proto/mailbox.proto`](https://github.com/platform-boutique/app-email-service/blob/main/proto/mailbox.proto)); additive to the existing API.

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
