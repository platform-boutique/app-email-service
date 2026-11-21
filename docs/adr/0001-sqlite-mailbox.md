# 1. The mailbox is a SQLite file

Status: proposed. No written reason existed; the reasons below are reconstructed from the code and have not been confirmed by the owner.

## Context

The shop sends no real mail, but the demo shows the confirmation mail in the storefront (`/emails`). Something has to keep the rendered mails between the consumer and the frontend's reads.

## Decision

Keep the mails in a SQLite file (`MAILBOX_DB`, default `/data/mailbox.db`, WAL mode, one lock, forward-only versioned migrations) instead of a database server. The unique `event_id` makes the consumer idempotent. Tests use `:memory:`.

## Consequences

- No database server to run for a demo feature, and the data is small.
- One replica only, and a persistent volume on Kubernetes; the file is per pod.
- No real delivery, no retention. If the service ever needs replicas or durability, the move is to Postgres like the other services; the versioned migrations make that move safer.
