# 1. The mailbox is a SQLite file

Status: accepted (owner decision 2026-11-22).

## Context

The shop sends no real mail, but the demo shows the confirmation mail in the storefront (`/emails`). Something has to keep the rendered mails between the consumer and the frontend's reads.

The owner's decision context: this is a demo feature with a small amount of data, so a database server (and its cost and operation) would be out of proportion. The local mailbox is enough to show the whole flow, from the `orders.placed` event to the rendered mail on the page.

## Decision

Keep the mails in a SQLite file (`MAILBOX_DB`, default `/data/mailbox.db`, WAL mode, one lock, forward-only versioned migrations) instead of a database server. The unique `event_id` makes the consumer idempotent. Tests use `:memory:`.

Revisit when real mail through AWS SES is built (needs an SES client, a verified sender and a send-vs-store switch; the local mailbox stays the default).

## Consequences

- No database server to run for a demo feature, and the data is small.
- One replica only, and a persistent volume on Kubernetes; the file is per pod.
- No real delivery, no retention. If the service ever needs replicas or durability, the move is to Postgres like the other services; the versioned migrations make that move safer.
