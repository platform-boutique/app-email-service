# 2. Commit the Kafka offset after the mail is stored; idempotent by `event_id`

Status: accepted (reconstructed from the code and the earlier README).

## Context

The consumer reads `orders.placed` events and stores one mail per order. A crash between storing and committing, a rebalance or a republish must not lose a mail or store it twice.

## Decision

Commit by hand, one record at a time, only after the mail is stored. Delivery is therefore at-least-once; the store does `INSERT OR IGNORE` on a UNIQUE `event_id` (a UUID v5 of the order id), so a repeated event stores nothing and its offset is committed. A record that is malformed or cannot be rendered is logged and skipped. A database failure is retried with backoff and never skipped.

## Consequences

- No mail is lost on a crash or rebalance; a replay is recognised.
- A permanently failing database blocks the partition by design.
- The mail's `created_at` is the time it was stored, not the event's `occurred_at`.
- The Kafka client is `kafka-python` (pure Python; `confluent-kafka` has no musllinux wheel), which cannot read compressed batches, so the producers stay uncompressed.
