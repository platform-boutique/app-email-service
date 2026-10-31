# Known limitations

- Does not send email; the mailbox is the only place a "sent" mail exists.
- Template placeholders `street_address_1/2` don't exist in the proto (the event's single `street_address` is therefore not shown in the HTML either; the plain-text body shows it).
- The gRPC path logs the recipient address (as before); the consumer path never logs email or address.
- The mailbox is one SQLite file per replica: **run one replica** with Kafka on (see Kubernetes). No retention or pruning: it grows with every order.
- The mailbox stores recipients and bodies in clear text; access control is the caller's job (`ListOrderEmails` trusts the order ids it is given).
- Delivery is at-least-once: after a crash between the store and the commit the event is read again and ignored by the unique `event_id`. A mail's `created_at` is the time it was stored, not the event's `occurred_at`.
- A record whose fields have the wrong types, or that cannot be rendered, is skipped (logged at `error`), not retried.
- No consumer lag metric; no Kafka authentication/TLS.
- `ListOrderEmails` rejects more than 50 distinct order ids (`INVALID_ARGUMENT`) instead of truncating.
- Health is static `SERVING`: it does not look at Kafka (a broker outage must not remove the pod) and stays `SERVING` during the 2 s shutdown drain; the pod is taken out of the Service by Kubernetes on termination, not by the probe.
- `requirements.txt` was compiled for Python 3.13 (`--python-version 3.13`) while the image runs 3.12; it installs and runs there (verified by the image build and the local test run on Python 3.12), but the lock was not recompiled for 3.12.
