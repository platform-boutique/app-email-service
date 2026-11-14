"""SQLite mailbox: the mails the orders.placed consumer "sent". Stdlib sqlite3, no extra dependency.

One connection shared by the consumer thread and the gRPC worker threads, guarded by a lock (a single writer at a
time; the mailbox is tiny). Idempotent by event_id: a replayed event stores nothing new.
"""

import json
import sqlite3
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

SCHEMA = """
CREATE TABLE IF NOT EXISTS emails (
    id         TEXT PRIMARY KEY,
    event_id   TEXT NOT NULL UNIQUE,
    order_id   TEXT NOT NULL,
    to_addr    TEXT NOT NULL,
    subject    TEXT NOT NULL,
    html_body  TEXT NOT NULL,
    text_body  TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS emails_order_id ON emails (order_id);
"""


@dataclass(frozen=True)
class Mail:
    id: str
    order_id: str
    to_addr: str
    subject: str
    html_body: str
    text_body: str
    created_at: datetime  # timezone-aware UTC


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _parse(ts: str) -> datetime:
    return datetime.fromisoformat(ts)


class MailboxStore:
    def __init__(self, path: str):
        self.path = path
        self._lock = threading.Lock()
        # check_same_thread=False: used from the consumer thread and gRPC worker threads, serialized by _lock.
        # isolation_level=None: autocommit, each statement is its own transaction (no transaction left open).
        self._db = sqlite3.connect(path, check_same_thread=False, isolation_level=None, timeout=10)
        self._db.row_factory = sqlite3.Row
        with self._lock:
            self._db.execute("PRAGMA busy_timeout = 10000")
            if path != ":memory:":
                self._db.execute("PRAGMA journal_mode = WAL")
            self._db.executescript(SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._db.close()

    def add(
        self, event_id: str, order_id: str, to_addr: str, subject: str, html_body: str, text_body: str
    ) -> str | None:
        """Store a mail. Returns its id, or None when this event_id was stored before (replay: nothing changes).

        Raises sqlite3.Error when the database is not usable (the caller treats that as transient).
        """
        mail_id = str(uuid.uuid4())
        with self._lock:
            cur = self._db.execute(
                "INSERT OR IGNORE INTO emails"
                " (id, event_id, order_id, to_addr, subject, html_body, text_body, created_at)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (mail_id, event_id, order_id, to_addr, subject, html_body, text_body, _now().isoformat()),
            )
            return mail_id if cur.rowcount == 1 else None

    def list_for_orders(self, order_ids: list[str]) -> list[Mail]:
        """Mails of the given orders, newest first, WITHOUT bodies (the body fields are empty)."""
        if not order_ids:
            return []
        # One bound JSON array instead of a "?,?,..." list built by string formatting: the statement text is a constant.
        with self._lock:
            rows = self._db.execute(
                "SELECT id, order_id, to_addr, subject, created_at FROM emails"
                " WHERE order_id IN (SELECT value FROM json_each(?))"
                " ORDER BY created_at DESC, rowid DESC",
                (json.dumps(order_ids),),
            ).fetchall()
        return [Mail(r["id"], r["order_id"], r["to_addr"], r["subject"], "", "", _parse(r["created_at"])) for r in rows]

    def get(self, email_id: str, order_id: str) -> Mail | None:
        """One mail WITH bodies; None when the id is unknown or the mail belongs to another order."""
        with self._lock:
            r = self._db.execute(
                "SELECT id, order_id, to_addr, subject, html_body, text_body, created_at FROM emails"
                " WHERE id = ? AND order_id = ?",
                (email_id, order_id),
            ).fetchone()
        if r is None:
            return None
        return Mail(
            r["id"], r["order_id"], r["to_addr"], r["subject"], r["html_body"], r["text_body"], _parse(r["created_at"])
        )

    def count(self) -> int:
        with self._lock:
            return self._db.execute("SELECT COUNT(*) FROM emails").fetchone()[0]
