"""SQLite mailbox: the mails the orders.placed consumer "sent". Stdlib sqlite3, no extra dependency.

One connection shared by the consumer thread and the gRPC worker threads, guarded by a lock (a single writer at a
time; the mailbox is tiny). Idempotent by event_id: a replayed event stores nothing new.
"""

import json
import sqlite3
import threading
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone


class SchemaError(sqlite3.DatabaseError):
    """The database schema cannot be brought to the version this code needs (never silently ignored)."""


# Forward-only migrations (CODE-08): (version, statements). The applied version is kept in `PRAGMA user_version`,
# which is changed in the same transaction as the statements, so a failed migration leaves nothing behind.
# Never edit a released migration: add the next number. Version 1 is the schema this service always had; it is written
# with IF NOT EXISTS so that a database created before versioning existed (user_version 0, tables present) is adopted.
MIGRATIONS: tuple[tuple[int, tuple[str, ...]], ...] = (
    (
        1,
        (
            """CREATE TABLE IF NOT EXISTS emails (
    id         TEXT PRIMARY KEY,
    event_id   TEXT NOT NULL UNIQUE,
    order_id   TEXT NOT NULL,
    to_addr    TEXT NOT NULL,
    subject    TEXT NOT NULL,
    html_body  TEXT NOT NULL,
    text_body  TEXT NOT NULL,
    created_at TEXT NOT NULL
)""",
            "CREATE INDEX IF NOT EXISTS emails_order_id ON emails (order_id)",
        ),
    ),
)


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


def migrate(db: sqlite3.Connection, migrations=MIGRATIONS) -> int:
    """Bring `db` (autocommit connection) to the newest version in `migrations`; returns the version it is at.

    Each migration runs in its own BEGIN IMMEDIATE transaction and the version is re-read inside it, so two processes
    starting together apply it once. A database NEWER than this code knows raises SchemaError (an older release must
    not run on a schema it does not understand); a failing migration is rolled back and re-raised.
    """
    latest = migrations[-1][0] if migrations else 0
    current = db.execute("PRAGMA user_version").fetchone()[0]
    if current > latest:
        raise SchemaError(f"database schema version {current} is newer than this release supports ({latest})")
    for version, statements in migrations:
        if version <= current:
            continue
        db.execute("BEGIN IMMEDIATE")
        try:
            if db.execute("PRAGMA user_version").fetchone()[0] >= version:
                db.execute("ROLLBACK")  # another process applied it while we waited for the lock
                continue
            for statement in statements:
                db.execute(statement)
            db.execute(f"PRAGMA user_version = {int(version)}")  # int(): the value is part of the statement text
            db.execute("COMMIT")
        except BaseException:
            if db.in_transaction:
                db.execute("ROLLBACK")
            raise
    return db.execute("PRAGMA user_version").fetchone()[0]


def _parse(ts: str) -> datetime:
    return datetime.fromisoformat(ts)


class MailboxStore:
    def __init__(self, path: str, clock: Callable[[], datetime] = _now, migrations=MIGRATIONS):
        """`clock` returns the timezone-aware UTC time stamped on new mails (injected in tests, CODE-07)."""
        self.path = path
        self._clock = clock
        self._lock = threading.Lock()
        # check_same_thread=False: used from the consumer thread and gRPC worker threads, serialized by _lock.
        # isolation_level=None: autocommit, each statement is its own transaction (no transaction left open).
        self._db = sqlite3.connect(path, check_same_thread=False, isolation_level=None, timeout=10)
        self._db.row_factory = sqlite3.Row
        with self._lock:
            self._db.execute("PRAGMA busy_timeout = 10000")
            if path != ":memory:":
                self._db.execute("PRAGMA journal_mode = WAL")
            self._schema_version = migrate(self._db, migrations)

    @property
    def schema_version(self) -> int:
        """The schema version the database is at after the migrations ran."""
        return self._schema_version

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
                (mail_id, event_id, order_id, to_addr, subject, html_body, text_body, self._clock().isoformat()),
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
