"""The SQLite mailbox: idempotency, ordering, ownership, file mode."""

import sqlite3
import threading

import pytest

import mailbox_store
from mailbox_store import MIGRATIONS, MailboxStore, SchemaError


def test_add_is_idempotent_by_event_id(store):
    first = store.add("evt-1", "ORD-1", "a@example.com", "s", "<p>h</p>", "t")
    again = store.add("evt-1", "ORD-1", "a@example.com", "s", "<p>h</p>", "t")
    assert first is not None and again is None
    assert store.count() == 1


def test_same_order_with_another_event_id_is_another_mail(store):
    store.add("evt-1", "ORD-1", "a@example.com", "s", "h", "t")
    store.add("evt-2", "ORD-1", "a@example.com", "s", "h", "t")
    assert store.count() == 2


def test_list_is_newest_first_without_bodies(store):
    store.add("evt-1", "ORD-1", "a@example.com", "first", "<p>1</p>", "t1")
    store.add("evt-2", "ORD-2", "b@example.com", "second", "<p>2</p>", "t2")
    store.add("evt-3", "ORD-3", "c@example.com", "other order", "<p>3</p>", "t3")
    mails = store.list_for_orders(["ORD-1", "ORD-2"])
    assert [m.subject for m in mails] == ["second", "first"]
    assert all(m.html_body == "" and m.text_body == "" for m in mails)
    assert mails[0].created_at.tzinfo is not None


def test_list_of_unknown_or_no_orders_is_empty(store):
    store.add("evt-1", "ORD-1", "a@example.com", "s", "h", "t")
    assert store.list_for_orders(["nope"]) == []
    assert store.list_for_orders([]) == []


def test_list_treats_order_ids_as_data_and_takes_many(store):
    store.add("evt-1", "ORD-1", "a@example.com", "mine", "h", "t")
    store.add("evt-2", "x' OR '1'='1", "b@example.com", "odd id", "h", "t")
    assert [m.subject for m in store.list_for_orders(["x' OR '1'='1"])] == ["odd id"]
    assert store.list_for_orders(["' OR 1=1 --"]) == []
    # more ids than SQLite's limit of bound parameters (999 in older builds)
    many = [f"ORD-{i}" for i in range(5000)]
    assert [m.subject for m in store.list_for_orders(many)] == ["mine"]


def test_get_returns_bodies_only_for_the_owning_order(store):
    mail_id = store.add("evt-1", "ORD-1", "a@example.com", "s", "<p>hi</p>", "hi")
    mail = store.get(mail_id, "ORD-1")
    assert mail.html_body == "<p>hi</p>" and mail.text_body == "hi" and mail.to_addr == "a@example.com"
    assert store.get(mail_id, "ORD-2") is None
    assert store.get("unknown", "ORD-1") is None


def test_file_store_uses_wal_and_survives_reopen(tmp_path):
    path = str(tmp_path / "mailbox.db")
    store = MailboxStore(path)
    store.add("evt-1", "ORD-1", "a@example.com", "s", "h", "t")
    assert sqlite3.connect(path).execute("PRAGMA journal_mode").fetchone()[0] == "wal"
    store.close()
    reopened = MailboxStore(path)
    assert reopened.count() == 1
    assert reopened.add("evt-1", "ORD-1", "a@example.com", "s", "h", "t") is None  # replay after a restart
    reopened.close()


def test_table_has_the_contract_columns(store):
    cols = [r[1] for r in store._db.execute("PRAGMA table_info(emails)")]
    assert cols == ["id", "event_id", "order_id", "to_addr", "subject", "html_body", "text_body", "created_at"]


def test_concurrent_writers_replaying_one_event_store_one_row(tmp_path):
    store = MailboxStore(str(tmp_path / "m.db"))
    results = []

    def worker():
        results.append(store.add("evt-1", "ORD-1", "a@example.com", "s", "h", "t"))

    threads = [threading.Thread(target=worker) for _ in range(8)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert sum(r is not None for r in results) == 1 and store.count() == 1
    store.close()


# ---------------------------------------------------------------------------
# versioned, forward-only migrations (CODE-08) and the injected clock (CODE-07)
# ---------------------------------------------------------------------------

LEGACY_SCHEMA = """
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


def _user_version(path):
    con = sqlite3.connect(path)
    try:
        return con.execute("PRAGMA user_version").fetchone()[0]
    finally:
        con.close()


def test_a_new_database_is_migrated_to_the_latest_version(tmp_path):
    path = str(tmp_path / "new.db")
    s = MailboxStore(path)
    assert s.schema_version == MIGRATIONS[-1][0]
    s.close()
    assert _user_version(path) == MIGRATIONS[-1][0]


def test_migration_numbers_are_consecutive_from_one():
    assert [v for v, _ in MIGRATIONS] == list(range(1, len(MIGRATIONS) + 1))


def test_a_database_from_before_versioning_is_adopted_and_keeps_its_mails(tmp_path):
    path = str(tmp_path / "legacy.db")
    con = sqlite3.connect(path)
    con.executescript(LEGACY_SCHEMA)
    con.execute(
        "INSERT INTO emails VALUES ('m1', 'e1', 'ORD-1', 'a@example.com', 's', 'h', 't', '2026-01-01T00:00:00+00:00')"
    )
    con.commit()
    assert con.execute("PRAGMA user_version").fetchone()[0] == 0
    con.close()

    s = MailboxStore(path)
    assert s.schema_version == MIGRATIONS[-1][0]
    assert s.count() == 1
    assert [m.id for m in s.list_for_orders(["ORD-1"])] == ["m1"]
    s.close()


def test_opening_twice_changes_nothing(tmp_path):
    path = str(tmp_path / "twice.db")
    first = MailboxStore(path)
    first.add("evt-1", "ORD-1", "a@example.com", "s", "h", "t")
    first.close()
    second = MailboxStore(path)
    assert second.schema_version == MIGRATIONS[-1][0] and second.count() == 1
    second.close()


def test_a_newer_database_is_refused(tmp_path):
    path = str(tmp_path / "newer.db")
    con = sqlite3.connect(path)
    con.execute(f"PRAGMA user_version = {MIGRATIONS[-1][0] + 1}")
    con.close()
    with pytest.raises(SchemaError):
        MailboxStore(path)
    assert isinstance(SchemaError("x"), sqlite3.Error)  # server.open_store treats it like an unusable database


def test_later_migrations_run_in_order_and_only_once(tmp_path):
    path = str(tmp_path / "steps.db")
    two = (
        *MIGRATIONS,
        (2, ("ALTER TABLE emails ADD COLUMN read_at TEXT", "CREATE INDEX emails_read_at ON emails (read_at)")),
    )
    s = MailboxStore(path, migrations=MIGRATIONS)
    s.add("evt-1", "ORD-1", "a@example.com", "s", "h", "t")
    s.close()
    s = MailboxStore(path, migrations=two)  # an upgrade of an existing database
    assert s.schema_version == 2 and s.count() == 1
    s.close()
    s = MailboxStore(path, migrations=two)  # and again: the ALTER TABLE must not run twice
    assert s.schema_version == 2
    s.close()


def test_a_failing_migration_is_rolled_back_completely(tmp_path):
    path = str(tmp_path / "broken.db")
    MailboxStore(path).close()
    broken = (
        *MIGRATIONS,
        (2, ("ALTER TABLE emails ADD COLUMN ok TEXT", "THIS IS NOT SQL")),
    )
    with pytest.raises(sqlite3.Error):
        MailboxStore(path, migrations=broken)
    assert _user_version(path) == 1  # still the old version
    con = sqlite3.connect(path)
    columns = [r[1] for r in con.execute("PRAGMA table_info(emails)")]
    con.close()
    assert "ok" not in columns  # the first statement of the failed migration was undone
    MailboxStore(path).close()  # and the database is still usable by the released code


def test_a_second_store_on_a_migrated_database_skips_the_migration(tmp_path):
    path = str(tmp_path / "race.db")
    a = MailboxStore(path)
    b = MailboxStore(path)  # opened while the first is still open: sees the version and skips
    assert a.schema_version == b.schema_version == MIGRATIONS[-1][0]
    a.close()
    b.close()


def test_created_at_comes_from_the_injected_clock_and_orders_the_list(tmp_path):
    from datetime import datetime, timedelta, timezone

    now = [datetime(2026, 11, 14, 12, 0, 0, tzinfo=timezone.utc)]
    s = MailboxStore(":memory:", clock=lambda: now[0])
    s.add("evt-1", "ORD-1", "a@example.com", "older", "h", "t")
    now[0] -= timedelta(minutes=30)  # the wall clock jumped back: the list still follows the stamped time
    s.add("evt-2", "ORD-1", "a@example.com", "stamped earlier", "h", "t")
    mails = s.list_for_orders(["ORD-1"])
    assert [m.subject for m in mails] == ["older", "stamped earlier"]
    assert mails[0].created_at == datetime(2026, 11, 14, 12, 0, 0, tzinfo=timezone.utc)
    s.close()


def test_the_default_clock_is_timezone_aware_utc():
    assert mailbox_store._now().tzinfo is not None
