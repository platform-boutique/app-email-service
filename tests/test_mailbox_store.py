"""The SQLite mailbox: idempotency, ordering, ownership, file mode."""

import sqlite3
import threading

from mailbox_store import MailboxStore


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
