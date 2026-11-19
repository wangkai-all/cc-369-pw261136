"""SQLite-backed store.

Schema intentionally keeps BOTH the legacy text column and a typed shadow
value per field (dual columns). The raw text is never overwritten, so a rule
change can re-derive the typed value without replaying the source.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager
from typing import Optional

SCHEMA = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS batches (
    batch_id      TEXT PRIMARY KEY,
    received_at   TEXT NOT NULL,
    status        TEXT NOT NULL,          -- staged | projected | quarantined
    rule_version  INTEGER NOT NULL,
    source        TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS records (
    batch_id      TEXT NOT NULL REFERENCES batches(batch_id),
    row_index     INTEGER NOT NULL,
    raw_json      TEXT NOT NULL,          -- the untouched incoming row
    PRIMARY KEY (batch_id, row_index)
);

-- Dual columns: raw text kept beside typed shadow per field.
CREATE TABLE IF NOT EXISTS record_values (
    batch_id       TEXT NOT NULL,
    row_index      INTEGER NOT NULL,
    field          TEXT NOT NULL,
    raw_text       TEXT,
    typed_value    TEXT,                  -- NULL when unparseable
    typed_kind     TEXT,                  -- date | money | enum | NULL
    parse_state    TEXT NOT NULL,         -- ok | failed | pending
    rule_version   INTEGER,
    PRIMARY KEY (batch_id, row_index, field)
);

CREATE TABLE IF NOT EXISTS failures (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id       TEXT NOT NULL,
    row_index      INTEGER NOT NULL,
    field          TEXT NOT NULL,
    raw            TEXT,
    reason         TEXT NOT NULL,
    rule_version   INTEGER NOT NULL,
    resolved       INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS rule_versions (
    version        INTEGER PRIMARY KEY AUTOINCREMENT,
    label          TEXT NOT NULL,
    config_json    TEXT NOT NULL,
    created_at     TEXT NOT NULL,
    is_active      INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS query_versions (
    name           TEXT NOT NULL,        -- e.g. typed_v1
    rule_version   INTEGER NOT NULL,
    activated_at   TEXT NOT NULL,
    PRIMARY KEY (name)
);

CREATE TABLE IF NOT EXISTS audit (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    at             TEXT NOT NULL,
    actor          TEXT NOT NULL,
    action         TEXT NOT NULL,
    payload        TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_failures_batch ON failures(batch_id, resolved);
CREATE INDEX IF NOT EXISTS idx_values_field ON record_values(field, parse_state);
"""


class Store:
    def __init__(self, path: str = ":memory:"):
        self.path = path
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(SCHEMA)
        self._conn.commit()

    @contextmanager
    def tx(self):
        with self._lock:
            try:
                yield self._conn
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise

    def close(self):
        self._conn.close()

    # -- rules -------------------------------------------------------------

    def current_rule(self) -> sqlite3.Row:
        row = self._conn.execute(
            "SELECT * FROM rule_versions WHERE is_active=1 ORDER BY version DESC LIMIT 1"
        ).fetchone()
        if row is None:
            raise RuntimeError("no active rule version")
        return row

    def publish_rule(self, config: dict, label: str, now: str) -> int:
        with self.tx() as conn:
            conn.execute("UPDATE rule_versions SET is_active=0")
            cur = conn.execute(
                "INSERT INTO rule_versions(label, config_json, created_at, is_active) VALUES (?,?,?,1)",
                (label, json.dumps(config, sort_keys=True), now),
            )
            return cur.lastrowid

    def activate_rule(self, version: int) -> None:
        with self.tx() as conn:
            exists = conn.execute("SELECT 1 FROM rule_versions WHERE version=?", (version,)).fetchone()
            if exists is None:
                raise KeyError(f"rule version {version} does not exist")
            conn.execute("UPDATE rule_versions SET is_active=0")
            conn.execute("UPDATE rule_versions SET is_active=1 WHERE version=?", (version,))

    def get_rule(self, version: int) -> dict:
        row = self._conn.execute("SELECT * FROM rule_versions WHERE version=?", (version,)).fetchone()
        if row is None:
            raise KeyError(f"rule version {version} does not exist")
        return json.loads(row["config_json"])

    # -- batches -----------------------------------------------------------

    def stage_batch(self, batch_id: str, rows: list[dict], source: str, rule_version: int, now: str) -> None:
        with self.tx() as conn:
            conn.execute(
                "INSERT INTO batches(batch_id, received_at, status, rule_version, source) VALUES (?,?,?,?,?)",
                (batch_id, now, "staged", rule_version, source),
            )
            conn.executemany(
                "INSERT INTO records(batch_id, row_index, raw_json) VALUES (?,?,?)",
                [(batch_id, i, json.dumps(r, ensure_ascii=False)) for i, r in enumerate(rows)],
            )

    def batches(self) -> list[sqlite3.Row]:
        return self._conn.execute("SELECT * FROM batches ORDER BY received_at, batch_id").fetchall()

    def batch_rows(self, batch_id: str) -> list[dict]:
        rows = self._conn.execute(
            "SELECT raw_json FROM records WHERE batch_id=? ORDER BY row_index", (batch_id,)
        ).fetchall()
        return [json.loads(r["raw_json"]) for r in rows]

    def mark_batch(self, batch_id: str, status: str, rule_version: Optional[int] = None) -> None:
        with self.tx() as conn:
            if rule_version is None:
                conn.execute("UPDATE batches SET status=? WHERE batch_id=?", (status, batch_id))
            else:
                conn.execute(
                    "UPDATE batches SET status=?, rule_version=? WHERE batch_id=?",
                    (status, rule_version, batch_id),
                )

    # -- values / failures -------------------------------------------------

    def put_value(self, batch_id, row_index, field, raw_text, typed_value, typed_kind, parse_state, rule_version):
        with self.tx() as conn:
            conn.execute(
                """INSERT INTO record_values
                   (batch_id,row_index,field,raw_text,typed_value,typed_kind,parse_state,rule_version)
                   VALUES (?,?,?,?,?,?,?,?)
                   ON CONFLICT(batch_id,row_index,field) DO UPDATE SET
                     raw_text=excluded.raw_text,
                     typed_value=excluded.typed_value,
                     typed_kind=excluded.typed_kind,
                     parse_state=excluded.parse_state,
                     rule_version=excluded.rule_version""",
                (batch_id, row_index, field, raw_text, typed_value, typed_kind, parse_state, rule_version),
            )

    def add_failure(self, batch_id, row_index, field, raw, reason, rule_version) -> None:
        with self.tx() as conn:
            conn.execute(
                "INSERT INTO failures(batch_id,row_index,field,raw,reason,rule_version) VALUES (?,?,?,?,?,?)",
                (batch_id, row_index, field, raw, reason, rule_version),
            )

    def clear_failures(self, batch_id: str) -> None:
        with self.tx() as conn:
            conn.execute("DELETE FROM failures WHERE batch_id=?", (batch_id,))

    def failures(self, batch_id: Optional[str] = None, unresolved_only: bool = False) -> list[sqlite3.Row]:
        sql = "SELECT * FROM failures"
        where, args = [], []
        if batch_id:
            where.append("batch_id=?")
            args.append(batch_id)
        if unresolved_only:
            where.append("resolved=0")
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY id"
        return self._conn.execute(sql, args).fetchall()

    def values_for_batch(self, batch_id: str) -> list[sqlite3.Row]:
        return self._conn.execute(
            "SELECT * FROM record_values WHERE batch_id=? ORDER BY row_index, field", (batch_id,)
        ).fetchall()

    # -- query version -----------------------------------------------------

    def activate_query_version(self, name: str, rule_version: int, now: str) -> None:
        with self.tx() as conn:
            conn.execute(
                """INSERT INTO query_versions(name, rule_version, activated_at) VALUES (?,?,?)
                   ON CONFLICT(name) DO UPDATE SET rule_version=excluded.rule_version,
                                                   activated_at=excluded.activated_at""",
                (name, rule_version, now),
            )

    def query_version(self) -> Optional[sqlite3.Row]:
        return self._conn.execute("SELECT * FROM query_versions WHERE name='typed'").fetchone()

    def log(self, now: str, actor: str, action: str, payload: dict) -> None:
        with self.tx() as conn:
            conn.execute(
                "INSERT INTO audit(at,actor,action,payload) VALUES (?,?,?,?)",
                (now, actor, action, json.dumps(payload, sort_keys=True)),
            )
