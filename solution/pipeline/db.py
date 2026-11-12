"""SQLite schema for Assignment A.

Design
------
* ``leads`` -- one row per (workflow_id, phone_hash). The phone is
  never stored in plaintext (see pii.py); ``phone_last4`` is kept so
  reports can mask it. ``customer_name_enc`` is Fernet-encrypted.
  All other lead fields live in ``fields_json`` (normalised values
  only, no PII except the encrypted name which is stored separately);
  ``external_ref`` is denormalised for the lead_report sort.
  ``batch_id`` is the file stem of the first valid row for the lead
  and is never overwritten by a later duplicate.
* ``events`` -- the append-only interaction ledger. One row per
  accepted (linked) event, keyed by ``event_id``. Nothing here is
  ever updated or deleted: an override is a new row, and the
  effective disposition is computed at report time. Unknown-
  disposition events are kept here too (flagged by their code not
  being in ``dispositions``); reports ignore them by joining to
  ``dispositions``.
* ``orphan_events`` -- events that could not yet be linked to a lead.
  Kept separately so they stay out of every report, and so a later
  lead ingest can move them into ``events`` (re-linking). The final
  orphan count is simply COUNT(*) here.
* ``ingested_files`` / ``file_stats`` -- idempotency. A file is
  identified by the SHA-256 of its bytes; a re-ingest of the same
  file is skipped entirely in the same transaction that recorded it,
  so counters never move on a re-run. Per-file stats are summed at
  report time for every counter except orphans (final state) and
  unknown dispositions (recomputed from ``events``, so an orphan
  that later links and turns out unknown is counted correctly).
* ``dispositions`` -- the catalogue from dispositions.csv.
* ``rejections`` -- one row per rejected lead row (no PII: only
  workflow, batch, row number and reason).
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS leads (
    workflow_id     TEXT NOT NULL,
    phone_hash      TEXT NOT NULL,
    phone_last4     TEXT NOT NULL,
    customer_name_enc TEXT,
    external_ref    TEXT,
    batch_id        TEXT NOT NULL,
    fields_json     TEXT NOT NULL DEFAULT '{}',
    PRIMARY KEY (workflow_id, phone_hash)
);
CREATE TABLE IF NOT EXISTS events (
    event_id        TEXT PRIMARY KEY,
    workflow_id     TEXT NOT NULL,
    call_id         TEXT,
    event_type      TEXT NOT NULL,
    occurred_at     TEXT,              -- ISO-8601, UTC
    occurred_at_ist TEXT,              -- ISO-8601, IST (+05:30)
    phone_hash      TEXT,
    payload_json    TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_events_call
    ON events (workflow_id, call_id, event_type, occurred_at);
CREATE INDEX IF NOT EXISTS idx_events_phone
    ON events (workflow_id, phone_hash);
CREATE TABLE IF NOT EXISTS orphan_events (
    event_id        TEXT PRIMARY KEY,
    workflow_id     TEXT NOT NULL,
    call_id         TEXT,
    event_type      TEXT NOT NULL,
    occurred_at     TEXT,
    occurred_at_ist TEXT,
    phone_hash      TEXT,
    payload_json    TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_orphan_call ON orphan_events (call_id);
CREATE INDEX IF NOT EXISTS idx_orphan_phone
    ON orphan_events (workflow_id, phone_hash);
CREATE TABLE IF NOT EXISTS rejections (
    workflow_id TEXT NOT NULL,
    batch_id    TEXT NOT NULL,
    row_number  INTEGER NOT NULL,
    reason      TEXT NOT NULL,
    PRIMARY KEY (workflow_id, batch_id, row_number)
);
CREATE TABLE IF NOT EXISTS dispositions (
    workflow_id TEXT NOT NULL,
    code        TEXT NOT NULL,
    is_terminal INTEGER NOT NULL,
    is_success  INTEGER NOT NULL,
    PRIMARY KEY (workflow_id, code)
);
CREATE TABLE IF NOT EXISTS ingested_files (
    file_hash TEXT PRIMARY KEY,
    path      TEXT NOT NULL,
    kind      TEXT NOT NULL,
    ingested_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS file_stats (
    file_hash               TEXT PRIMARY KEY,
    lead_rows_total         INTEGER NOT NULL DEFAULT 0,
    lead_rows_rejected      INTEGER NOT NULL DEFAULT 0,
    duplicate_lead_rows_merged INTEGER NOT NULL DEFAULT 0,
    field_warnings          INTEGER NOT NULL DEFAULT 0,
    events_total            INTEGER NOT NULL DEFAULT 0,
    malformed_events        INTEGER NOT NULL DEFAULT 0,
    duplicate_events_dropped INTEGER NOT NULL DEFAULT 0
);
"""


def connect(db_path: str | Path) -> sqlite3.Connection:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    # Bulk-load setting: with WAL, synchronous=OFF still guarantees
    # transaction atomicity on a process crash (a torn commit is
    # rolled back on recovery); only an OS crash / power loss could
    # lose a committed transaction -- and a lost file-hash record
    # just means that file is re-ingested, which is safe by design.
    # On this VM it cuts ingest wall time several-fold (fsync wait
    # dominated: 98s sys vs 41s user on the large run with NORMAL).
    conn.execute("PRAGMA synchronous=OFF")
    conn.execute("PRAGMA cache_size=-64000")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(SCHEMA)
    conn.commit()
    return conn
