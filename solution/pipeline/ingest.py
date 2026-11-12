"""Ingestion for Assignment A: leads + events, idempotent and incremental.

Order of work for one drop: lead files (filename order) first, then
event files (filename order), with orphan resolution after every file
and once more at the end. Final state is independent of whether an
event or its lead arrived first, because unresolved events wait in
``orphan_events`` and are moved into the ledger when their lead (or
their call) shows up.

Idempotency: a file is identified by the SHA-256 of its bytes. Its
hash is recorded in the same transaction as its effects, so a
re-ingest of the same drop is skipped wholesale and changes nothing
-- not even the counters. A crash mid-file rolls the file back, so
re-running it is safe.
"""
from __future__ import annotations

import csv
import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from . import db as dbmod
from .normalize import (
    ist_iso,
    normalize_header,
    normalize_phone,
    parse_date,
    parse_decimal,
    parse_enum,
    parse_timestamp,
)
from .pii import PiiProtector, phone_hash, phone_last4


def _file_hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class Schemas:
    """Workflow schemas + disposition catalogue, loaded from the repo."""

    def __init__(self, schemas_dir: str | Path):
        self.dir = Path(schemas_dir)
        self.workflows: dict[str, dict] = {}
        for path in sorted(self.dir.glob("*.json")):
            data = json.loads(path.read_text(encoding="utf-8"))
            self.workflows[data["workflow_id"]] = data
        self.dispositions: dict[tuple[str, str], tuple[bool, bool]] = {}
        disp_path = self.dir / "dispositions.csv"
        if disp_path.exists():
            with disp_path.open(newline="", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    self.dispositions[(row["workflow_id"], row["code"])] = (
                        row["is_terminal"].strip().lower() == "true",
                        row["is_success"].strip().lower() == "true",
                    )

    def field_map(self, workflow_id: str) -> dict[str, dict]:
        """Normalised header/alias -> field definition."""
        out: dict[str, dict] = {}
        for field in self.workflows[workflow_id]["fields"]:
            out[normalize_header(field["name"])] = field
            for alias in field.get("aliases", []):
                out.setdefault(normalize_header(alias), field)
        return out

    def reference_field(self, workflow_id: str) -> str | None:
        for field in self.workflows[workflow_id]["fields"]:
            if field.get("reference"):
                return field["name"]
        return None

    def apply_defaults(self, workflow_id: str, fields: dict) -> dict:
        out = dict(fields)
        for field in self.workflows[workflow_id]["fields"]:
            if out.get(field["name"]) is None and "default" in field:
                out[field["name"]] = field["default"]
        return out


def find_schemas_dir(start: Path | None = None) -> Path:
    """Locate assignment-a-data/schemas from the cwd or the repo layout."""
    candidates = []
    if start is not None:
        candidates.append(Path(start))
    candidates.append(Path.cwd())
    # solution/pipeline/ingest.py -> repo root is ../../
    candidates.append(Path(__file__).resolve().parents[2])
    for base in candidates:
        for cand in (base / "assignment-a-data" / "schemas",
                     base / "schemas"):
            if (cand / "dispositions.csv").exists():
                return cand
    raise FileNotFoundError(
        "could not find assignment-a-data/schemas; pass --schemas explicitly"
    )


class Ingestor:
    def __init__(self, conn: sqlite3.Connection, schemas: Schemas,
                 pii: PiiProtector):
        self.conn = conn
        self.schemas = schemas
        self.pii = pii
        self._leads: dict[tuple[str, str], dict] | None = None
        self._call_map: dict[tuple[str, str], str] | None = None
        self._seen_events: set[str] | None = None
        self._load_dispositions()

    # -- lazy in-memory maps (kept in sync as we ingest) -----------------
    def _load_dispositions(self) -> None:
        rows = [
            (wf, code, int(term), int(succ))
            for (wf, code), (term, succ) in self.schemas.dispositions.items()
        ]
        self.conn.executemany(
            "INSERT OR REPLACE INTO dispositions VALUES (?,?,?,?)", rows)
        self.conn.commit()

    @property
    def leads(self) -> dict[tuple[str, str], dict]:
        if self._leads is None:
            self._leads = {}
            for r in self.conn.execute(
                    "SELECT workflow_id, phone_hash, phone_last4,"
                    " customer_name_enc, external_ref, batch_id, fields_json"
                    " FROM leads"):
                self._leads[(r["workflow_id"], r["phone_hash"])] = {
                    "phone_last4": r["phone_last4"],
                    "customer_name_enc": r["customer_name_enc"],
                    "external_ref": r["external_ref"],
                    "batch_id": r["batch_id"],
                    "fields": json.loads(r["fields_json"]),
                }
        return self._leads

    @property
    def call_map(self) -> dict[tuple[str, str], str]:
        if self._call_map is None:
            self._call_map = {}
            for r in self.conn.execute(
                    "SELECT workflow_id, call_id, phone_hash FROM events"
                    " WHERE call_id IS NOT NULL"):
                self._call_map[(r["workflow_id"], r["call_id"])] = r["phone_hash"]
        return self._call_map

    @property
    def seen_events(self) -> set[str]:
        if self._seen_events is None:
            self._seen_events = set()
            for (eid,) in self.conn.execute("SELECT event_id FROM events"):
                self._seen_events.add(eid)
            for (eid,) in self.conn.execute(
                    "SELECT event_id FROM orphan_events"):
                self._seen_events.add(eid)
        return self._seen_events

    # -- leads ------------------------------------------------------------
    def ingest_lead_file(self, path: Path, workflow_id: str) -> dict:
        """Ingest one lead CSV. Returns stats (zeros + skipped flag)."""
        fhash = _file_hash(path)
        if self.conn.execute(
                "SELECT 1 FROM ingested_files WHERE file_hash=?",
                (fhash,)).fetchone():
            return {"skipped": True}
        if workflow_id not in self.schemas.workflows:
            raise ValueError(f"no schema for workflow {workflow_id!r}")

        schema = self.schemas.workflows[workflow_id]
        fmap = self.schemas.field_map(workflow_id)
        ref_field = self.schemas.reference_field(workflow_id)
        stats = {"lead_rows_total": 0, "lead_rows_rejected": 0,
                 "duplicate_lead_rows_merged": 0, "field_warnings": 0}
        rejections: list[tuple] = []
        dirty: dict[tuple[str, str], dict] = {}

        with path.open(newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames is None:
                # Empty file: record it as ingested with zero stats.
                with self.conn:
                    self._record_file(fhash, str(path), "leads", stats)
                return stats
            # Column index -> field definition (first mapping wins).
            col_field: dict[int, dict] = {}
            for i, header in enumerate(reader.fieldnames):
                if header is None:
                    continue
                field = fmap.get(normalize_header(header))
                if field is not None and field["name"] not in [
                        v["name"] for v in col_field.values()]:
                    col_field[i] = field
            row_number = 0
            for raw_row in reader:
                # DictReader gives None for missing trailing cells and a
                # None key for surplus cells; rebuild the cell list.
                cells = [raw_row.get(h) for h in reader.fieldnames]
                if raw_row.get(None):
                    cells.extend(raw_row[None])
                if all(c is None or str(c).strip() == "" for c in cells):
                    continue  # blank row: skipped, not counted
                row_number += 1
                stats["lead_rows_total"] += 1
                values: dict[str, str | None] = {}
                for i, field in col_field.items():
                    if i >= len(cells):
                        continue
                    cell = cells[i]
                    if cell is None:
                        continue
                    cell = str(cell).strip()
                    if cell == "":
                        continue
                    # First non-empty value wins if two columns map alike.
                    values.setdefault(field["name"], cell)

                # Rejection: walk fields in schema order, first failure wins.
                reason = None
                canonical_phone = None
                for field in schema["fields"]:
                    name = field["name"]
                    present = name in values
                    if field.get("required") and not present:
                        reason = f"missing_required:{name}"
                        break
                    if field["type"] == "phone" and present:
                        canonical_phone = normalize_phone(values[name])
                        if canonical_phone is None:
                            reason = "invalid_phone"
                            break
                if reason is not None:
                    stats["lead_rows_rejected"] += 1
                    batch_id = path.stem
                    rejections.append(
                        (workflow_id, batch_id, row_number, reason))
                    continue

                # Parse optional fields; invalid optional -> null + warning.
                parsed: dict[str, object] = {}
                name_value = None
                for field in schema["fields"]:
                    name = field["name"]
                    if name not in values:
                        parsed[name] = None
                        continue
                    raw = values[name]
                    ftype = field["type"]
                    if ftype == "string":
                        parsed[name] = raw
                    elif ftype == "phone":
                        parsed[name] = canonical_phone
                    elif ftype == "date":
                        v = parse_date(raw)
                        if v is None:
                            stats["field_warnings"] += 1
                        parsed[name] = v
                    elif ftype == "decimal":
                        v = parse_decimal(raw)
                        if v is None:
                            stats["field_warnings"] += 1
                            parsed[name] = None
                        else:
                            parsed[name] = str(v)
                    elif ftype == "enum":
                        v = parse_enum(raw, field.get("values", []))
                        if v is None:
                            stats["field_warnings"] += 1
                        parsed[name] = v
                    else:
                        parsed[name] = raw
                    if field.get("pii") and name == "customer_name":
                        name_value = parsed[name]
                # Split PII out of the stored fields.
                fields_store = {k: v for k, v in parsed.items()
                                if k not in ("phone", "customer_name")}
                phash = phone_hash(canonical_phone)
                key = (workflow_id, phash)
                existing = self.leads.get(key)
                if existing is None:
                    lead = {
                        "phone_last4": phone_last4(canonical_phone),
                        "customer_name_enc": (
                            self.pii.encrypt_name(name_value)
                            if name_value else None),
                        "external_ref": parsed.get(ref_field) if ref_field else None,
                        "batch_id": path.stem,
                        "fields": fields_store,
                    }
                    self.leads[key] = lead
                    dirty[key] = lead
                else:
                    stats["duplicate_lead_rows_merged"] += 1
                    # Last write wins, only where the new value is non-null.
                    for k, v in fields_store.items():
                        if v is not None:
                            existing["fields"][k] = v
                    if name_value is not None:
                        existing["customer_name_enc"] = self.pii.encrypt_name(name_value)
                    if ref_field and parsed.get(ref_field) is not None:
                        existing["external_ref"] = parsed[ref_field]
                    # batch_id is permanent: never overwritten.
                    dirty[key] = existing

        # One transaction for the whole file: leads + rejections + stats
        # + file hash, so a crash mid-file leaves nothing behind.
        with self.conn:
            # executemany, not one execute per lead: per-row executes
            # were the slowest part of lead ingest on the large dataset.
            self.conn.executemany(
                "INSERT INTO leads"
                " (workflow_id, phone_hash, phone_last4,"
                "  customer_name_enc, external_ref, batch_id, fields_json)"
                " VALUES (?,?,?,?,?,?,?)"
                " ON CONFLICT(workflow_id, phone_hash) DO UPDATE SET"
                "  phone_last4=excluded.phone_last4,"
                "  customer_name_enc=excluded.customer_name_enc,"
                "  external_ref=excluded.external_ref,"
                "  batch_id=excluded.batch_id,"
                "  fields_json=excluded.fields_json",
                [(wf, phash, lead["phone_last4"],
                  lead["customer_name_enc"], lead["external_ref"],
                  lead["batch_id"], json.dumps(lead["fields"]))
                 for (wf, phash), lead in dirty.items()])
            self.conn.executemany(
                "INSERT OR IGNORE INTO rejections VALUES (?,?,?,?)", rejections)
            self._record_file(fhash, str(path), "leads", stats)
        self.resolve_orphans()
        return stats

    def _record_file(self, fhash: str, path: str, kind: str, stats: dict) -> None:
        self.conn.execute(
            "INSERT OR IGNORE INTO ingested_files VALUES (?,?,?,?)",
            (fhash, path, kind, _now_iso()))
        self.conn.execute(
            "INSERT OR IGNORE INTO file_stats"
            " (file_hash, lead_rows_total, lead_rows_rejected,"
            "  duplicate_lead_rows_merged, field_warnings, events_total,"
            "  malformed_events, duplicate_events_dropped)"
            " VALUES (?,?,?,?,?,?,?,?)",
            (fhash, stats.get("lead_rows_total", 0),
             stats.get("lead_rows_rejected", 0),
             stats.get("duplicate_lead_rows_merged", 0),
             stats.get("field_warnings", 0),
             stats.get("events_total", 0),
             stats.get("malformed_events", 0),
             stats.get("duplicate_events_dropped", 0)))

    # -- events -----------------------------------------------------------
    def _phone_hash_if_valid(self, raw_phone) -> str | None:
        """Hash of a normalisable phone, whether or not its lead exists.

        Orphans keep this hash so they can be re-linked when the lead
        arrives later; linkedness is decided by the caller.
        """
        if raw_phone is None:
            return None
        canon = normalize_phone(str(raw_phone))
        if canon is None:
            return None
        return phone_hash(canon)

    def ingest_event_file(self, path: Path) -> dict:
        fhash = _file_hash(path)
        if self.conn.execute(
                "SELECT 1 FROM ingested_files WHERE file_hash=?",
                (fhash,)).fetchone():
            return {"skipped": True}
        stats = {"events_total": 0, "malformed_events": 0,
                 "duplicate_events_dropped": 0}
        linked: list[tuple] = []
        orphans: list[tuple] = []

        with path.open(encoding="utf-8") as f:
            for line in f:
                if line.strip() == "":
                    continue  # empty lines are not counted at all
                stats["events_total"] += 1
                try:
                    ev = json.loads(line)
                except json.JSONDecodeError:
                    stats["malformed_events"] += 1
                    continue
                if not isinstance(ev, dict) or not ev.get("event_id"):
                    stats["malformed_events"] += 1
                    continue
                event_id = str(ev["event_id"])
                if event_id in self.seen_events:
                    stats["duplicate_events_dropped"] += 1
                    continue
                self.seen_events.add(event_id)
                workflow_id = ev.get("workflow_id") or ""
                event_type = ev.get("event_type") or ""
                call_id = ev.get("call_id")
                call_id = str(call_id) if call_id else None
                dt = parse_timestamp(ev.get("occurred_at"))
                occurred = dt.isoformat(timespec="seconds") if dt else None
                occurred_ist = ist_iso(dt)
                payload = ev.get("payload") or {}
                if not isinstance(payload, dict):
                    payload = {}
                payload_json = json.dumps(payload, ensure_ascii=False)

                phash = self._phone_hash_if_valid(ev.get("phone"))
                linked_now = False
                if event_type == "disposition.override" and call_id:
                    via_call = self.call_map.get((workflow_id, call_id))
                    if via_call is not None:
                        phash = via_call
                        linked_now = True
                if not linked_now and phash is not None and \
                        (workflow_id, phash) in self.leads:
                    linked_now = True
                row = (event_id, workflow_id, call_id, event_type,
                       occurred, occurred_ist, phash, payload_json)
                if linked_now:
                    linked.append(row)
                    if call_id:
                        self.call_map[(workflow_id, call_id)] = phash
                else:
                    orphans.append(row)

        with self.conn:
            self.conn.executemany(
                "INSERT OR IGNORE INTO events VALUES (?,?,?,?,?,?,?,?)", linked)
            self.conn.executemany(
                "INSERT OR IGNORE INTO orphan_events VALUES (?,?,?,?,?,?,?,?)",
                orphans)
            self._record_file(fhash, str(path), "events", stats)
        self.resolve_orphans()
        return stats

    def resolve_orphans(self) -> int:
        """Move newly-linkable orphans into the ledger. Returns # moved.

        Linkable orphans are found with SQL joins (against ``leads``
        for phone-linked events, against ``events`` for overrides via
        their call_id), never by scanning the whole orphan table in
        Python -- on the large dataset that scan ran after every one
        of ~130 event files and dominated the run time. Iterating
        matters because moving an initiated event can enable a
        phone-less override for the same call.
        """
        moved_total = 0
        for _ in range(20):
            rows = list(self.conn.execute(
                "SELECT o.* FROM orphan_events o"
                " JOIN leads l ON l.workflow_id = o.workflow_id"
                "   AND l.phone_hash = o.phone_hash"
                " UNION"
                " SELECT o.* FROM orphan_events o"
                " JOIN events e ON e.workflow_id = o.workflow_id"
                "   AND e.call_id = o.call_id"
                " WHERE o.event_type = 'disposition.override'"
                "   AND o.call_id IS NOT NULL"))
            if not rows:
                break
            to_move: list[tuple[sqlite3.Row, str]] = []
            for r in rows:
                phash = None
                if r["event_type"] == "disposition.override" and r["call_id"]:
                    phash = self.call_map.get((r["workflow_id"], r["call_id"]))
                if phash is None:
                    phash = r["phone_hash"]
                if phash is not None:
                    to_move.append((r, phash))
            if not to_move:
                break
            with self.conn:
                self.conn.executemany(
                    "INSERT OR IGNORE INTO events VALUES (?,?,?,?,?,?,?,?)",
                    [(r["event_id"], r["workflow_id"], r["call_id"],
                      r["event_type"], r["occurred_at"], r["occurred_at_ist"],
                      phash, r["payload_json"]) for r, phash in to_move])
                self.conn.executemany(
                    "DELETE FROM orphan_events WHERE event_id=?",
                    [(r["event_id"],) for r, _ in to_move])
            for r, phash in to_move:
                if r["call_id"]:
                    self.call_map[(r["workflow_id"], r["call_id"])] = phash
            moved_total += len(to_move)
        return moved_total

    # -- drops ------------------------------------------------------------
    def ingest_drop(self, drop: Path) -> dict:
        drop = Path(drop)
        summary: dict[str, object] = {"drop": str(drop), "files": []}
        lead_files: list[tuple[str, Path]] = []
        for p in sorted(drop.rglob("leads/**/*.csv")):
            # .../leads/<workflow_id>/batch_001.csv
            workflow_id = p.parent.name
            lead_files.append((workflow_id, p))
        lead_files.sort(key=lambda t: (t[0], t[1].name))
        for workflow_id, p in lead_files:
            summary["files"].append(
                (str(p), self.ingest_lead_file(p, workflow_id)))
        event_files = sorted(drop.rglob("events/**/*.jsonl"),
                             key=lambda p: p.name)
        for p in event_files:
            summary["files"].append(
                (str(p), self.ingest_event_file(p)))
        self.resolve_orphans()
        return summary
