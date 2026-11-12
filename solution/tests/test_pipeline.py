"""End-to-end tests on tiny hand-built drops.

Covers: rejection reasons in schema order, duplicate merging
(last-write-wins, batch_id permanent), orphan events that link
when the lead arrives later, overrides (including phone-less
overrides via call_id, and override beating a later set),
unknown dispositions, malformed/duplicate events, idempotent
re-ingest, and PII not appearing in plaintext in the DB file.
"""
import csv
import json
from pathlib import Path

import pytest

from solution.pipeline import db as dbmod
from solution.pipeline.ingest import Ingestor, Schemas
from solution.pipeline.pii import PiiProtector
from solution.pipeline.reports import generate_reports

REPO = Path(__file__).resolve().parents[2]
SCHEMAS = REPO / "assignment-a-data" / "schemas"


@pytest.fixture()
def ing(tmp_path):
    db_path = tmp_path / "test.db"
    conn = dbmod.connect(db_path)
    schemas = Schemas(SCHEMAS)
    yield Ingestor(conn, schemas, PiiProtector(db_path)), tmp_path, conn
    conn.close()


def _write_drop(base: Path, name: str, leads=None, events=None):
    drop = base / name
    (drop / "leads" / "motor_renewal").mkdir(parents=True, exist_ok=True)
    (drop / "events").mkdir(parents=True, exist_ok=True)
    if leads is not None:
        with (drop / "leads" / "motor_renewal" / "batch_001.csv").open(
                "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["Customer Name", "Mobile No", "Policy No",
                        "Expiry Date", "Premium", "Language"])
            w.writerows(leads)
    if events is not None:
        with (drop / "events" / "events_0001.jsonl").open("w") as f:
            for line in events:
                f.write(line if isinstance(line, str)
                        else json.dumps(line))
                f.write("\n")
    return drop


LEAD = ["Asha Verma", "+91 98765 43210", "MOT/2026/000001",
        "01-Dec-2026", "₹10,000", "hi"]
LEAD_DUP = ["Asha Verma", "9876543210", "MOT/2026/000001",
            "", "Rs. 12000", ""]  # premium overwrites, language stays hi


def _event(eid, etype, call, ts, phone="+919876543210", payload=None,
           workflow="motor_renewal"):
    ev = {"event_id": eid, "event_type": etype, "workflow_id": workflow,
          "call_id": call, "occurred_at": ts, "payload": payload or {}}
    if phone is not None:
        ev["phone"] = phone
    return ev


def test_lead_rejection_schema_order_and_duplicate_merge(ing):
    ingestor, tmp, conn = ing
    drop = _write_drop(tmp, "drop_1", leads=[
        ["", "5876543210", "MOT/2026/000009", "", "", ""],  # name missing first
        ["Ravi", "5876543210", "MOT/2026/000010", "", "", ""],  # bad phone
        ["Ravi", "+919876543210", "", "", "", ""],  # missing ref
        LEAD,
        LEAD_DUP,
        ["", "", "", "", "", ""],  # blank row: not counted
    ])
    ingestor.ingest_drop(drop)
    rows = conn.execute(
        "SELECT batch_id, row_number, reason FROM rejections"
        " ORDER BY row_number").fetchall()
    assert [tuple(r) for r in rows] == [
        ("batch_001", 1, "missing_required:customer_name"),
        ("batch_001", 2, "invalid_phone"),
        ("batch_001", 3, "missing_required:policy_number"),
    ]
    leads = conn.execute("SELECT * FROM leads").fetchall()
    assert len(leads) == 1
    lead = leads[0]
    assert lead["batch_id"] == "batch_001"
    assert lead["external_ref"] == "MOT/2026/000001"
    fields = json.loads(lead["fields_json"])
    # Duplicate overwrote premium (12000) but not language (null -> hi kept)
    assert fields["premium_amount"] == "12000.00"
    assert fields["preferred_language"] == "hi"
    stats = conn.execute(
        "SELECT lead_rows_total, lead_rows_rejected,"
        " duplicate_lead_rows_merged FROM file_stats").fetchone()
    assert tuple(stats) == (5, 3, 1)  # blank row not counted


def test_orphan_links_when_lead_arrives_later_and_override_wins(ing):
    ingestor, tmp, conn = ing
    # Drop 1: events only, no lead yet -> orphans.
    drop1 = _write_drop(tmp, "drop_1", events=[
        _event("evt_1", "call.initiated", "call_1", "2026-10-02T10:00:00Z"),
        _event("evt_2", "call.connected", "call_1", "2026-10-02T10:01:00Z"),
        _event("evt_3", "call.ended", "call_1", "2026-10-02T10:05:00Z",
               payload={"duration_sec": 240, "end_reason": "completed"}),
        _event("evt_4", "disposition.set", "call_1", "2026-10-02T10:06:00Z",
               payload={"disposition_code": "renewed"}),
        # Phone-less override, later than the set, flips it to terminal
        # non-success. A second set AFTER the override must not win.
        _event("evt_5", "disposition.override", "call_1",
               "2026-10-03T10:00:00Z", phone=None,
               payload={"disposition_code": "not_interested",
                        "overridden_by": "supervisor_1", "reason": "qa"}),
        _event("evt_6", "disposition.set", "call_1", "2026-10-04T10:00:00Z",
               payload={"disposition_code": "renewed"}),
        # Unknown disposition: counted, ignored.
        _event("evt_7", "disposition.set", "call_1", "2026-10-04T11:00:00Z",
               payload={"disposition_code": "RENEWD"}),
        "this is not json",
        _event("evt_1", "call.initiated", "call_1", "2026-10-02T10:00:00Z"),
    ])
    ingestor.ingest_drop(drop1)
    assert conn.execute("SELECT COUNT(*) FROM orphan_events").fetchone()[0] == 7
    assert conn.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 0

    # Drop 2: the lead arrives -> all 7 orphans link.
    drop2 = _write_drop(tmp, "drop_2", leads=[LEAD])
    # _write_drop used batch_001 again; that is fine, it is a new file.
    ingestor.ingest_drop(drop2)
    assert conn.execute("SELECT COUNT(*) FROM orphan_events").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 7

    out = tmp / "reports"
    generate_reports(conn, out)
    quality = json.loads((out / "data_quality.json").read_text())
    assert quality["orphan_events"] == 0
    assert quality["unknown_disposition_events"] == 1
    assert quality["malformed_events"] == 1
    assert quality["duplicate_events_dropped"] == 1
    lead_rows = list(csv.DictReader((out / "lead_report.csv").open()))
    assert len(lead_rows) == 1
    row = lead_rows[0]
    assert row["bucket"] == "completed"  # not_interested is terminal
    assert row["current_disposition"] == "not_interested"  # override wins
    assert row["ever_success"] == "false"
    assert row["total_calls"] == "1" and row["connected_calls"] == "1"
    assert row["phone_masked"] == "+91******3210"
    # IST date of the 10:00Z initiated call is 2026-10-02.
    assert row["last_call_at"] == "2026-10-02T15:30:00+05:30"
    daily = list(csv.DictReader((out / "daily_overview.csv").open()))
    assert daily == [{"workflow_id": "motor_renewal", "date": "2026-10-02",
                      "total_calls": "1", "connected_calls": "1",
                      "connectivity_rate": "1.0000", "success_leads": "0"}]


def test_telephony_failed_bucket_and_reingest_idempotent(ing):
    ingestor, tmp, conn = ing
    drop = _write_drop(tmp, "drop_1", leads=[LEAD], events=[
        _event("evt_1", "call.initiated", "call_1", "2026-10-02T10:00:00Z"),
        _event("evt_2", "call.telephony_failed", "call_1",
               "2026-10-02T10:00:05Z", payload={"reason": "carrier_rejected"}),
    ])
    ingestor.ingest_drop(drop)
    out = tmp / "reports"
    generate_reports(conn, out)
    first = (out / "data_quality.json").read_bytes()
    first_lead = (out / "lead_report.csv").read_bytes()
    # Re-ingest the same drop: nothing may change.
    ingestor.ingest_drop(drop)
    generate_reports(conn, out)
    assert (out / "data_quality.json").read_bytes() == first
    assert (out / "lead_report.csv").read_bytes() == first_lead
    row = list(csv.DictReader((out / "lead_report.csv").open()))[0]
    assert row["bucket"] == "in_progress_telephony_failed"


def test_pii_not_plaintext_in_db(ing):
    ingestor, tmp, conn = ing
    drop = _write_drop(tmp, "drop_1", leads=[LEAD])
    ingestor.ingest_drop(drop)
    conn.close()
    raw = (tmp / "test.db").read_bytes()
    assert b"Asha Verma" not in raw
    assert b"9876543210" not in raw
    assert b"+919876543210" not in raw
    # ...but the name is recoverable with the right key.
    conn = dbmod.connect(tmp / "test.db")
    enc = conn.execute(
        "SELECT customer_name_enc FROM leads").fetchone()[0]
    assert PiiProtector(tmp / "test.db").decrypt_name(enc) == "Asha Verma"
