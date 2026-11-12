"""Reports for Assignment A.

Aggregation is done in SQL (SQLite window functions + CTEs), not in
application code -- the stretch goal. Python only formats the SQL
results (4-decimal rates, masked phones, true/false) and writes files.

Derived facts, per the spec:
* a call exists only if its ``call.initiated`` is in the ledger;
* a call's date is the IST date of its ``call.initiated``;
* effective disposition = latest *known* ``disposition.override``
  by occurred_at, else latest *known* ``disposition.set`` (unknown
  codes are ignored, so they never become the effective disposition
  and never shadow a known ``set``);
* bucket order: yet_to_reach_out > completed (any terminal) >
  in_progress_telephony_failed (every call failed at telephony) >
  in_progress_dialed.
"""
from __future__ import annotations

import csv
import json
import sqlite3
from pathlib import Path

# One shared CTE: every call, with its flags and effective disposition.
CALLS_CTE = """
WITH initiated AS (
    SELECT workflow_id, call_id, phone_hash, occurred_at, occurred_at_ist,
           -- occurred_at_ist is already an IST wall-clock string, so
           -- take its date part directly. SQLite's date() would convert
           -- the '+05:30' suffix back to UTC and shift early-morning
           -- IST calls onto the previous day -- that bug cost a full
           -- re-check of daily_overview against a brute-force script.
           substr(occurred_at_ist, 1, 10) AS call_date
    FROM events
    WHERE event_type = 'call.initiated' AND occurred_at_ist IS NOT NULL
),
overrides AS (
    SELECT workflow_id, call_id,
           json_extract(payload_json, '$.disposition_code') AS code,
           ROW_NUMBER() OVER (
               PARTITION BY workflow_id, call_id
               ORDER BY occurred_at DESC, event_id DESC) AS rn
    FROM events
    WHERE event_type = 'disposition.override'
      AND EXISTS (SELECT 1 FROM dispositions d
                  WHERE d.workflow_id = events.workflow_id
                    AND d.code = json_extract(events.payload_json,
                                              '$.disposition_code'))
),
sets AS (
    SELECT workflow_id, call_id,
           json_extract(payload_json, '$.disposition_code') AS code,
           ROW_NUMBER() OVER (
               PARTITION BY workflow_id, call_id
               ORDER BY occurred_at DESC, event_id DESC) AS rn
    FROM events
    WHERE event_type = 'disposition.set'
      AND EXISTS (SELECT 1 FROM dispositions d
                  WHERE d.workflow_id = events.workflow_id
                    AND d.code = json_extract(events.payload_json,
                                              '$.disposition_code'))
),
calls AS (
    SELECT i.workflow_id, i.call_id, i.phone_hash, i.occurred_at,
           i.occurred_at_ist, i.call_date,
           EXISTS (SELECT 1 FROM events c
                   WHERE c.workflow_id = i.workflow_id
                     AND c.call_id = i.call_id
                     AND c.event_type = 'call.connected') AS connected,
           EXISTS (SELECT 1 FROM events c
                   WHERE c.workflow_id = i.workflow_id
                     AND c.call_id = i.call_id
                     AND c.event_type = 'call.telephony_failed') AS tele_failed,
           COALESCE(o.code, s.code) AS eff_code
    FROM initiated i
    LEFT JOIN overrides o
           ON o.workflow_id = i.workflow_id AND o.call_id = i.call_id
          AND o.rn = 1
    LEFT JOIN sets s
           ON s.workflow_id = i.workflow_id AND s.call_id = i.call_id
          AND s.rn = 1
),
calls_flagged AS (
    SELECT c.*,
           COALESCE(d.is_terminal, 0) AS is_terminal,
           COALESCE(d.is_success, 0) AS is_success
    FROM calls c
    LEFT JOIN dispositions d
           ON d.workflow_id = c.workflow_id AND d.code = c.eff_code
),
lead_calls AS (
    SELECT l.workflow_id, l.phone_hash, l.phone_last4, l.external_ref,
           l.batch_id,
           COUNT(c.call_id) AS total_calls,
           COALESCE(SUM(c.connected), 0) AS connected_calls,
           MAX(c.occurred_at_ist) AS last_call_at,
           COALESCE(SUM(c.is_terminal), 0) AS terminal_calls,
           COALESCE(SUM(CASE WHEN c.tele_failed THEN 0 ELSE 1 END), 0)
               AS non_failed_calls,
           COALESCE(SUM(c.is_success), 0) AS success_calls,
           CASE
               WHEN COUNT(c.call_id) = 0 THEN 'yet_to_reach_out'
               WHEN COALESCE(SUM(c.is_terminal), 0) > 0 THEN 'completed'
               WHEN COALESCE(SUM(CASE WHEN c.tele_failed THEN 0 ELSE 1 END), 0) = 0
                   THEN 'in_progress_telephony_failed'
               ELSE 'in_progress_dialed'
           END AS bucket
    FROM leads l
    LEFT JOIN calls_flagged c
           ON c.workflow_id = l.workflow_id
          AND c.phone_hash = l.phone_hash
    GROUP BY l.workflow_id, l.phone_hash, l.phone_last4,
             l.external_ref, l.batch_id
)
"""


def _write_csv(path: Path, header: list[str], rows: list[list]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(header)
        w.writerows(rows)


def generate_reports(conn: sqlite3.Connection, out_dir: str | Path) -> dict:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    written: dict[str, int] = {}

    # 1. batch_summary.csv
    rows = conn.execute(
        CALLS_CTE + """
        SELECT workflow_id, batch_id, COUNT(*) AS total_leads,
               SUM(CASE WHEN bucket='yet_to_reach_out' THEN 1 ELSE 0 END),
               SUM(CASE WHEN bucket='in_progress_telephony_failed' THEN 1 ELSE 0 END),
               SUM(CASE WHEN bucket='in_progress_dialed' THEN 1 ELSE 0 END),
               SUM(CASE WHEN bucket='completed' THEN 1 ELSE 0 END)
        FROM lead_calls
        GROUP BY workflow_id, batch_id
        ORDER BY workflow_id, batch_id
        """).fetchall()
    _write_csv(out / "batch_summary.csv",
               ["workflow_id", "batch_id", "total_leads", "yet_to_reach_out",
                "in_progress_telephony_failed", "in_progress_dialed",
                "completed"],
               [list(r) for r in rows])
    written["batch_summary.csv"] = len(rows)

    # 2. daily_overview.csv
    rows = conn.execute(
        CALLS_CTE + """
        SELECT workflow_id, call_date, COUNT(*) AS total_calls,
               SUM(connected) AS connected_calls,
               COUNT(DISTINCT CASE WHEN is_success THEN phone_hash END)
                   AS success_leads
        FROM calls_flagged
        GROUP BY workflow_id, call_date
        ORDER BY workflow_id, call_date
        """).fetchall()
    daily = []
    for wf, d, total, connected, success in rows:
        rate = f"{(connected / total if total else 0):.4f}"
        daily.append([wf, d, total, connected, rate, success])
    _write_csv(out / "daily_overview.csv",
               ["workflow_id", "date", "total_calls", "connected_calls",
                "connectivity_rate", "success_leads"], daily)
    written["daily_overview.csv"] = len(daily)

    # 3. lead_report.csv -- current disposition needs the most recent
    # call (by initiated time) that has an effective disposition.
    current_disp = conn.execute(
        CALLS_CTE + """
        SELECT workflow_id, phone_hash, eff_code
        FROM (
            SELECT workflow_id, phone_hash, eff_code,
                   ROW_NUMBER() OVER (
                       PARTITION BY workflow_id, phone_hash
                       ORDER BY occurred_at_ist DESC, call_id DESC) AS rn
            FROM calls_flagged
            WHERE eff_code IS NOT NULL
        )
        WHERE rn = 1
        """).fetchall()
    current = {(r[0], r[1]): r[2] for r in current_disp}
    rows = conn.execute(
        CALLS_CTE + """
        SELECT workflow_id, external_ref, batch_id, phone_last4, bucket,
               total_calls, connected_calls, last_call_at,
               success_calls, phone_hash
        FROM lead_calls
        ORDER BY workflow_id, external_ref
        """).fetchall()
    lead_rows = []
    for (wf, ref, batch, last4, bucket, total, connected,
         last_at, success_calls, phash) in rows:
        lead_rows.append([
            wf, ref, batch, f"+91******{last4}", bucket, total, connected,
            last_at or "", current.get((wf, phash), "") or "",
            "true" if success_calls > 0 else "false",
        ])
    _write_csv(out / "lead_report.csv",
               ["workflow_id", "external_ref", "batch_id", "phone_masked",
                "bucket", "total_calls", "connected_calls", "last_call_at",
                "current_disposition", "ever_success"], lead_rows)
    written["lead_report.csv"] = len(lead_rows)

    # 4. rejections.csv (no PII by construction)
    rows = conn.execute(
        "SELECT workflow_id, batch_id, row_number, reason FROM rejections"
        " ORDER BY workflow_id, batch_id, row_number").fetchall()
    _write_csv(out / "rejections.csv",
               ["workflow_id", "batch_id", "row_number", "reason"],
               [list(r) for r in rows])
    written["rejections.csv"] = len(rows)

    # 5. data_quality.json
    stats = conn.execute(
        "SELECT COALESCE(SUM(lead_rows_total),0),"
        " COALESCE(SUM(lead_rows_rejected),0),"
        " COALESCE(SUM(duplicate_lead_rows_merged),0),"
        " COALESCE(SUM(field_warnings),0),"
        " COALESCE(SUM(events_total),0),"
        " COALESCE(SUM(malformed_events),0),"
        " COALESCE(SUM(duplicate_events_dropped),0)"
        " FROM file_stats").fetchone()
    (lead_total, lead_rejected, dup_merged, warnings,
     events_total, malformed, dup_events) = stats
    rejections_by_reason = {
        r[0]: r[1] for r in conn.execute(
            "SELECT reason, COUNT(*) FROM rejections"
            " GROUP BY reason ORDER BY reason").fetchall()
    }
    orphan = conn.execute(
        "SELECT COUNT(*) FROM orphan_events").fetchone()[0]
    unknown = conn.execute(
        "SELECT COUNT(*) FROM events"
        " WHERE event_type IN ('disposition.set', 'disposition.override')"
        " AND NOT EXISTS (SELECT 1 FROM dispositions d"
        "   WHERE d.workflow_id = events.workflow_id"
        "     AND d.code = json_extract(events.payload_json,"
        "                             '$.disposition_code'))").fetchone()[0]
    quality = {
        "lead_rows_total": lead_total,
        "lead_rows_rejected": lead_rejected,
        "rejections_by_reason": rejections_by_reason,
        "duplicate_lead_rows_merged": dup_merged,
        "field_warnings": warnings,
        "events_total": events_total,
        "malformed_events": malformed,
        "duplicate_events_dropped": dup_events,
        "orphan_events": orphan,
        "unknown_disposition_events": unknown,
    }
    (out / "data_quality.json").write_text(
        json.dumps(quality, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    written["data_quality.json"] = 1
    return written
