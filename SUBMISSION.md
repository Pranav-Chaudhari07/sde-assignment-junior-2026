# Submission

## Candidate
- **Name:** Pranav Chaudhari
- **Assignment chosen:** A (Data)
- **Time spent (honest estimate):** ~7h (including building, independent verification, and the large-scale run)

## How to run
```bash
# from a clean clone, in the repo root
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

python assignment-a-data/tools/generate_data.py --seed 42 --scale small --out data
python -m solution.pipeline ingest data/drop_1
python -m solution.pipeline ingest data/drop_2
python -m solution.pipeline report --out reports/

# tests
python -m pytest solution/tests/ -q
```
Notes: the database defaults to `au.db` in the current directory
(`--db` to change it); the PII key is `au.db.key` next to it. Both
are git-ignored. `ingest` accepts several drops in one call.
Python 3.11+; only extra dependency is `cryptography` (Fernet).

## Design
SQLite, one database file, five tables that matter:

- `leads (workflow_id, phone_hash)` — one row per lead identity.
  The phone is stored only as a SHA-256 hash of the canonical
  `+91XXXXXXXXXX` form, plus `phone_last4` (needed for the masked
  report column). The customer name is Fernet-encrypted
  (`customer_name_enc`); the key comes from `AU_PII_KEY` or is
  generated once into `<db>.key` (0600, never committed). All other
  lead fields live as normalised values in `fields_json`, with
  `external_ref` denormalised for the lead-report sort. `batch_id`
  is the file stem of the first valid row and is never overwritten.
- `events` — the append-only interaction ledger, keyed by
  `event_id`. Nothing here is ever updated or deleted; an override
  is a new row and the effective disposition is computed at report
  time. Unknown-disposition events are kept here too; reports
  ignore them by joining to `dispositions`.
- `orphan_events` — events that cannot yet be linked to a lead.
  Kept apart so they stay out of every report, and so a later lead
  ingest can move them into the ledger. The final orphan count is
  simply `COUNT(*)` here.
- `rejections` — one row per rejected lead row (workflow, batch,
  row number, reason; no PII by construction).
- `ingested_files` / `file_stats` — idempotency and counters. A
  file is identified by the SHA-256 of its bytes, recorded in the
  same transaction as its effects.

Ingestion order per drop: lead files (filename order), then event
files (filename order), with orphan resolution after every file
and once more at the end. Final state does not depend on whether
an event or its lead arrived first.

Reports are aggregated in SQL, not in application code: one CTE
builds every call (initiated + connected/telephone-failed flags +
effective disposition via window functions over overrides/sets),
a second CTE rolls calls up to leads (bucket, totals, last call),
and the five files are plain `SELECT`s over those CTEs. Python
only formats (4-decimal rate, `+91******1234`, `true`/`false`).

## Decisions & assumptions

| Decision | Why | Alternative considered |
|---|---|---|
| Chose A over B | Data correctness is the core of the platform and my strongest area (Python/SQL/data); B's simulated-clock dispatcher is riskier to get to 0 violations in the same time | B: durable dispatcher on a DB state machine |
| Phone as SHA-256 hash (+ last-4) | Events must still join on phone, so a deterministic one-way hash is the only joinable protection; an optional `AU_PII_PEPPER` env var hardens it against rainbow tables of all 10-digit numbers | HMAC with a KMS key (better in prod; same shape) |
| Name as Fernet-encrypted, key in `<db>.key` | Name is never joined on or reported, so reversible encryption is right; the DB alone must not reveal it | Hashing the name (destroys it; reports don't need it, but a client lookup would) |
| File hash = idempotency key | Re-ingesting a drop must change *nothing*, including counters; per-file dedupe by `event_id` alone would double-count duplicate-lead merges on a re-run | Event/lead dedupe only, no file tracking |
| Orphan events kept, re-linked later | Spec: an event orphan after drop_1 must count once its lead arrives in drop_2; final orphan count reflects final state | Counting orphans at ingest time (wrong after drop_2) |
| Unknown disposition recomputed at report time | An orphan that later links and turns out unknown must then count as unknown, not as orphan | Counting unknown at ingest time only |
| Defaults applied after merging | Spec says so; in practice I store merged fields as-is and apply defaults on read — no report column shows a defaulted field, so this is invisible in output but kept spec-true | Applying defaults at row creation (same observable result) |
| Required field present but invalid (date/decimal/enum) → warning + null, not rejection | The spec gives only two rejection reasons (`missing_required:*`, `invalid_phone`); there is no rejection reason for a bad optional-type value on a required field, and neither schema has a required date/decimal/enum field, so this never fires on the given schemas | Rejecting it as `missing_required:*` (would invent a reason) |
| `call_date` via `substr(occurred_at_ist, 1, 10)`, not SQLite `date()` | SQLite's `date()` converts a `+05:30` timestamp back to UTC, shifting early-morning IST calls onto the previous day — found by cross-checking daily_overview against a brute-force script | Storing IST wall time without an offset (would break the `+05:30` format of `last_call_at`) |
| Effective disposition: latest *known* override wins over any set | Spec: override first, else latest set; "unknown" events are ignored, so an unknown override must not shadow a known set | Latest disposition event overall, regardless of type |
| SQLite `synchronous=OFF` for bulk ingest | On this VM ingest fell from 6m13s to 1m48s (fsync wait dominated: 98s sys vs 41s user with NORMAL). With WAL, a process crash still rolls back a torn commit; only an OS crash could lose a committed transaction, and a lost file-hash record just means that file is re-ingested | `synchronous=NORMAL` (safer on power loss, 3.5x slower here) |

## Failure modes considered
- **Re-ingest / re-run:** file-hash skip; reports byte-identical after a
  second full ingest (verified with md5).
- **Crash mid-file:** each file's effects + its file hash commit in
  one transaction; a torn file is simply re-ingested.
- **Events before leads / overrides before initiated:** orphans wait
  in `orphan_events`; resolution is iterative (a moved initiated
  enables a phone-less override for the same call) and SQL-join
  based, so it stays fast on the large dataset.
- **Duplicate events across drops, malformed lines, events for
  never-uploaded numbers, rejected leads with calls:** all counted
  in `data_quality.json` and kept out of reports; verified on
  seeds 42 and 7.
- **Dirty phones/dates/currency/enum, renamed/reordered columns,
  blank rows, extra columns:** covered by `normalize.py` and its
  unit tests (6 phone formats, day-first dates, `₹`/`Rs.`/`INR`,
  case-insensitive enum/month).
- **PII in logs/reports/DB:** no full phone or name is ever logged
  or written to a report; I grepped the DB bytes for sample names
  and phones — none present; the encrypted name round-trips with
  the right key.
- **Not handled:** a `call.initiated` with an unparsable timestamp
  is stored with a null time and excluded from daily counts (the
  generator never produces one); concurrent writers to the same
  DB file are not supported (single-writer CLI); the PII key file
  must be backed up with the DB or names become unrecoverable —
  by design.

## Testing
- `python -m pytest solution/tests/ -q` — **13 passed**.
  Unit tests for every normalisation rule, and end-to-end tests on
  hand-built drops: rejection order (schema order, first failure),
  duplicate merge (last-write-wins, permanent `batch_id`),
  orphan-then-lead, phone-less override beating a later set,
  telephony-failed bucket, unknown/malformed/duplicate events,
  re-ingest idempotency, PII not in DB bytes.
- Independent verification: I wrote a separate brute-force
  re-implementation of the spec (kept outside the repo) and diffed
  all five report files against it on **seed 42 and seed 7, small
  scale — all five files matched exactly on both**. That cross-check
  caught a real bug (the SQLite `date()` UTC shift above) that the
  unit tests had missed.
- **Not tested:** PostgreSQL (SQLite was chosen; the SQL uses
  window functions and `json_extract`, both portable, but I did
  not run it against Postgres), and a kill-mid-ingest crash test
  (transaction rollback is SQLite's guarantee, not mine to prove
  by killing the process).

## Results
Seed 42, small (1,000 leads per workflow, 2,000 total;
8,745 event lines):
```json
{"lead_rows_total": 2057, "lead_rows_rejected": 108,
 "duplicate_lead_rows_merged": 57, "field_warnings": 122,
 "events_total": 8745, "malformed_events": 8,
 "duplicate_events_dropped": 237, "orphan_events": 462,
 "unknown_disposition_events": 18}
```
1,892 leads, 6 batch rows, 33 daily rows. Orphans fell from 1,123
after drop_1 alone to 462 after drop_2 — 661 events were rescued
when their leads arrived. Incremental ingest (drop_1, then drop_2)
produces byte-identical reports to ingesting both drops at once.

Large scale (`--scale large`, 140,000 leads, 632,602 event lines),
measured on the shared 1.5-CPU VM I worked on:
- **ingest (both drops): 1m48s** real (44.8s user, 28.1s sys),
  133 files, exit 0
- **report: 40s** real (12.6s user)
- **Total: ~2m28s — the under-2-minutes stretch target is not met
  on this machine.** CPU time for both steps is only ~57s; the
  rest is I/O wait / throttling on this shared VM. With the
  default `synchronous=NORMAL` ingest alone took 6m13s here, so
  the bulk-load setting below bought most of the improvement.
  On a laptop with a local SSD I would expect it under the
  target, but I did not have one to measure on, so I am not
  claiming it.
```json
{"lead_rows_total": 143990, "lead_rows_rejected": 7097,
 "duplicate_lead_rows_merged": 3990, "field_warnings": 8218,
 "events_total": 632602, "malformed_events": 631,
 "duplicate_events_dropped": 18490, "orphan_events": 34424,
 "unknown_disposition_events": 1005}
```
132,903 leads, 36 daily rows. The large `data_quality.json`
is identical across the two large runs (before/after the
synchronous change), so the optimisation did not change results.

Surprising in the data: the generator's "unknown" dispositions are
rare (18 / 8,745) but real; duplicate event deliveries are common
(237 / 8,745, ~2.7%); and about a quarter of leads never get a call
at all, which is why `yet_to_reach_out` is the second-largest bucket.

## If I had another day
1. Postgres run + a `docker compose` one-command path, to prove the
   SQL is portable and the schema works under concurrent readers.
2. A kill-mid-ingest test script (kill -9 at random points, re-run,
   diff reports) to demonstrate the rollback story instead of
   asserting it.
3. Truly incremental reporting: cache per-lead/per-day aggregates
   and update only the leads touched by a drop, instead of
   recomputing all reports from the ledger each time.
4. A production note (scheduling, backfills, dead-letter handling
   for orphans that never link) — currently only sketched in
   `SUBMISSION.md` §Failure modes, not written up as a runbook.
