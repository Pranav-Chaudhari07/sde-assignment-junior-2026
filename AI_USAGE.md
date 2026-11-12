# AI Usage Disclosure

AI tools are allowed. We want to know **how** you used them, because using them well is a
skill. Honest answers are never held against you. Discovering undisclosed use is.

## Tools used
Muse (Meta's personal AI agent, powered by Muse Spark) — my own
assistant, acting on my instructions throughout. No other AI tool
was used. I directed it, reviewed its output, and made the calls
below myself.

## What I used them for
- Reading both assignment READMEs and summarising the choice
  between A and B.
- Scaffolding the first draft of the pipeline modules
  (`normalize.py`, `pii.py`, `db.py`, `ingest.py`, `reports.py`)
  from the spec I had read, and the first draft of the pytest suite.
- Drafting `SUBMISSION.md` and this file from my run notes.

## What I wrote or decided myself
- The choice of Assignment A, and why (see SUBMISSION.md).
- The design: phone-as-hash + Fernet name, orphan table with
  re-linking, file-hash idempotency, SQL-first reports, and the
  decision to recompute unknown dispositions at report time.
- Every edge-case call in the Decisions table (override priority,
  defaults after merging, required-but-invalid handling, the
  `substr` date fix).
- The independent brute-force verifier and the decision to diff
  all five files on two seeds before trusting the pipeline.

## Where the AI was wrong or unhelpful
- Its first `daily_overview` used SQLite's `date()` on the IST
  timestamp, which silently shifted early-morning IST calls onto
  the previous UTC day. The unit tests did not catch it — my
  brute-force cross-check did, and I made it use `substr(...,1,10)`
  instead. That is the single most valuable catch in this
  submission, and it came from verification, not from the AI.
- Its first orphan-resolution scanned the whole orphan table in
  Python after every event file; on the large dataset that
  dominated the run. I had it rewritten as SQL joins.
- Its first lead upsert executed one SQL statement per lead and
  encrypted every name with a fresh Fernet call; profiling showed
  both as the top costs, and I had them changed to `executemany`
  and a per-name encryption cache.

## How I verified AI-generated code
- Read every module line by line before running it.
- `pytest` (13 tests) plus hand-built end-to-end drops.
- The independent brute-force verifier, run on seeds 42 and 7:
  all five report files matched exactly on both seeds.
- Idempotency checked by md5 of all reports after a second full
  ingest; PII checked by grepping the raw DB bytes for sample
  names and phones; orphan re-linking checked by reporting after
  drop_1 alone (1,123 orphans) and after drop_2 (462).
- Large-scale run (`--scale large`) timed end to end; see
  SUBMISSION.md for the numbers.
