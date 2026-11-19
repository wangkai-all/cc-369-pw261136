# Typed Migration: loose text → date / fixed-point money / controlled enum

A from-scratch implementation of the migration pipeline described below. It is
stdlib-only Python 3.11 (SQLite, `http.server`), so it runs anywhere.

```bash
python3 -m pytest tests -q          # or: /tmp/venv/bin/python -m pytest tests -q
python3 -m scripts.demo             # end-to-end walkthrough with real output
python3 -m migrator.web.console --db migrator.db --port 8765   # operator page
python3 -m migrator.worker --db migrator.db --once             # drain staged batches
```

## What the problem asks for and where it lives

| Requirement | Implementation |
|---|---|
| Loose text → date, fixed-point money, controlled enum | `migrator/types.py` (`parse_date`, `parse_money`, `parse_enum`) |
| Historical bad values must **not** silently become NULL | `ParseFailure` is a first-class value; every failure is persisted in `failures` with `batch=… row=… field=…`, raw text and rule version. `render()` is the only place a failure becomes NULL, and only after the failure row is written. |
| Adapter / migration worker / query service cooperate | `migrator/ingest.py`, `migrator/worker.py` + `migrator/engine.py`, `migrator/query.py` |
| Live new batches keep arriving during migration | Ingest only writes `records` + `status='staged'`. Workers compute outside transactions and only take a short write transaction to commit, keyed on `(batch_id, rule_version)`. |
| Whole-batch reject vs. quarantine bad rows vs. keep dual columns | `on_failure = reject_batch \| quarantine_row \| preserve_dual` |
| Callers must know what is comparable and when the new type is truly promised | `query.readiness()` splits `comparable` (can compare in scope *now*) from `committed` (type promise). `typed` queries raise `NotComparable` rather than fall back to text. |
| Preview failed samples, revise rules, replay affected batches, switch query version | Console: `/preview`, `/publish`, `/replay`, `/activate` in `migrator/web/console.py` |
| Late migration process must not overwrite newer rules | `engine.commit_projection` compare-and-swaps on the batch's stored rule version; stale commits raise `StaleWorkerError`. `replay_batch` first moves the batch to the new version. |
| Cross-file error positions and real output | Failures carry `location = batch=… row=… field=…` across `types.py`, `engine.py`, `store.py`, `query.py`. `scripts/demo.py` prints real output. |
| Not "just turn everything into strings" | Values are `date` / `Decimal` / canonical enum members; comparisons use typed ordering (`_sort_key`), sums use `Decimal`. |

## Policies: what each one does

All three keep the raw text in `records.raw_json` and in `record_values.raw_text`,
so a later rule revision can re-derive without the source.

- **`quarantine_row`** (default): clean rows are projected; dirty rows are held
  out of `record_values`, but each failing field is recorded in `failures` with
  position and raw text; the batch is marked `quarantined`.
- **`reject_batch`**: if *any* row is dirty, nothing is committed for the batch
  (no `record_values`), but the failures are still recorded and the batch is
  marked `rejected`. The query version is untouched.
- **`preserve_dual`**: every row is projected; a failing field keeps its raw
  text and has `typed_value = NULL`, `parse_state = 'failed'`. Nothing is lost
  and the row remains queryable under `legacy`.

## Concurrency model

1. **Ingest** writes raw rows and marks the batch `staged`. It never parses and
   never blocks on workers, so live batches arrive during a replay.
2. **Workers** compute projections without a transaction. Before computing they
   claim a *staged* batch for the target rule version; when committing they
   compare-and-swap the batch's `rule_version`. A worker that computed under v2
   after an operator replayed the batch under v3 gets `StaleWorkerError` and
   writes nothing (see `test_stale_worker_cannot_overwrite_newer_replay`).
3. **Operator replay** re-derives from untouched raw rows under a chosen rule
   version, advancing the batch's version first so stale workers lose the CAS.
4. **Query version** (`query_versions.typed`) is a separate pointer. A field is
   only `committed` when query version = active rule = scope rule, and there is
   no failure blocking it. `comparable` can be true earlier, per batch.

## Console walkthrough

`python3 -m scripts.demo` prints:

- ingest of a 5-row batch with an impossible date (`2024-13-40`), a missing
  field, a 3-decimal amount, and unknown enum values;
- projection failures with exact `batch=… row=… field=…` positions;
- readiness before/after, showing `typed_promise: false` until replay +
  activate;
- preview of a revised rule with zero writes (`sample_failures` visible);
- publish v2 + replay, then a slow worker's v1 commit being refused;
- a live batch staged mid-migration and projected later;
- typed `sum` and `between` queries only after the promise holds.

## Tests

`tests/test_migration.py` covers: failure raw+position retention, the three
policies, readiness gating, stale-worker CAS, worker claim only for staged
batches, live ingest during replay, dry-run preview, invalid-rule paths,
enum case/alias control, exact date formats, and Decimal money arithmetic.
