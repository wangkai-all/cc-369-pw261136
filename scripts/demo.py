"""End-to-end walkthrough with real output.

Run: python -m scripts.demo
"""

from __future__ import annotations

import json
import sys
import tempfile

from migrator import rules as rulesmod
from migrator.engine import StaleWorkerError, commit_projection, compute_projection, project_batch, replay_batch
from migrator.ingest import receive_batch
from migrator.query import NotComparable, query, readiness
from migrator.store import Store
from migrator.types import ParseContext, parse_date, parse_money

FIELDS = ["ordered_on", "total", "status"]
RULE_V1 = {
    "fields": {
        "ordered_on": {"type": "date"},
        "total": {"type": "money"},
        "status": {"type": "enum", "allowed": ["paid", "pending", "void"],
                   "aliases": {"PEND": "pending"}},
    },
    "on_failure": "quarantine_row",
}


def show(title: str, payload) -> None:
    print(f"\n=== {title} ===")
    if isinstance(payload, str):
        print(payload)
    else:
        print(json.dumps(payload, indent=2, ensure_ascii=False, default=str))


def main() -> int:
    db = tempfile.NamedTemporaryFile(suffix=".sqlite3", delete=False).name
    store = Store(db)
    v1 = store.publish_rule(rulesmod.normalize(RULE_V1), "initial", "2026-11-20T00:00:00+00:00")
    store.activate_query_version("typed", v1, "2026-11-20T00:00:00+00:00")

    # 1. a realistic batch arrives through the adapter
    rows = [
        {"ordered_on": "2024-03-01", "total": "$1,204.50", "status": "paid"},
        {"ordered_on": "03/02/2024", "total": "89.9", "status": "PEND"},
        {"ordered_on": "2024-13-40", "total": "1,204", "status": "paid"},      # impossible date
        {"ordered_on": "2024/03/03", "total": "(12.00)", "status": "settled"}, # unknown alias
        {"ordered_on": None, "total": "12.345", "status": "weird"},            # missing + bad
    ]
    got = receive_batch(store, rows, source="nightly-etl", fields=FIELDS, batch_id="b-2024-03-05")
    show("1. ingest (staging only, nothing parsed)", got)

    # 2. projection under v1: quarantine bad rows, record every failure
    res = project_batch(store, "b-2024-03-05", rule_version=v1)
    show("2. project v1", {
        "committed": res.committed, "policy": res.policy,
        "clean_rows": res.clean_rows, "dirty_rows": res.dirty_rows,
        "failures": res.failures,
    })

    # 3. what the query service tells callers now
    show("3. readiness before fixing anything", {
        "typed_promise": readiness(store)["typed_promise"],
        "fields": readiness(store)["fields"],
    })

    # 4. the typed view refuses to compare a dirty field, per batch
    try:
        query(store, "typed", "total", ">", "100", batch_id="b-2024-03-05")
    except NotComparable as e:
        show("4. typed view refusal", f"NotComparable: {e}")
    show("4b. legacy view still readable (not comparable)", {
        "comparable": query(store, "legacy", "total", ">", "100")["comparable"],
        "note": query(store, "legacy", "total", ">", "100")["note"],
    })

    # 5. preview a rule revision with no writes
    revised = rulesmod.normalize({
        "fields": {
            "ordered_on": {"type": "date"},
            "total": {"type": "money"},
            "status": {"type": "enum", "allowed": ["paid", "pending", "void"],
                       "aliases": {"PEND": "pending", "settled": "paid"}},
        },
        "on_failure": "preserve_dual",
    })
    show("5. preview revised rule (dry run over raw rows)",
         rulesmod.validate_against_rows(revised, store.batch_rows("b-2024-03-05")))

    # 6. a slow worker computes under v1; meanwhile the operator publishes v2
    slow = compute_projection(store, "b-2024-03-05", rule_version=v1)
    v2 = store.publish_rule(revised, "fix alias + keep dual", "2026-11-20T00:05:00+00:00")
    show("6. operator publishes v2 and replays", {
        "v2": v2, "diff": rulesmod.diff(rulesmod.normalize(RULE_V1), revised),
    })
    show("6b. replay result", json.loads(json.dumps(
        replay_batch(store, "b-2024-03-05", rule_version=v2).as_json(), default=str)))

    # 7. the late worker tries to commit v1 output -> refused
    try:
        commit_projection(store, slow)
    except StaleWorkerError as e:
        show("7. late worker commit refused", str(e))
    vals = {(r["row_index"], r["field"]): dict(r) for r in store.values_for_batch("b-2024-03-05")}
    show("7b. batch state untouched by the stale worker", {
        "batch_rule": store._conn.execute(
            "SELECT rule_version FROM batches WHERE batch_id='b-2024-03-05'").fetchone()[0],
        "row2_total": vals[(2, "total")], "row4_ordered_on": vals[(4, "ordered_on")],
    })

    # 8. a live batch arrives during the replay cycle; it is staged, not blocked
    live = receive_batch(store,
                         [{"ordered_on": "2024-04-01", "total": "5.00", "status": "paid"}],
                         source="live-stream", fields=FIELDS, batch_id="b-live-0401")
    show("8. live batch staged mid-migration", live)

    # 9. still not comparable: one impossible date is still untyped
    show("9. readiness after replay", {
        "typed_promise": readiness(store)["typed_promise"],
        "reason_by_field": {f["field"]: f["reason"] for f in readiness(store)["fields"]},
    })

    # 10. the bad source value is corrected and the affected batch replayed
    store._conn.execute(
        "UPDATE records SET raw_json=? WHERE batch_id='b-2024-03-05' AND row_index=2",
        (json.dumps({"ordered_on": "2024-03-05", "total": "1,204", "status": "paid"}),),
    )
    store._conn.execute(
        "UPDATE records SET raw_json=? WHERE batch_id='b-2024-03-05' AND row_index=4",
        (json.dumps({"ordered_on": "2024-03-06", "total": "12.34", "status": "void"}),),
    )
    store._conn.commit()
    replay_batch(store, "b-2024-03-05", rule_version=v2)
    project_batch(store, "b-live-0401", rule_version=v2)
    store.activate_query_version("typed", v2, "2026-11-20T00:10:00+00:00")

    rdy = readiness(store)
    show("10. after replay + activate: the type is now promised", {
        "typed_promise": rdy["typed_promise"],
        "query_version": rdy["query_version"],
        "fields": [{k: f[k] for k in ("field", "kind", "typed_rows", "failed_rows",
                                      "comparable", "committed", "reason")} for f in rdy["fields"]],
    })
    show("11. typed comparison is now allowed", query(store, "typed", "total", "sum"))
    show("12. date range", query(store, "typed", "ordered_on", "between", "2024-03-01,2024-03-05"))

    # 13. how a raw parser reports position
    ctx = ParseContext(batch_id="b-x", row_index=9, field="total", rule_version=v2)
    show("13. a parse failure carries its own position", [
        parse_date("2024-13-40", ctx).as_json(),
        parse_money("12.345", ctx).as_json(),
    ])
    return 0


if __name__ == "__main__":
    sys.exit(main())
