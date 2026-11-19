"""Migration worker: pulls staged batches and projects them under a rule version.

Run as a process (`python -m migrator.worker`) or drive it from tests with a
fixed rule version to simulate a slow worker. The important guarantee is in
the commit path (see engine.project_batch): a worker whose rule version is no
longer the one recorded on the batch cannot commit.

Deliberately has no lease/lock over `records`: ingestion appends new batches
throughout. Worker only takes a write transaction for the commit itself.
"""

from __future__ import annotations

import argparse
import json
import sys
import time

from .engine import BatchError, StaleWorkerError, _now, project_batch
from .store import Store


def claim_staged(store: Store, limit: int = 20) -> list[str]:
    rows = store._conn.execute(
        "SELECT batch_id FROM batches WHERE status='staged' ORDER BY received_at LIMIT ?",
        (limit,),
    ).fetchall()
    return [r["batch_id"] for r in rows]


def run_once(store: Store, rule_version: int | None = None, actor: str = "worker") -> list[dict]:
    results = []
    for batch_id in claim_staged(store):
        try:
            res = project_batch(store, batch_id, rule_version=rule_version, actor=actor)
            results.append(res.as_json())
        except StaleWorkerError as e:
            # A late worker is expected during replays; record and move on.
            store.log(
                _now(), actor, "worker.stale_commit_refused",
                {"batch_id": e.batch_id, "worker_rule": e.worker_rule, "applied_rule": e.applied_rule},
            )
            results.append({"batch_id": batch_id, "committed": False, "stale": True, "error": str(e)})
        except BatchError as e:
            results.append({"batch_id": batch_id, "committed": False, "error": str(e)})
    return results


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="migrator.worker")
    parser.add_argument("--db", default="migrator.db")
    parser.add_argument("--rule-version", type=int, default=None,
                        help="project under this rule (default: whatever is active)")
    parser.add_argument("--once", action="store_true", help="drain staged batches and exit")
    parser.add_argument("--interval", type=float, default=2.0)
    args = parser.parse_args(argv)

    store = Store(args.db)
    if args.once:
        out = run_once(store, args.rule_version)
        print(json.dumps(out, indent=2))
        return 0 if all(r.get("committed") or r.get("stale") for r in out) else 1

    while True:
        out = run_once(store, args.rule_version)
        if out:
            print(json.dumps(out), flush=True)
        time.sleep(args.interval)


if __name__ == "__main__":
    sys.exit(main())
