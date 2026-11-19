"""Projection engine: parse incoming rows, commit with optimistic concurrency.

Workers compute a projection *outside* any transaction (the slow part), then
commit with a compare-and-swap on the batch's stored rule version. A worker
that computed under v2 cannot overwrite values a replay under v3 already
committed: its commit raises `StaleWorkerError` and nothing changes.
"""

from __future__ import annotations

import datetime as _dt
import hashlib
import json
from dataclasses import dataclass, field
from typing import Optional

from .store import Store
from .types import (
    ParseContext,
    ParseFailure,
    Parsed,
    parse_date,
    parse_enum,
    parse_money,
    render,
)


class StaleWorkerError(RuntimeError):
    """A worker attempted to commit values derived from a superseded rule."""

    def __init__(self, batch_id: str, worker_rule: int, applied_rule: Optional[int]):
        self.batch_id = batch_id
        self.worker_rule = worker_rule
        self.applied_rule = applied_rule
        super().__init__(
            f"batch={batch_id}: worker computed under rule v{worker_rule} but "
            f"batch is already at rule v{applied_rule}; refusing late commit"
        )


class BatchError(RuntimeError):
    pass


def parse_field(raw: object, spec: dict, ctx: ParseContext) -> Parsed:
    if raw is None:
        return ParseFailure(ctx.batch_id, ctx.row_index, ctx.field, "", "missing field", ctx.rule_version)
    if not isinstance(raw, str):
        return ParseFailure(
            ctx.batch_id, ctx.row_index, ctx.field, json.dumps(raw),
            f"expected a string from the legacy text column, got {type(raw).__name__}",
            ctx.rule_version,
        )
    ftype = spec["type"]
    if ftype == "date":
        return parse_date(raw, ctx)
    if ftype == "money":
        return parse_money(raw, ctx)
    if ftype == "enum":
        return parse_enum(raw, ctx, tuple(spec["allowed"]), spec["aliases"])
    raise AssertionError(f"unhandled type {ftype}")


def parse_row(row: dict, config: dict, batch_id: str, row_index: int, rule_version: int):
    values: dict[str, Parsed] = {}
    failures: list[ParseFailure] = []
    for field_name, spec in config["fields"].items():
        ctx = ParseContext(batch_id=batch_id, row_index=row_index, field=field_name, rule_version=rule_version)
        parsed = parse_field(row.get(field_name), spec, ctx)
        values[field_name] = parsed
        if isinstance(parsed, ParseFailure):
            failures.append(parsed)
    return values, failures


@dataclass
class RowProjection:
    row_index: int
    values: dict[str, Parsed]
    failures: list[ParseFailure]


@dataclass
class Projection:
    """A computed-but-uncommitted result. Safe to hold while live data arrives."""

    batch_id: str
    rule_version: int
    policy: str
    rows: list[RowProjection]
    raw_rows: list[dict]
    computed_at: str = field(default_factory=lambda: _now())

    @property
    def clean_rows(self) -> int:
        return sum(1 for r in self.rows if not r.failures)

    @property
    def dirty_rows(self) -> int:
        return sum(1 for r in self.rows if r.failures)

    @property
    def failures(self) -> list[ParseFailure]:
        return [f for r in self.rows for f in r.failures]


@dataclass
class ProjectionResult:
    batch_id: str
    rule_version: int
    policy: str
    clean_rows: int
    dirty_rows: int
    committed: bool
    reason: str = ""
    failures: Optional[list[dict]] = None

    def as_json(self) -> dict:
        return {
            "batch_id": self.batch_id,
            "rule_version": self.rule_version,
            "policy": self.policy,
            "clean_rows": self.clean_rows,
            "dirty_rows": self.dirty_rows,
            "committed": self.committed,
            "reason": self.reason,
            "failures": self.failures or [],
        }


def compute_projection(store: Store, batch_id: str, rule_version: Optional[int] = None) -> Projection:
    if rule_version is None:
        rule_version = int(store.current_rule()["version"])
    config = store.get_rule(rule_version)
    rows = store.batch_rows(batch_id)
    if not rows:
        raise BatchError(f"batch={batch_id} has no rows or does not exist")
    out = []
    for i, row in enumerate(rows):
        values, failures = parse_row(row, config, batch_id, i, rule_version)
        out.append(RowProjection(i, values, failures))
    return Projection(batch_id, rule_version, config["on_failure"], out, rows)


def commit_projection(store: Store, projection: Projection, actor: str = "worker") -> ProjectionResult:
    """Commit a computed projection, guarded by the batch's rule version."""
    batch_id = projection.batch_id
    rule_version = projection.rule_version
    config = store.get_rule(rule_version)  # re-read: protects against config drift
    policy = config["on_failure"]
    if policy != projection.policy:
        raise BatchError(
            f"batch={batch_id}: projection policy {projection.policy!r} no longer matches "
            f"rule v{rule_version} policy {policy!r}"
        )

    dirty = projection.dirty_rows
    all_failures = [f.as_json() for f in projection.failures]

    if policy == "reject_batch" and dirty:
        # Refuse the whole batch: no record_values are written, but the
        # failures are still recorded so the operator can inspect and fix.
        with store.tx() as conn:
            row = conn.execute("SELECT rule_version FROM batches WHERE batch_id=?", (batch_id,)).fetchone()
            if row is None:
                raise BatchError(f"batch={batch_id} does not exist")
            if row["rule_version"] != rule_version:
                raise StaleWorkerError(batch_id, rule_version, row["rule_version"])
            conn.execute("DELETE FROM failures WHERE batch_id=?", (batch_id,))
            conn.execute("DELETE FROM record_values WHERE batch_id=?", (batch_id,))
            for rp in projection.rows:
                for f in rp.failures:
                    conn.execute(
                        "INSERT INTO failures(batch_id,row_index,field,raw,reason,rule_version) VALUES (?,?,?,?,?,?)",
                        (batch_id, rp.row_index, f.field, f.raw or "", f.reason, rule_version),
                    )
            conn.execute("UPDATE batches SET status='rejected', rule_version=? WHERE batch_id=?",
                         (rule_version, batch_id))
            conn.execute(
                "INSERT INTO audit(at,actor,action,payload) VALUES (?,?,?,?)",
                (_now(), actor, "project.rejected",
                 json.dumps({"batch_id": batch_id, "rule_version": rule_version, "dirty_rows": dirty},
                            sort_keys=True)),
            )
        return ProjectionResult(
            batch_id, rule_version, policy, projection.clean_rows, dirty, False,
            f"on_failure=reject_batch: {dirty} of {len(projection.rows)} rows have unparseable "
            f"values; batch refused, nothing committed",
            all_failures,
        )

    to_write = projection.rows if policy == "preserve_dual" else [r for r in projection.rows if not r.failures]

    with store.tx() as conn:
        row = conn.execute("SELECT rule_version FROM batches WHERE batch_id=?", (batch_id,)).fetchone()
        if row is None:
            raise BatchError(f"batch={batch_id} does not exist")
        if row["rule_version"] != rule_version:
            raise StaleWorkerError(batch_id, rule_version, row["rule_version"])

        conn.execute("DELETE FROM failures WHERE batch_id=?", (batch_id,))
        conn.execute("DELETE FROM record_values WHERE batch_id=?", (batch_id,))
        for rp in to_write:
            raw_row = projection.raw_rows[rp.row_index]
            for field_name, parsed in rp.values.items():
                raw_val = raw_row.get(field_name)
                raw_text = raw_val if isinstance(raw_val, str) or raw_val is None else json.dumps(raw_val)
                kind = config["fields"][field_name]["type"]
                if isinstance(parsed, ParseFailure):
                    conn.execute(
                        """INSERT INTO record_values
                           (batch_id,row_index,field,raw_text,typed_value,typed_kind,parse_state,rule_version)
                           VALUES (?,?,?,?,NULL,?,'failed',?)""",
                        (batch_id, rp.row_index, field_name, raw_text, kind, rule_version),
                    )
                else:
                    conn.execute(
                        """INSERT INTO record_values
                           (batch_id,row_index,field,raw_text,typed_value,typed_kind,parse_state,rule_version)
                           VALUES (?,?,?,?,?,?,'ok',?)""",
                        (batch_id, rp.row_index, field_name, raw_text, render(parsed), parsed.kind, rule_version),
                    )

        # Failures are recorded for every row in every policy. Under
        # quarantine the dirty row is absent from record_values, but its raw
        # text remains here (and untouched in records.raw_json): never blanked.
        for rp in projection.rows:
            raw_row = projection.raw_rows[rp.row_index]
            for f in rp.failures:
                raw_val = raw_row.get(f.field)
                raw_text = raw_val if isinstance(raw_val, str) or raw_val is None else json.dumps(raw_val)
                conn.execute(
                    "INSERT INTO failures(batch_id,row_index,field,raw,reason,rule_version) VALUES (?,?,?,?,?,?)",
                    (batch_id, rp.row_index, f.field, raw_text or "", f.reason, rule_version),
                )
        status = "quarantined" if dirty and policy == "quarantine_row" else "projected"
        conn.execute(
            "UPDATE batches SET status=?, rule_version=? WHERE batch_id=?",
            (status, rule_version, batch_id),
        )
        conn.execute(
            "INSERT INTO audit(at,actor,action,payload) VALUES (?,?,?,?)",
            (
                _now(), actor, "project.committed",
                json.dumps(
                    {"batch_id": batch_id, "rule_version": rule_version, "policy": policy,
                     "clean_rows": projection.clean_rows, "dirty_rows": dirty},
                    sort_keys=True,
                ),
            ),
        )

    return ProjectionResult(
        batch_id, rule_version, policy, projection.clean_rows, dirty, True, "", all_failures
    )


def claim_staged_version(store: Store, batch_id: str, target_version: int) -> bool:
    """Move a *staged* batch onto `target_version` before computing under it.

    This is the worker's generation claim. It is guarded on status='staged' so
    it can never disturb a batch an operator has already replayed, and it is a
    no-op when the batch is already on the target version.
    """
    store.get_rule(target_version)
    with store.tx() as conn:
        cur = conn.execute(
            "UPDATE batches SET rule_version=? WHERE batch_id=? AND status='staged'",
            (target_version, batch_id),
        )
        if cur.rowcount:
            return True
        row = conn.execute(
            "SELECT status, rule_version FROM batches WHERE batch_id=?", (batch_id,)
        ).fetchone()
    return bool(row and row["status"] == "staged" and row["rule_version"] == target_version)


def project_batch(store: Store, batch_id: str, rule_version: Optional[int] = None,
                  actor: str = "worker") -> ProjectionResult:
    if rule_version is None:
        rule_version = int(store.current_rule()["version"])
    claim_staged_version(store, batch_id, rule_version)
    return commit_projection(store, compute_projection(store, batch_id, rule_version), actor=actor)


def replay_batch(store: Store, batch_id: str, rule_version: Optional[int] = None,
                 actor: str = "operator") -> ProjectionResult:
    """Re-derive one batch from untouched raw rows under a (usually newer) rule.

    Advancing the batch's rule version first means any in-flight worker that
    computed under the old version fails its CAS commit instead of clobbering
    this result.
    """
    if rule_version is None:
        rule_version = int(store.current_rule()["version"])
    store.get_rule(rule_version)
    projection = compute_projection(store, batch_id, rule_version)
    with store.tx() as conn:
        row = conn.execute("SELECT 1 FROM batches WHERE batch_id=?", (batch_id,)).fetchone()
        if row is None:
            raise BatchError(f"batch={batch_id} does not exist")
        conn.execute("UPDATE batches SET rule_version=? WHERE batch_id=?", (rule_version, batch_id))
    return commit_projection(store, projection, actor=actor)


def _now() -> str:
    return _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="seconds")


def fingerprint(payload: object) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
