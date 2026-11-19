"""Query service.

Two readable views over the same dual columns:

  legacy  -- raw text, orderable only as text, nothing is promised.
  typed   -- typed shadow values, comparable only where every value in scope
             actually parsed under one rule version.

The service never presents a failed parse as a comparable value. Two separate
questions are answered:

  comparable   for *these rows* can I compare typed values today?
  committed    is the new type actually promised to consumers yet? That needs
               full coverage, no unresolved failures, and a query version that
               still matches the active rule.
"""

from __future__ import annotations

import datetime as _dt
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Optional

from .store import Store


class ViewNotFound(KeyError):
    pass


class NotComparable(RuntimeError):
    pass


COMPARABLE_OPS = {
    "date": ["<", "<=", "=", ">=", ">", "between"],
    "money": ["<", "<=", "=", ">=", ">", "between", "sum"],
    "enum": ["=", "!=", "in"],
}


@dataclass
class FieldReadiness:
    field: str
    kind: Optional[str]
    typed_rows: int
    failed_rows: int
    pending_rows: int
    comparable: bool
    committed: bool
    reason: str

    def as_json(self) -> dict:
        return self.__dict__.copy()


def _as_float_text(v: str) -> str:
    return v


def _check_kind(kind: str, raw: str) -> None:
    """Ensure a caller-supplied comparison value parses as the field's type."""
    try:
        if kind == "money":
            Decimal(raw)
        elif kind == "date":
            _dt.date.fromisoformat(raw)
    except (InvalidOperation, ValueError) as e:
        raise NotComparable(f"comparison value {raw!r} is not a valid {kind}: {e}")


def readiness(store: Store, rule_version: Optional[int] = None,
              batch_id: Optional[str] = None) -> dict:
    """Per-field account of what consumers may rely on, optionally per batch.

    Default scope is the *whole* typed surface at the active rule: a promise
    is a statement about every row, so a clean batch must not hide a dirty one.
    Pass `batch_id` (or an explicit `rule_version`) to narrow the scope.
    """
    qv = store.query_version()
    qv_rule = int(qv["rule_version"]) if qv else None
    active_version = int(store.current_rule()["version"])
    batch_rule = None
    b_status = None
    if batch_id:
        b = store._conn.execute("SELECT rule_version, status FROM batches WHERE batch_id=?", (batch_id,)).fetchone()
        if b is None:
            raise KeyError(f"batch={batch_id} does not exist")
        batch_rule = int(b["rule_version"])
        b_status = b["status"]
    if rule_version is None:
        if batch_rule is not None:
            rule_version = batch_rule
        else:
            # The surface is only uniform at a single rule version if the
            # query version is the active one; otherwise the promise is
            # evaluated against the active rule and mixed batches show up as
            # stale below.
            rule_version = active_version

    scope_sql = " AND rv.batch_id = ?" if batch_id else ""
    scope_args = (batch_id,) if batch_id else ()

    fields: dict[str, dict] = {}
    rows = store._conn.execute(
        f"""SELECT rv.field AS field, rv.typed_kind AS typed_kind,
                   rv.parse_state AS parse_state, COUNT(*) AS n
            FROM record_values rv
            WHERE 1=1{scope_sql}
            GROUP BY rv.field, rv.typed_kind, rv.parse_state""",
        scope_args,
    ).fetchall()
    for r in rows:
        slot = fields.setdefault(
            r["field"], {"kind": None, "typed_rows": 0, "failed_rows": 0, "pending_rows": 0}
        )
        if r["typed_kind"]:
            slot["kind"] = r["typed_kind"]
        if r["parse_state"] == "ok":
            slot["typed_rows"] += r["n"]
        elif r["parse_state"] == "failed":
            slot["failed_rows"] += r["n"]
        else:
            slot["pending_rows"] += r["n"]

    # Batches projected under a different rule version make the surface mixed.
    stale_sql = "SELECT COUNT(*) AS n FROM batches WHERE rule_version != ? AND status != 'staged'"
    stale_args: list = [rule_version]
    if batch_id:
        stale_sql += " AND batch_id = ?"
        stale_args.append(batch_id)
    stale_batches = store._conn.execute(stale_sql, stale_args).fetchone()["n"]

    # Fields typed as more than one kind cannot be compared as one type.
    kind_sql = ("SELECT rv.field AS field FROM record_values rv WHERE rv.parse_state='ok'"
                + scope_sql + " GROUP BY rv.field HAVING COUNT(DISTINCT rv.typed_kind) > 1")
    mixed_kinds = {r["field"] for r in store._conn.execute(kind_sql, scope_args).fetchall()}

    unresolved_sql = "SELECT COUNT(*) AS n FROM failures WHERE resolved=0"
    unresolved_args: list = []
    if batch_id:
        unresolved_sql += " AND batch_id = ?"
        unresolved_args.append(batch_id)
    unresolved = store._conn.execute(unresolved_sql, unresolved_args).fetchone()["n"]

    # Per-field failures in scope, including quarantined rows that never made
    # it into record_values. `failures` is the source of truth: every failed
    # value is mirrored there, so counts never double up across policies.
    fail_sql = "SELECT field, COUNT(*) AS n FROM failures WHERE resolved=0"
    fail_args: list = []
    if batch_id:
        fail_sql += " AND batch_id = ?"
        fail_args.append(batch_id)
    fail_sql += " GROUP BY field"
    failures_by_field = {r["field"]: r["n"] for r in store._conn.execute(fail_sql, fail_args).fetchall()}

    # Live batches that have not been projected at all. Their fields have no
    # record_values rows, so pending coverage must be counted from `records`
    # and the field list of the batch's rule config.
    pending_by_field: dict[str, int] = {}
    pending_batches = store._conn.execute(
        "SELECT batch_id FROM batches WHERE status='staged'"
        + (" AND batch_id=?" if batch_id else ""),
        ((batch_id,) if batch_id else ()),
    ).fetchall()
    for b in pending_batches:
        n = store._conn.execute(
            "SELECT COUNT(*) AS n FROM records WHERE batch_id=?", (b["batch_id"],)
        ).fetchone()["n"]
        try:
            cfg = store.get_rule(
                int(store._conn.execute(
                    "SELECT rule_version FROM batches WHERE batch_id=?", (b["batch_id"],)
                ).fetchone()["rule_version"])
            )
        except KeyError:
            continue
        for fname in cfg.get("fields", {}):
            pending_by_field[fname] = pending_by_field.get(fname, 0) + n

    # Fields that only appear in quarantined/rejected rows must be visible too.
    for fname in set(failures_by_field) | set(pending_by_field):
        fields.setdefault(fname, {"kind": None, "typed_rows": 0, "failed_rows": 0, "pending_rows": 0})

    out = []
    for field, slot in sorted(fields.items()):
        failed = failures_by_field.get(field, slot["failed_rows"])
        pending = pending_by_field.get(field, slot["pending_rows"])
        reasons = []
        if field in mixed_kinds:
            reasons.append("field is typed under more than one kind in scope")
        if stale_batches:
            reasons.append(f"{stale_batches} batch(es) in scope sit on another rule version")
        if failed:
            reasons.append(f"{failed} value(s) failed to parse")
        if pending:
            reasons.append(f"{pending} value(s) are in staged batches awaiting projection")
        comparable = not reasons

        committed = (
            comparable
            and unresolved == 0
            and qv_rule is not None
            and qv_rule == rule_version
            and qv_rule == active_version
        )
        if comparable and not committed:
            if qv_rule is None:
                reasons.append("no typed query version has been activated")
            elif qv_rule != active_version:
                reasons.append(
                    f"typed query version v{qv_rule} does not match active rule v{active_version}; "
                    f"replay and activate before promising the type"
                )
            elif qv_rule != rule_version:
                reasons.append(
                    f"typed query version v{qv_rule} does not match scope rule v{rule_version}"
                )
            if unresolved:
                reasons.append(f"{unresolved} unresolved failure(s) block the type promise")
        if not reasons:
            reasons.append("fully typed under the active query version")

        out.append(
            FieldReadiness(
                field=field,
                kind=slot["kind"],
                typed_rows=slot["typed_rows"],
                failed_rows=failed,
                pending_rows=pending,
                comparable=comparable,
                committed=committed,
                reason="; ".join(reasons),
            ).as_json()
        )

    if batch_id:
        scope = {"batch_id": batch_id, "rule_version": batch_rule, "status": b_status}
    else:
        scope = {"batch_id": None, "rule_version": rule_version}

    return {
        "rule_version": rule_version,
        "scope": scope,
        "query_version": {
            "name": qv["name"] if qv else None,
            "rule_version": qv_rule,
            "activated_at": qv["activated_at"] if qv else None,
            # A query version only promises output for batches projected at or
            # after its rule; replayed batches are moved onto that version.
            "matches_active_rule": qv_rule == active_version,
        },
        "typed_promise": all(f["committed"] for f in out) if out else False,        "fields": out,
        "comparable_operators": COMPARABLE_OPS,
    }


def _sort_key(kind: Optional[str]):
    if kind == "money":
        return lambda v: Decimal(v)
    if kind == "date":
        return lambda v: _dt.date.fromisoformat(v)
    return lambda v: v


def query(store: Store, view: str, field: str, op: str, value: Optional[str] = None,
          batch_id: Optional[str] = None) -> dict:
    """Run a comparison under an explicit view.

    `legacy` compares raw text and says so. `typed` refuses fields that are
    not comparable in scope instead of quietly falling back to text.
    """
    if view not in ("legacy", "typed"):
        raise ViewNotFound(f"unknown view {view!r}; expected 'legacy' or 'typed'")

    if view == "legacy":
        sql = ("SELECT batch_id, row_index, field, raw_text AS value FROM record_values "
               "WHERE field=? AND raw_text IS NOT NULL")
        args: list = [field]
        if batch_id:
            sql += " AND batch_id=?"
            args.append(batch_id)
        sql += " ORDER BY raw_text"
        rows = store._conn.execute(sql, args).fetchall()
        return {
            "view": "legacy",
            "field": field,
            "op": op,
            "comparable": False,
            "note": "legacy view orders raw text only; no type or range comparison is promised",
            "rows": [dict(r) for r in rows],
        }

    rdy = readiness(store, batch_id=batch_id)
    status = {f["field"]: f for f in rdy["fields"]}
    if field not in status:
        raise NotComparable(f"field={field} has no typed values in scope")
    finfo = status[field]
    if not finfo["comparable"]:
        raise NotComparable(f"field={field} is not comparable: {finfo['reason']}")
    if op not in COMPARABLE_OPS.get(finfo["kind"], []):
        raise NotComparable(f"op={op!r} not supported for type {finfo['kind']}")

    sql = ("SELECT batch_id, row_index, raw_text, typed_value AS value, typed_kind "
           "FROM record_values WHERE field=? AND parse_state='ok'")
    args = [field]
    if batch_id:
        sql += " AND batch_id=?"
        args.append(batch_id)
    records = [dict(r) for r in store._conn.execute(sql, args).fetchall()]

    if op == "sum":
        total = sum((Decimal(r["value"]) for r in records), Decimal("0"))
        return {"view": "typed", "field": field, "op": op, "value": f"{total:.2f}",
                "kind": finfo["kind"], "comparable": True,
                "committed": rdy["typed_promise"], "rows": records}

    if value is not None:
        for part in value.split(",") if op in ("in", "between") else [value]:
            _check_kind(finfo["kind"], part.strip())

    return {
        "view": "typed",
        "field": field,
        "op": op,
        "kind": finfo["kind"],
        "comparable": True,
        # Comparable rows, but maybe not yet the global type promise.
        "committed": rdy["typed_promise"],
        "rows": _apply_predicate(records, finfo["kind"], op, value),
    }


def _apply_predicate(records: list[dict], kind: str, op: str, value: Optional[str]) -> list[dict]:
    if op == "in":
        if not value:
            raise NotComparable("op='in' requires a value (comma separated)")
        wanted = {v.strip().casefold() for v in value.split(",")}
        return [r for r in records if (r["value"] or "").casefold() in wanted]
    key = _sort_key(kind)
    if op == "between":
        if not value or "," not in value:
            raise NotComparable("op='between' requires 'low,high'")
        low_raw, high_raw = (p.strip() for p in value.split(",", 1))
        low, high = key(low_raw), key(high_raw)
        return [r for r in records if low <= key(r["value"]) <= high]
    if op == "!=":
        if value is None:
            raise NotComparable("op='!=' requires a value")
        target = key(value)
        return [r for r in records if key(r["value"]) != target]
    if value is None:
        raise NotComparable(f"op={op!r} requires a value")
    target = key(value)
    cmp = {
        "<": lambda x: x < target,
        "<=": lambda x: x <= target,
        "=": lambda x: x == target,
        ">=": lambda x: x >= target,
        ">": lambda x: x > target,
    }
    if op not in cmp:
        raise NotComparable(f"unsupported op {op!r}")
    return [r for r in records if cmp[op](key(r["value"]))]
