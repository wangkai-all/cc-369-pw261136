"""Conversion rules: validation, normalization and change impact analysis.

A rule is plain JSON so the operator UI can edit and version it:

    {
      "fields": {
        "ordered_on": {"type": "date"},
        "total":      {"type": "money"},
        "status":     {"type": "enum",
                       "allowed": ["paid", "pending", "void"],
                       "aliases": {"PEND": "pending", "settled": "paid"}}
      },
      "on_failure": "quarantine_row" | "reject_batch" | "preserve_dual"
    }

`on_failure` selects how unparseable values are handled:

  reject_batch    whole batch is refused; no typed values are committed.
  quarantine_row  bad rows are held back, clean rows are projected.
  preserve_dual   every row is projected; failed fields keep raw text and
                  leave the typed shadow NULL.

Dual columns are retained in all three modes, so switching modes never loses
raw text.
"""

from __future__ import annotations

import copy

SUPPORTED_TYPES = ("date", "money", "enum")
POLICIES = ("quarantine_row", "reject_batch", "preserve_dual")
DEFAULT_POLICY = "quarantine_row"


class RuleError(ValueError):
    pass


def normalize(config: dict) -> dict:
    """Validate and canonicalize a rule config. Raises RuleError with a path."""
    if not isinstance(config, dict):
        raise RuleError("rule: expected an object")
    fields = config.get("fields")
    if not isinstance(fields, dict) or not fields:
        raise RuleError("rule.fields: expected a non-empty object")
    out_fields = {}
    for name, spec in fields.items():
        path = f"rule.fields.{name}"
        if not isinstance(spec, dict):
            raise RuleError(f"{path}: expected an object")
        ftype = spec.get("type")
        if ftype not in SUPPORTED_TYPES:
            raise RuleError(f"{path}.type: expected one of {list(SUPPORTED_TYPES)}, got {ftype!r}")
        clean = {"type": ftype}
        if ftype == "enum":
            allowed = spec.get("allowed")
            if not isinstance(allowed, list) or not allowed:
                raise RuleError(f"{path}.allowed: expected a non-empty list")
            norm_allowed = []
            for i, v in enumerate(allowed):
                if not isinstance(v, str) or not v.strip():
                    raise RuleError(f"{path}.allowed[{i}]: expected a non-empty string")
                norm_allowed.append(v.strip().casefold())
            if len(set(norm_allowed)) != len(norm_allowed):
                raise RuleError(f"{path}.allowed: duplicate members after case folding")
            clean["allowed"] = norm_allowed
            aliases = spec.get("aliases", {})
            if not isinstance(aliases, dict):
                raise RuleError(f"{path}.aliases: expected an object")
            norm_aliases = {}
            for src, dst in aliases.items():
                if not isinstance(dst, str) or dst.strip().casefold() not in norm_allowed:
                    raise RuleError(
                        f"{path}.aliases[{src!r}]: target {dst!r} is not in allowed set"
                    )
                norm_aliases[str(src).casefold()] = dst.strip().casefold()
            clean["aliases"] = norm_aliases
        elif ftype == "money":
            if spec.get("currency") not in (None, "auto"):
                raise RuleError(f"{path}.currency: only 'auto' is supported")
        out_fields[name] = clean

    policy = config.get("on_failure", DEFAULT_POLICY)
    if policy not in POLICIES:
        raise RuleError(f"rule.on_failure: expected one of {list(POLICIES)}, got {policy!r}")
    return {"fields": out_fields, "on_failure": policy}


def validate_against_rows(config: dict, rows: list[dict], sample: int = 25) -> dict:
    """Dry-run a rule over incoming rows without writing anything.

    Returns a preview: per-field counts plus up to `sample` failure objects.
    This is what the operator page shows before publishing a rule revision.
    """
    from .engine import parse_row

    failures = []
    ok = 0
    per_field: dict[str, dict[str, int]] = {
        name: {"ok": 0, "failed": 0} for name in config["fields"]
    }
    for row_index, row in enumerate(rows):
        _values, row_failures = parse_row(row, config, batch_id="<preview>", row_index=row_index, rule_version=0)
        failed_fields = {f.field for f in row_failures}
        if row_failures:
            for f in row_failures:
                per_field[f.field]["failed"] += 1
                if len(failures) < sample:
                    failures.append(f.as_json())
        else:
            ok += 1
        for name in config["fields"]:
            if name not in failed_fields:
                per_field[name]["ok"] += 1
    return {
        "rows": len(rows),
        "clean_rows": ok,
        "dirty_rows": len(rows) - ok,
        "per_field": per_field,
        "sample_failures": failures,
        "sample_truncated": len(failures) >= sample and len(rows) > sample,
        "on_failure": config["on_failure"],
    }


def diff(old: dict, new: dict) -> dict:
    """Human/UI-facing difference between two normalized rule configs."""
    added = sorted(set(new["fields"]) - set(old["fields"]))
    removed = sorted(set(old["fields"]) - set(new["fields"]))
    changed = sorted(
        name for name in set(old["fields"]) & set(new["fields"])
        if old["fields"][name] != new["fields"][name]
    )
    return {
        "added_fields": added,
        "removed_fields": removed,
        "changed_fields": changed,
        "policy": {"from": old["on_failure"], "to": new["on_failure"]},
        "requires_replay": bool(added or removed or changed or old["on_failure"] != new["on_failure"]),
    }


def deepcopy(config: dict) -> dict:
    return copy.deepcopy(config)
