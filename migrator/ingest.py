"""Ingest adapter: the only component that accepts live batches.

It writes the raw rows to `records` and marks the batch `staged`. It never
parses and never blocks on migration workers: a live batch arriving while a
replay is running is simply staged for the next projection pass.
"""

from __future__ import annotations

import datetime as _dt
import uuid

from .engine import fingerprint
from .store import Store


class IngestError(ValueError):
    pass


def _now() -> str:
    return _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="seconds")


def normalize_rows(rows: list[dict], fields: list[str]) -> list[dict]:
    """Coerce the transport envelope into rows of loose text.

    Missing fields become None (kept as a parse-time failure, never an empty
    string). Non-string scalars are stringified for the legacy column but the
    original type is preserved inside raw_json so a rule can reject it.
    """
    out = []
    for i, row in enumerate(rows):
        if not isinstance(row, dict):
            raise IngestError(f"rows[{i}]: expected object, got {type(row).__name__}")
        norm = {}
        for f in fields:
            if f not in row:
                norm[f] = None
                continue
            v = row[f]
            if v is None:
                norm[f] = None
            elif isinstance(v, str):
                norm[f] = v
            else:
                norm[f] = v  # keep native type; parse_row flags it with a location
        out.append(norm)
    return out


def receive_batch(store: Store, rows: list[dict], source: str, fields: list[str],
                  batch_id: str | None = None) -> dict:
    if not rows:
        raise IngestError("batch has no rows")
    batch_id = batch_id or f"b-{uuid.uuid4().hex[:12]}"
    rule = store.current_rule()
    normalized = normalize_rows(rows, fields)
    store.stage_batch(batch_id, normalized, source, int(rule["version"]), _now())
    store.log(
        _now(), source, "ingest.staged",
        {"batch_id": batch_id, "rows": len(normalized), "fingerprint": fingerprint(normalized)},
    )
    return {
        "batch_id": batch_id,
        "rows": len(normalized),
        "status": "staged",
        "rule_version": int(rule["version"]),
        "note": "staged only; typed values appear after projection",
    }
