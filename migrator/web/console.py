"""Operator console (stdlib HTTP, server-rendered HTML).

Flow on one page:
  1. Preview   -- dry-run a rule revision against the last ingested batch.
  2. Publish   -- normalize + store a new rule version (refused if invalid).
  3. Replay    -- re-derive affected batches from raw rows.
  4. Activate  -- point the typed query version at a rule version.

Every mutation refuses to proceed when the rule is invalid, and replay under a
newer rule bumps the batch's stored rule version so in-flight workers under
the old rule fail their commit instead of overwriting the result.
"""

from __future__ import annotations

import datetime as _dt
import html
import json
from urllib.parse import parse_qs, urlparse

from .. import rules as rulesmod
from ..engine import StaleWorkerError, project_batch, replay_batch
from ..ingest import receive_batch
from ..query import NotComparable, readiness
from ..store import Store

SAMPLE_ROWS = [
    {"ordered_on": "2024-03-01", "total": "$1,204.50", "status": "paid"},
    {"ordered_on": "03/02/2024", "total": "89.9", "status": "PEND"},
    {"ordered_on": "2024-13-40", "total": "1,204", "status": "paid"},
    {"ordered_on": "2024/03/03", "total": "(12.00)", "status": "settled"},
    {"ordered_on": "not a date", "total": "12.345", "status": "weird"},
]

DEFAULT_RULE = {
    "fields": {
        "ordered_on": {"type": "date"},
        "total": {"type": "money"},
        "status": {
            "type": "enum",
            "allowed": ["paid", "pending", "void"],
            "aliases": {"PEND": "pending", "settled": "paid"},
        },
    },
    "on_failure": "quarantine_row",
}

DEFAULT_POLICY = DEFAULT_RULE["on_failure"]


def _now() -> str:
    return _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="seconds")


def bootstrap(store: Store) -> dict:
    """Seed a rule, a few batches and a projection so the console has data."""
    v1 = store.publish_rule(rulesmod.normalize(DEFAULT_RULE), "initial", _now())
    fields = list(DEFAULT_RULE["fields"])
    batches = [
        ("b-live-1", SAMPLE_ROWS[:3], "nightly-etl"),
        ("b-live-2", SAMPLE_ROWS[3:], "nightly-etl"),
        ("b-late", [{"ordered_on": "31.12.2024", "total": "EUR 5", "status": "VOID"}], "backfill"),
    ]
    for bid, rows, src in batches:
        receive_batch(store, rows, source=src, fields=fields, batch_id=bid)
    for bid, _, _ in batches:
        project_batch(store, bid, rule_version=v1, actor="bootstrap")
    store.activate_query_version("typed", v1, _now())
    return {"rule_version": v1}


# --------------------------------------------------------------------------
# rendering


def _page(store: Store, notice: str = "", error: str = "",
          preview: dict | None = None, form_config: dict | None = None) -> str:
    active = store.current_rule()
    if form_config is None:
        version = int(active["version"])
        label = active["label"]
        config = json.loads(active["config_json"])
    else:
        version = int(active["version"])
        label = f"{active['label']} (unsaved edit)"
        config = dict(form_config)
        if "on_failure" not in config:
            config["on_failure"] = DEFAULT_POLICY
    rdy = readiness(store)
    # The typed view is a commitment, so its pointer must agree with the rule
    # that produced the data. Only the exact same rule version counts.
    qv_version = rdy["query_version"]["rule_version"]
    if qv_version != version:
        rdy["typed_note"] = (
            f"typed query version v{qv_version} does not match active rule v{version}: "
            f"nothing new is promised until replay + activate"
        )
    else:
        rdy["typed_note"] = ""
    batch_rows = store._conn.execute(
        """SELECT b.batch_id, b.source, b.status, b.rule_version, b.received_at,
                  (SELECT COUNT(*) FROM records r WHERE r.batch_id=b.batch_id) AS n
           FROM batches b ORDER BY b.received_at, b.batch_id"""
    ).fetchall()
    failures = store.failures(unresolved_only=True)
    audit = store._conn.execute("SELECT * FROM audit ORDER BY id DESC LIMIT 12").fetchall()

    def esc(x):
        return html.escape(str(x))

    parts = [
        "<!doctype html><meta charset=utf-8><title>Typed migration console</title>",
        "<style>body{font:14px/1.45 system-ui;margin:2rem auto;max-width:1100px;color:#111}"
        "table{border-collapse:collapse;width:100%;margin:.6rem 0 1.4rem}"
        "th,td{border:1px solid #ccd;padding:4px 8px;text-align:left;vertical-align:top}"
        "th{background:#eef}code{background:#f4f4f8;padding:1px 4px}"
        ".bad{background:#fde8e8}.ok{background:#e8f7ec}.warn{background:#fff6d8}"
        "form{display:inline-block;margin:0 .4rem .4rem 0}textarea{width:100%;height:14rem;font-family:ui-monospace}"
        "fieldset{margin:1rem 0}</style>",
        "<h1>Typed migration console</h1>",
    ]
    if notice:
        parts.append(f"<p class=ok><b>{esc(notice)}</b></p>")
    if error:
        parts.append(f"<p class=bad><b>refused:</b> {esc(error)}</p>")

    parts.append(
        f"<p>active rule: <b>v{version}</b> ({esc(label)}) "
        f"&middot; policy <code>{esc(config['on_failure'])}</code> "
        f"&middot; query version <code>typed</code> &rarr; "
        f"v{rdy['query_version']['rule_version']} "
        f"&middot; typed promise: <b>{'yes' if rdy['typed_promise'] else 'no'}</b></p>"
    )
    if rdy["typed_note"]:
        parts.append(f"<p class=warn><b>{esc(rdy['typed_note'])}</b></p>")

    # -- readiness
    parts.append("<h2>What callers may compare</h2><table><tr><th>field<th>kind<th>typed<th>failed<th>comparable<th>promised<th>reason")
    for f in rdy["fields"]:
        cls = "ok" if f["committed"] else ("bad" if f["failed_rows"] else "warn")
        parts.append(
            f"<tr class={cls}><td>{esc(f['field'])}<td>{esc(f['kind'])}"
            f"<td>{f['typed_rows']}<td>{f['failed_rows']}"
            f"<td>{'yes' if f['comparable'] else 'no'}"
            f"<td>{'yes' if f['committed'] else 'no'}<td>{esc(f['reason'])}</tr>"
        )
    parts.append("</table>")
    parts.append(
        "<p>comparable operators: "
        + ", ".join(f"<code>{esc(k)}</code>: {esc(', '.join(v))}" for k, v in rdy["comparable_operators"].items())
        + "</p>"
    )

    # -- batches
    parts.append("<h2>Batches</h2><table><tr><th>batch<th>source<th>rows<th>status<th>rule<th>received")
    for b in batch_rows:
        cls = {"projected": "ok", "quarantined": "warn", "rejected": "bad", "staged": ""}[b["status"]]
        parts.append(
            f"<tr class={cls}><td>{esc(b['batch_id'])}<td>{esc(b['source'])}<td>{b['n']}"
            f"<td>{esc(b['status'])}<td>v{b['rule_version']}<td>{esc(b['received_at'])}</tr>"
        )
    parts.append("</table>")

    parts.append("<h2>Unresolved failures</h2>")
    if failures:
        parts.append("<table><tr><th>location<th>raw<th>reason<th>rule")
        for f in failures:
            parts.append(
                f"<tr class=bad><td><code>batch={esc(f['batch_id'])} row={f['row_index']} "
                f"field={esc(f['field'])}</code><td><code>{esc(f['raw'])}</code>"
                f"<td>{esc(f['reason'])}<td>v{f['rule_version']}</tr>"
            )
        parts.append("</table>")
    else:
        parts.append("<p class=ok>none &mdash; every value typed under the active rule.</p>")

    # -- preview result
    if preview:
        parts.append("<h2>Preview (nothing written)</h2>")
        parts.append(
            f"<p>{preview['rows']} rows, {preview['clean_rows']} clean, "
            f"{preview['dirty_rows']} dirty, policy <code>{esc(preview['on_failure'])}</code></p>"
        )
        parts.append("<table><tr><th>field<th>ok<th>failed")
        for name, c in preview["per_field"].items():
            parts.append(f"<tr><td>{esc(name)}<td>{c['ok']}<td>{c['failed']}</tr>")
        parts.append("</table>")
        if preview["sample_failures"]:
            parts.append("<table><tr><th>location<th>raw<th>reason")
            for f in preview["sample_failures"]:
                parts.append(
                    f"<tr class=bad><td><code>{esc(f['location'])}</code>"
                    f"<td><code>{esc(f['raw'])}</code><td>{esc(f['reason'])}</tr>"
                )
            parts.append("</table>")

    # -- forms
    parts.append("<h2>1 &middot; Revise the rule (JSON)</h2>")
    parts.append(
        "<form method=post action=/preview>"
        f"<textarea name=config>{esc(json.dumps(config, indent=2))}</textarea>"
        "<button>Preview against newest batch</button></form>"
        "<form method=post action=/publish>"
        f"<textarea name=config>{esc(json.dumps(config, indent=2))}</textarea>"
        "<button>Publish as new rule version</button></form>"
    )

    parts.append("<h2>2 &middot; Replay / activate</h2>")
    for b in batch_rows:
        parts.append(
            f"<form method=post action=/replay><input type=hidden name=batch_id value={esc(b['batch_id'])}>"
            f"<button>Replay {esc(b['batch_id'])} under v{version}</button></form>"
        )
    parts.append(
        f"<form method=post action=/activate><input type=hidden name=version value={version}>"
        f"<button>Commit query version typed &rarr; v{version}</button></form>"
    )

    parts.append("<h2>3 &middot; Ingest a live batch</h2>")
    parts.append(
        "<form method=post action=/ingest><textarea name=rows>"
        + esc(json.dumps(SAMPLE_ROWS[1:3], indent=2))
        + "</textarea><button>Ingest + project</button></form>"
    )

    if audit:
        parts.append("<h2>Audit trail</h2><table><tr><th>at<th>actor<th>action<th>payload")
        for a in audit:
            parts.append(
                f"<tr><td>{esc(a['at'])}<td>{esc(a['actor'])}<td><code>{esc(a['action'])}</code>"
                f"<td><code>{esc(a['payload'])}</code></tr>"
            )
        parts.append("</table>")
    return "\n".join(parts)


def _parse_config(raw: str) -> dict:
    try:
        cfg = json.loads(raw)
    except json.JSONDecodeError as e:
        raise rulesmod.RuleError(f"config: invalid JSON at line {e.lineno} col {e.colno}: {e.msg}")
    return rulesmod.normalize(cfg)


def _latest_batch(store: Store) -> str | None:
    row = store._conn.execute(
        "SELECT batch_id FROM batches ORDER BY received_at DESC, batch_id DESC LIMIT 1"
    ).fetchone()
    return row["batch_id"] if row else None


def handle(store: Store, method: str, path: str, body: str) -> tuple[int, str]:
    form = {k: v[0] for k, v in parse_qs(body, keep_blank_values=True).items()}
    page_ok, page_err, preview = "", "", None
    edit: dict | None = None
    try:
        if path == "/" and method == "GET":
            pass
        elif path == "/preview" and method == "POST":
            cfg = _parse_config(form.get("config", ""))
            bid = _latest_batch(store)
            if not bid:
                raise rulesmod.RuleError("no batch to preview against")
            rows = store.batch_rows(bid)
            preview = rulesmod.validate_against_rows(cfg, rows)
            preview["batch_id"] = bid
            page_ok = f"preview computed over batch {bid}"
        elif path == "/publish" and method == "POST":
            cfg = _parse_config(form.get("config", ""))
            old = json.loads(store.current_rule()["config_json"])
            d = rulesmod.diff(old, cfg)
            v = store.publish_rule(cfg, label=f"revised@{_now()}", now=_now())
            store.log(_now(), "operator", "rule.published", {"version": v, "diff": d})
            page_ok = f"published rule v{v}; replay affected batches, then activate"
            if d["requires_replay"]:
                page_ok += f" (diff: {json.dumps(d, sort_keys=True)})"
        elif path == "/replay" and method == "POST":
            bid = form.get("batch_id", "")
            res = replay_batch(store, bid, rule_version=int(store.current_rule()["version"]), actor="operator")
            store.log(_now(), "operator", "batch.replayed",
                      {"batch_id": bid, "rule_version": res.rule_version, "committed": res.committed})
            page_ok = f"replayed {bid} under v{res.rule_version}: {res.clean_rows} clean / {res.dirty_rows} dirty"
        elif path == "/activate" and method == "POST":
            v = int(form.get("version", "0"))
            store.get_rule(v)
            store.activate_query_version("typed", v, _now())
            store.log(_now(), "operator", "query_version.activated", {"version": v})
            page_ok = f"typed query version now points at rule v{v}"
        elif path == "/ingest" and method == "POST":
            try:
                rows = json.loads(form.get("rows", "[]"))
            except json.JSONDecodeError as e:
                raise rulesmod.RuleError(f"rows: invalid JSON at line {e.lineno} col {e.colno}: {e.msg}")
            got = receive_batch(store, rows, source="console", fields=list(json.loads(store.current_rule()["config_json"])["fields"]))
            res = project_batch(store, got["batch_id"])
            page_ok = f"ingested {got['batch_id']}: {res.clean_rows} clean / {res.dirty_rows} dirty"
        else:
            return 404, _page(store, error=f"no route {method} {path}")
    except (rulesmod.RuleError, NotComparable, StaleWorkerError, KeyError, ValueError) as e:
        page_err = str(e)
        # If the operator submitted a config, echo their unsaved edit back
        # rather than silently replacing it with the active rule.
        raw_cfg = form.get("config")
        if raw_cfg:
            try:
                edit = json.loads(raw_cfg)
            except json.JSONDecodeError:
                edit = None
    return 200, _page(store, notice=page_ok, error=page_err, preview=preview, form_config=edit)


def serve(db_path: str = "migrator.db", host: str = "127.0.0.1", port: int = 8765, seed: bool = True):
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    store = Store(db_path)
    info = bootstrap(store) if seed and not store.batches() else {"rule_version": None}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self._dispatch("GET", "")

        def do_POST(self):
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length).decode("utf-8") if length else ""
            self._dispatch("POST", body)

        def _dispatch(self, method, body):
            u = urlparse(self.path)
            status, out = handle(store, method, u.path, body)
            data = out.encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, *a):  # keep console output for real results
            pass

    httpd = ThreadingHTTPServer((host, port), Handler)
    print(json.dumps({"listening": f"http://{host}:{port}/", **info}))
    httpd.serve_forever()


if __name__ == "__main__":
    serve()
