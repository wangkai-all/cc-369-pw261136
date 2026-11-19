"""Unreserved: unparseable values remain visible in `failures` with a location.

Every parse failure is persisted with `batch=… row=… field=…`, the raw text and
the rule version, in all three policies. `preserve_dual` also keeps raw_text in
record_values with a NULL typed shadow, so nothing is silently blanked.
"""

from __future__ import annotations

import pytest

from migrator import rules as rulesmod
from migrator.engine import StaleWorkerError, commit_projection, compute_projection, project_batch, replay_batch
from migrator.ingest import receive_batch
from migrator.query import NotComparable, query, readiness
from migrator.store import Store
from migrator.web.console import DEFAULT_RULE


def _store():
    s = Store(":memory:")
    v = s.publish_rule(rulesmod.normalize(DEFAULT_RULE), "initial", "2024-01-01T00:00:00+00:00")
    s.activate_query_version("typed", v, "2024-01-01T00:00:00+00:00")
    return s, v


def _batch(s, rows, bid="b1", fields=("ordered_on", "total", "status")):
    return receive_batch(s, rows, source="test", fields=list(fields), batch_id=bid)


GOOD = [
    {"ordered_on": "2024-03-01", "total": "$1,204.50", "status": "paid"},
    {"ordered_on": "03/02/2024", "total": "89.9", "status": "PEND"},
]
BAD = [
    {"ordered_on": "2024-13-40", "total": "1,204", "status": "paid"},
    {"ordered_on": "2024-03-04", "total": "12.345", "status": "weird"},
]


def test_failures_keep_raw_value_and_location():
    s, v = _store()
    _batch(s, BAD)
    res = project_batch(s, "b1")
    assert res.committed
    fails = {(f["row_index"], f["field"]): f for f in res.failures}
    f = fails[(0, "ordered_on")]
    assert f["raw"] == "2024-13-40"
    assert f["location"] == "batch=b1 row=0 field=ordered_on"
    # raw text still present in the failure record, typed shadow NULL on the
    # rows that were projected (quarantine keeps only clean rows in values)
    fails = {(f["row_index"], f["field"]): f for f in s.failures("b1")}
    assert fails[(1, "total")]["raw"] == "12.345"
    assert fails[(1, "total")]["reason"].startswith("unrecognized money")
    vals = {(r["row_index"], r["field"]): r for r in s.values_for_batch("b1")}
    assert vals == {}  # every row was dirty, so none is projected under quarantine
    assert s._conn.execute("SELECT status FROM batches WHERE batch_id='b1'").fetchone()[0] == "quarantined"


def test_quarantine_drops_dirty_rows_but_records_them():
    s, v = _store()
    _batch(s, GOOD + BAD)
    res = project_batch(s, "b1")
    assert res.clean_rows == 2 and res.dirty_rows == 2
    assert s._conn.execute("SELECT status FROM batches WHERE batch_id='b1'").fetchone()["status"] == "quarantined"
    # only clean rows got record_values
    rows = {r["row_index"] for r in s.values_for_batch("b1")}
    assert rows == {0, 1}
    # dirty rows are still inspectable with exact positions, raw text intact
    stored = {(f["row_index"], f["field"]): f for f in s.failures("b1")}
    assert stored[(2, "ordered_on")]["raw"] == "2024-13-40"
    assert stored[(3, "total")]["raw"] == "12.345"
    assert stored[(3, "status")]["raw"] == "weird"
    locs = {f"{f['batch_id']} row={f['row_index']} field={f['field']}" for f in stored.values()}
    assert "b1 row=3 field=status" in locs
    # untouched raw rows remain in records
    assert s.batch_rows("b1")[2]["ordered_on"] == "2024-13-40"


def test_reject_batch_writes_no_values_but_keeps_failures():
    s, v = _store()
    cfg = rulesmod.normalize({**DEFAULT_RULE, "on_failure": "reject_batch"})
    v2 = s.publish_rule(cfg, "strict", "2024-01-02T00:00:00+00:00")
    _batch(s, GOOD + BAD)
    res = project_batch(s, "b1", rule_version=v2)
    assert not res.committed
    assert "reject_batch" in res.reason
    assert s.values_for_batch("b1") == []
    assert len(s.failures("b1")) >= 2
    assert s.query_version()["rule_version"] == v  # untouched


def test_preserve_dual_projects_all_rows():
    s, _ = _store()
    cfg = rulesmod.normalize({**DEFAULT_RULE, "on_failure": "preserve_dual"})
    v2 = s.publish_rule(cfg, "dual", "2024-01-02T00:00:00+00:00")
    _batch(s, GOOD + BAD)
    res = project_batch(s, "b1", rule_version=v2)
    assert res.committed and res.dirty_rows == 2
    rows = {r["row_index"] for r in s.values_for_batch("b1")}
    assert rows == {0, 1, 2, 3}
    failed = [r for r in s.values_for_batch("b1") if r["parse_state"] == "failed"]
    assert failed and all(r["raw_text"] for r in failed)


def test_readiness_blocks_comparison_until_clean():
    s, _ = _store()
    _batch(s, GOOD + BAD)
    project_batch(s, "b1")
    rdy = readiness(s)
    assert rdy["typed_promise"] is False
    field = {f["field"]: f for f in rdy["fields"]}["total"]
    assert field["comparable"] is False
    assert "failed to parse" in field["reason"]
    with pytest.raises(NotComparable):
        query(s, "typed", "total", ">", "100")


def test_typed_query_compares_after_clean_replay():
    s, _ = _store()
    _batch(s, GOOD + BAD)
    project_batch(s, "b1")
    # fix the rule: widen money decimals, add an alias, then replay
    cfg = rulesmod.normalize({
        "fields": {
            "ordered_on": {"type": "date"},
            "total": {"type": "money"},
            "status": {"type": "enum", "allowed": ["paid", "pending", "void", "weird"], "aliases": {"PEND": "pending"}},
        },
        "on_failure": "quarantine_row",
    })
    s.publish_rule(cfg, "widened", "2024-01-02T00:00:00+00:00")
    # still invalid dates -> stays non-comparable for ordered_on
    replay_batch(s, "b1")
    with pytest.raises(NotComparable):
        query(s, "typed", "ordered_on", ">", "2024-01-01")
    # now a rule accepting the remaining date via a corrected raw value
    _batch(s, [{"ordered_on": "2024-03-05", "total": "1204", "status": "weird"}], bid="b2")
    project_batch(s, "b2")
    # b1 still dirty -> the global type promise is withheld
    assert readiness(s)["typed_promise"] is False
    # but callers can still compare b2's rows in isolation, and the response
    # says whether that is merely comparable or already a promise
    res = query(s, "typed", "total", ">", "100", batch_id="b2")
    assert res["comparable"] is True
    assert res["committed"] is False
    assert [r["raw_text"] for r in res["rows"]] == ["1204"]


def test_stale_worker_cannot_overwrite_newer_replay():
    s, v1 = _store()
    _batch(s, GOOD, bid="b1")
    project_batch(s, "b1", rule_version=v1)
    # a late live batch arrives under v1
    _batch(s, GOOD + BAD, bid="b2")
    # slow worker computes b2 under v1 ...
    slow = compute_projection(s, "b2", rule_version=v1)
    # ... operator publishes v2 and replays every batch under it
    cfg = rulesmod.normalize({**DEFAULT_RULE, "on_failure": "preserve_dual"})
    v2 = s.publish_rule(cfg, "dual", "2024-01-02T00:00:00+00:00")
    replay_batch(s, "b1", rule_version=v2)
    replay_batch(s, "b2", rule_version=v2)
    before = [dict(r) for r in s.values_for_batch("b2")]
    # late worker commits -> refused, state unchanged
    with pytest.raises(StaleWorkerError) as ei:
        commit_projection(s, slow)
    assert "already at rule v" in str(ei.value)
    after = [dict(r) for r in s.values_for_batch("b2")]
    assert before == after
    assert s._conn.execute("SELECT rule_version FROM batches WHERE batch_id='b2'").fetchone()[0] == v2


def test_worker_claims_new_rule_for_staged_batches_only():
    s, v1 = _store()
    _batch(s, GOOD, bid="b1")
    project_batch(s, "b1", rule_version=v1)  # b1 is now projected under v1
    _batch(s, GOOD, bid="b2")                # b2 still staged
    cfg = rulesmod.normalize({**DEFAULT_RULE, "on_failure": "preserve_dual"})
    v2 = s.publish_rule(cfg, "dual", "2024-01-02T00:00:00+00:00")

    from migrator.engine import claim_staged_version
    # staged batch can be claimed for v2
    assert claim_staged_version(s, "b2", v2) is True
    # already-projected batch is never moved by a worker claim
    assert claim_staged_version(s, "b1", v2) is False
    assert s._conn.execute("SELECT rule_version FROM batches WHERE batch_id='b1'").fetchone()[0] == v1
    # after its own projection b2 reflects v2
    res = project_batch(s, "b2", rule_version=v2)
    assert res.committed and res.policy == "preserve_dual"


def test_live_batch_arrives_during_replay():
    s, v1 = _store()
    _batch(s, GOOD, bid="b1")
    project_batch(s, "b1")
    # replay b1 while a new live batch is staged
    _batch(s, GOOD, bid="b2")
    replay_batch(s, "b1")
    # staging did not block and b2 is untouched/staged
    st = s._conn.execute("SELECT status FROM batches WHERE batch_id='b2'").fetchone()["status"]
    assert st == "staged"
    res = project_batch(s, "b2")
    assert res.committed


def test_preview_reports_failures_without_writing():
    s, _ = _store()
    _batch(s, GOOD + BAD)
    cfg = rulesmod.normalize(DEFAULT_RULE)
    pv = rulesmod.validate_against_rows(cfg, s.batch_rows("b1"))
    assert pv["rows"] == 4 and pv["clean_rows"] == 2 and pv["dirty_rows"] == 2
    assert pv["sample_failures"][0]["location"].startswith("batch=<preview>")
    assert s.values_for_batch("b1") == []  # nothing written


def test_invalid_rule_is_refused_with_path():
    with pytest.raises(rulesmod.RuleError) as e:
        rulesmod.normalize({"fields": {"total": {"type": "currency"}}})
    assert "rule.fields.total.type" in str(e.value)
    with pytest.raises(rulesmod.RuleError) as e:
        rulesmod.normalize({"fields": {"s": {"type": "enum", "allowed": ["a"], "aliases": {"x": "b"}}}})
    assert "aliases" in str(e.value)


def test_enum_aliases_and_case_are_controlled():
    s, _ = _store()
    cfg = rulesmod.normalize({
        "fields": {
            "ordered_on": {"type": "date"},
            "total": {"type": "money"},
            "status": {"type": "enum", "allowed": ["Paid", "pending"],
                       "aliases": {"settled": "paid"}},
        },
        "on_failure": "quarantine_row",
    })
    v = s.publish_rule(cfg, "case", "2024-01-02T00:00:00+00:00")
    _batch(s, [
        {"ordered_on": "2024-03-01", "total": "1.00", "status": "PAID"},
        {"ordered_on": "2024-03-02", "total": "2.00", "status": "settled"},
    ])
    project_batch(s, "b1", rule_version=v)
    vals = [r["typed_value"] for r in s.values_for_batch("b1") if r["field"] == "status"]
    assert vals == ["paid", "paid"]


def test_date_formats_are_exact_not_fuzzy():
    from migrator.types import ParseContext, parse_date
    ctx = ParseContext(batch_id="b", row_index=0, field="ordered_on", rule_version=1)
    assert parse_date("2024-03-01", ctx).as_json() == "2024-03-01"
    assert parse_date("01.03.2024", ctx).as_json() == "2024-03-01"   # d.m.Y
    assert parse_date("01/03/2024", ctx).as_json() == "2024-01-03"   # m/d/Y
    assert parse_date("2024-02-30", ctx).raw == "2024-02-30"          # invalid calendar day
    assert parse_date("tomorrow", ctx).location == "batch=b row=0 field=ordered_on"


def test_money_is_fixed_point_not_float():
    from decimal import Decimal
    from migrator.types import parse_money, ParseContext
    ctx = ParseContext(batch_id="b", row_index=0, field="total", rule_version=1)
    assert parse_money("$1,204.5", ctx).value == Decimal("1204.50")
    assert parse_money("(12.00)", ctx).value == Decimal("-12.00")
    assert parse_money("0.1", ctx).value + parse_money("0.2", ctx).value == Decimal("0.3")
    assert parse_money("12.345", ctx).raw == "12.345"


def test_console_flow_refuses_bad_json_and_reports_position():
    s = Store(":memory:")
    from migrator.web.console import bootstrap, handle
    bootstrap(s)
    status, body = handle(s, "POST", "/publish", "config=%7Bbad+json")
    assert status == 200 and "refused" in body
    assert "line 1" in body
