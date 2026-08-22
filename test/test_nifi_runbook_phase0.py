#!/usr/bin/env python3
"""Phase 0 tests: NiFi runbook schema, fixtures, deterministic fallback."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_empty_runbook_valid() -> None:
    from ratatoskr.nifi.runbook import empty_runbook, is_valid_runbook, validate_runbook

    rb = empty_runbook()
    assert is_valid_runbook(rb)
    assert validate_runbook(rb) == []
    assert rb["mode"] == "fallback"
    assert rb["schema_version"] == "1"


def test_validate_rejects_bad_mode_and_mutations() -> None:
    from ratatoskr.nifi.runbook import (
        empty_runbook,
        validate_runbook,
        validate_runbook_event,
        wrap_runbook_event,
    )

    rb = empty_runbook()
    rb["mode"] = "magic"
    assert "mode" in ";".join(validate_runbook(rb))

    event = wrap_runbook_event(empty_runbook())
    assert validate_runbook_event(event) == []
    event["mutations"] = [{"op": "start_processor"}]
    errs = validate_runbook_event(event)
    assert any("mutations" in e for e in errs)


def test_fixtures_load_and_list() -> None:
    from ratatoskr.nifi.runbook import list_fixture_ids, load_fixture

    ids = list_fixture_ids()
    assert "stop-generate" in ids
    assert "invalid-log" in ids
    assert "queue-backlog" in ids
    assert "stop-consume" in ids
    for fid in ids:
        ev = load_fixture(fid)
        assert ev["agent"] == "workflow_nifi_monitor"
        assert "classification" in ev
        assert "health" in ev


def test_fallback_healthy() -> None:
    from ratatoskr.nifi.runbook import (
        fallback_runbook,
        is_valid_runbook_event,
        load_fixture,
    )

    out = fallback_runbook(load_fixture("healthy"))
    assert is_valid_runbook_event(out)
    assert out["mutations"] == []
    assert out["runbook"]["mode"] == "fallback"
    assert "healthy" in out["runbook"]["headline"].lower()


def test_fallback_stop_generate_cites_heal_plan() -> None:
    from ratatoskr.nifi.runbook import fallback_runbook, is_valid_runbook_event, load_fixture

    out = fallback_runbook(load_fixture("stop-generate"))
    assert is_valid_runbook_event(out)
    rb = out["runbook"]
    assert "STOPPED" in rb["headline"] or "stop" in rb["headline"].lower()
    assert any("STOPPED" in c["cause"] or "STOPPED" in str(c.get("evidence")) for c in rb["likely_causes"])
    assert "start_processor:GenerateFlowFile" in rb["remediation"]["safe_options"]
    assert out["source"]["severities"] == ["STOPPED"]


def test_fallback_invalid_log_lab_options() -> None:
    from ratatoskr.nifi.runbook import fallback_runbook, is_valid_runbook_event, load_fixture

    out = fallback_runbook(load_fixture("invalid-log"))
    assert is_valid_runbook_event(out)
    lab = out["runbook"]["remediation"]["lab_options"]
    assert any(x.startswith("fix_processor_config:LogAttribute") for x in lab)
    assert any("INVALID" in c["cause"] for c in out["runbook"]["likely_causes"])


def test_fallback_queue_backlog() -> None:
    from ratatoskr.nifi.runbook import fallback_runbook, is_valid_runbook_event, load_fixture

    out = fallback_runbook(load_fixture("queue-backlog"))
    assert is_valid_runbook_event(out)
    rb = out["runbook"]
    assert any("queue" in c["cause"].lower() or "backlog" in c["cause"].lower() for c in rb["likely_causes"])
    assert any("empty_connection_queue" in x for x in rb["remediation"]["lab_options"])
    assert any("empty" in d.lower() for d in rb["remediation"]["do_not"])


def test_fallback_stop_consume() -> None:
    from ratatoskr.nifi.runbook import fallback_runbook, is_valid_runbook_event, load_fixture

    out = fallback_runbook(load_fixture("stop-consume"))
    assert is_valid_runbook_event(out)
    assert "start_processor:ConsumeKafka" in out["runbook"]["remediation"]["safe_options"]


def test_fallback_unreachable() -> None:
    from ratatoskr.nifi.runbook import fallback_runbook, is_valid_runbook_event, load_fixture

    out = fallback_runbook(load_fixture("nifi-unreachable"))
    assert is_valid_runbook_event(out)
    assert any("unreachable" in c["cause"].lower() for c in out["runbook"]["likely_causes"])
    steps = " ".join(s["step"].lower() for s in out["runbook"]["diagnostic_steps"])
    assert "nifi" in steps


def test_all_fixtures_produce_valid_runbooks() -> None:
    from ratatoskr.nifi.runbook import (
        fallback_runbook,
        is_valid_runbook_event,
        list_fixture_ids,
        load_fixture,
    )

    for fid in list_fixture_ids():
        out = fallback_runbook(load_fixture(fid))
        assert is_valid_runbook_event(out), fid
        assert out["mutations"] == []


def main() -> int:
    tests = [
        test_empty_runbook_valid,
        test_validate_rejects_bad_mode_and_mutations,
        test_fixtures_load_and_list,
        test_fallback_healthy,
        test_fallback_stop_generate_cites_heal_plan,
        test_fallback_invalid_log_lab_options,
        test_fallback_queue_backlog,
        test_fallback_stop_consume,
        test_fallback_unreachable,
        test_all_fixtures_produce_valid_runbooks,
    ]
    failed = 0
    for fn in tests:
        try:
            fn()
            print(f"OK  {fn.__name__}")
        except Exception as exc:  # noqa: BLE001
            failed += 1
            print(f"FAIL {fn.__name__}: {exc}")
    if failed:
        print(f"FAILED {failed}/{len(tests)}")
        return 1
    print(f"PASS ({len(tests)})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
