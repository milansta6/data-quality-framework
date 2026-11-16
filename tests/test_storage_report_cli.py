from datetime import datetime, timedelta
from pathlib import Path

import pytest

from dq.cli import main
from dq.engine import DQEngine
from dq.report import build_report_context, render_html_report, sparkline
from dq.storage import RESULTS, SAMPLES, SCORES, ResultStore
from tests.conftest import make_suite

EXAMPLE = Path(__file__).parents[1] / "examples" / "demo_suite.yml"


@pytest.fixture(scope="module")
def small_suite():
    return make_suite(
        [
            {"id": "N1", "table": "t", "type": "not_null", "column": "a"},
            {"id": "U1", "table": "t", "type": "unique", "column": "a", "severity": "warning"},
        ]
    )


@pytest.fixture(scope="module")
def small_df(spark):
    return spark.createDataFrame([(1, "<b>x</b>"), (1, "y"), (None, "z")], "a INT, b STRING")


def test_store_appends_runs_and_builds_history(spark, tmp_path, small_suite, small_df):
    store = ResultStore(spark, path=str(tmp_path / "out"))
    assert store.read(RESULTS) is None
    assert store.score_history() == []

    engine = DQEngine(spark, small_suite)
    t0 = datetime(2026, 1, 1, 8, 0, 0)
    first = engine.run({"t": small_df}, run_id="run1", run_ts=t0)
    second = engine.run({"t": small_df}, run_id="run2", run_ts=t0 + timedelta(days=1))
    store.save(first)
    written = store.save(second)
    assert set(written) == {RESULTS, SAMPLES, SCORES}

    results = store.read(RESULTS)
    assert results.count() == 4
    assert {r["run_id"] for r in results.select("run_id").collect()} == {"run1", "run2"}
    stored_ts = {r["run_ts"] for r in results.select("run_ts").collect()}
    assert stored_ts == {t0, t0 + timedelta(days=1)}

    history = store.score_history("test_suite")
    assert {h["run_id"] for h in history} == {"run1", "run2"}
    assert store.score_history("other_suite") == []


def test_store_needs_target(spark):
    with pytest.raises(ValueError):
        ResultStore(spark, path=None, database=None)


def test_store_location(spark):
    assert ResultStore(spark, database="dq").location(SCORES) == "dq.dq_scores"
    assert ResultStore(spark, path="/data/dq/").location(RESULTS) == "/data/dq/dq_results"


def test_html_report(spark, tmp_path, small_suite, small_df):
    result = DQEngine(spark, small_suite).run({"t": small_df}, run_id="rep1")
    history = [
        {**s, "run_id": "old", "run_ts": result.run_ts - timedelta(days=1), "score": 50.0}
        for s in result.score_rows
    ]
    path = render_html_report(result, tmp_path / "r" / "report.html", history)
    html = path.read_text(encoding="utf-8")
    assert "N1" in html and "U1" in html and "test_suite" in html
    assert "<svg" in html  # trend sparkline from history
    assert "<b>x</b>" not in html  # sample data is escaped
    assert "&lt;b&gt;x&lt;/b&gt;" in html or "\\u003cb" in html


def test_report_context_includes_current_run(spark, small_suite, small_df):
    result = DQEngine(spark, small_suite).run({"t": small_df})
    ctx = build_report_context(result, history=[])
    assert ctx["overall"]["scope"] == "suite"
    assert [t["table_name"] for t in ctx["tables"]] == ["t"]
    assert ctx["rules"][0]["status"] in ("FAIL", "WARN")  # worst first


def test_sparkline():
    assert "first run" in sparkline([(datetime(2026, 1, 1), 90.0)])
    svg = sparkline([(datetime(2026, 1, 1), 90.0), (datetime(2026, 1, 2), 95.0)])
    assert svg.startswith("<svg") and "90.0 → 95.0" in svg


def test_cli_validate(capsys):
    assert main(["validate", str(EXAMPLE)]) == 0
    assert "retail_demo" in capsys.readouterr().out


def test_cli_validate_bad_file(tmp_path, capsys):
    bad = tmp_path / "bad.yml"
    bad.write_text("rules:\n  - {id: X, table: t, type: nope}\n", encoding="utf-8")
    assert main(["validate", str(bad)]) == 2
    assert "Unknown rule type" in capsys.readouterr().err


def test_cli_rules(capsys):
    assert main(["rules"]) == 0
    assert "sum_reconciliation" in capsys.readouterr().out


def test_cli_run_demo(spark, tmp_path, capsys):
    out = tmp_path / "dq"
    code = main(["run", str(EXAMPLE), "--demo-data", "--output", str(out)])
    assert code == 1  # the demo data contains blocking failures on purpose
    text = capsys.readouterr().out
    assert "CUST_003" in text
    assert list((out / "reports").glob("dq_report_*.html"))

    code = main(
        [
            "run",
            str(EXAMPLE),
            "--demo-data",
            "--output",
            str(out),
            "--fail-on",
            "never",
            "--no-report",
        ]
    )
    assert code == 0
    assert len(list((out / "reports").glob("*.html"))) == 1
