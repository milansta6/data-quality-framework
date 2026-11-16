from pathlib import Path

import pytest

from dq.engine import DataQualityError, DQEngine
from tests.conftest import RUN_TS, make_suite

EXAMPLE = Path(__file__).parents[1] / "examples" / "demo_suite.yml"

# Expected outcome of the demo suite on the demo data (see dq/demo.py).
EXPECTED_DEMO = {
    "CUST_001": ("PASS", 0),
    "CUST_002": ("PASS", 3),
    "CUST_003": ("FAIL", 4),
    "CUST_004": ("WARN", 4),
    "CUST_005": ("FAIL", 2),
    "CUST_006": ("FAIL", 1),
    "ACC_001": ("FAIL", 3),
    "ACC_002": ("FAIL", 2),
    "ACC_003": ("FAIL", 2),
    "ACC_004": ("FAIL", 1),
    "ACC_005": ("WARN", 1),
    "TRX_001": ("PASS", 0),
    "TRX_002": ("WARN", 3),
    "TRX_003": ("FAIL", 2),
    "TRX_004": ("PASS", 0),
    "TRX_005": ("FAIL", 1),
    "REC_001": ("WARN", 3),
    "REC_002": ("PASS", 0),
}


@pytest.fixture(scope="module")
def demo_result(spark, demo_tables):
    return DQEngine(spark, EXAMPLE).run(demo_tables, run_id="demo-run")


def test_demo_suite_outcome(demo_result):
    actual = {r["rule_id"]: (r["status"], r["rows_failed"]) for r in demo_result.result_rows}
    assert actual == EXPECTED_DEMO


def test_inactive_rules_are_skipped(demo_result):
    assert [r.id for r in demo_result.skipped_rules] == ["REC_003"]
    assert "REC_003" not in {r["rule_id"] for r in demo_result.result_rows}


def test_results_dataframe_schema(demo_result):
    df = demo_result.results
    assert df.count() == 18
    for col in ("rule_id", "rows_checked", "rows_failed", "pass_rate", "status", "run_ts"):
        assert col in df.columns
    row = df.where("rule_id = 'CUST_003'").first()
    assert row["rows_checked"] == 202
    assert row["pass_rate"] == pytest.approx(198 / 202, abs=1e-6)
    assert row["run_id"] == "demo-run"


def test_failed_samples_are_limited_and_json(demo_result):
    import json

    samples = demo_result.failed_samples
    counts = {r["rule_id"]: r["count"] for r in samples.groupBy("rule_id").count().collect()}
    assert counts["CUST_003"] == 4
    assert "CUST_001" not in counts
    assert max(counts.values()) <= 10  # sample_size from the YAML
    email_sample = samples.where("rule_id = 'CUST_002'").first()["row_data"]
    assert json.loads(email_sample)["email"] is None  # NULLs are kept in the sample


def test_scores_are_produced(demo_result):
    scopes = {r["scope"] for r in demo_result.score_rows}
    assert scopes == {"suite", "table", "dimension", "table_dimension"}
    assert 0 < demo_result.overall_score < 100
    assert demo_result.scores.where("scope = 'table'").count() == 4


def test_broken_rule_gets_error_and_others_still_run(spark):
    df = spark.createDataFrame([(1,), (None,)], "a INT")
    suite = make_suite(
        [
            {"id": "OK", "table": "t", "type": "not_null", "column": "a"},
            {"id": "BAD_SQL", "table": "t", "type": "custom_sql", "sql": "SELECT * FROM nope"},
            {"id": "BAD_COL", "table": "t", "type": "not_null", "column": "missing"},
            {"id": "NO_TABLE", "table": "unknown", "type": "not_null", "column": "a"},
        ]
    )
    result = DQEngine(spark, suite).run({"t": df}, run_ts=RUN_TS)
    by_id = {r["rule_id"]: r for r in result.result_rows}
    assert by_id["OK"]["status"] == "FAIL"
    for rule_id in ("BAD_SQL", "BAD_COL", "NO_TABLE"):
        assert by_id[rule_id]["status"] == "ERROR"
        assert by_id[rule_id]["error_message"]
        assert by_id[rule_id]["pass_rate"] is None
    assert result.status_counts()["ERROR"] == 3


def test_threshold_and_severity_decide_status(spark):
    df = spark.createDataFrame([(1,), (2,), (3,), (None,)], "a INT")
    base = {"table": "t", "type": "not_null", "column": "a"}
    suite = make_suite(
        [
            {**base, "id": "strict"},
            {**base, "id": "lenient", "threshold": 0.75},
            {**base, "id": "warn", "severity": "warning"},
        ]
    )
    result = DQEngine(spark, suite).run({"t": df})
    status = {r["rule_id"]: r["status"] for r in result.result_rows}
    assert status == {"strict": "FAIL", "lenient": "PASS", "warn": "WARN"}


def test_empty_table_passes(spark):
    df = spark.createDataFrame([], "a INT")
    suite = make_suite([{"id": "e", "table": "t", "type": "not_null", "column": "a"}])
    row = DQEngine(spark, suite).run({"t": df}).result_rows[0]
    assert (row["rows_checked"], row["pass_rate"], row["status"]) == (0, 1.0, "PASS")


def test_raise_for_status(spark):
    df = spark.createDataFrame([(None,)], "a INT")
    warn_only = make_suite(
        [{"id": "w", "table": "t", "type": "not_null", "column": "a", "severity": "warning"}]
    )
    result = DQEngine(spark, warn_only).run({"t": df})
    result.raise_for_status()  # warnings do not block by default
    with pytest.raises(DataQualityError, match="w \\(WARN\\)"):
        result.raise_for_status(fail_on="warning")


def test_load_tables_from_suite(spark, tmp_path):
    csv = tmp_path / "people.csv"
    csv.write_text("id,name\n1,a\n2,\n", encoding="utf-8")
    suite = make_suite(
        [{"id": "n", "table": "people", "type": "not_null", "column": "name"}],
        tables={"people": {"path": str(csv), "format": "csv", "options": {"header": "true"}}},
    )
    engine = DQEngine(spark, suite)
    result = engine.run()
    assert result.result_rows[0]["rows_failed"] == 1


def test_missing_table_declaration(spark):
    suite = make_suite([{"id": "n", "table": "people", "type": "not_null", "column": "x"}])
    with pytest.raises(KeyError, match="not declared"):
        DQEngine(spark, suite).load_tables()


def test_summary_text(demo_result):
    text = demo_result.summary()
    assert "retail_demo" in text and "FAIL=" in text
