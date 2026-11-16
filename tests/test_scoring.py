from datetime import datetime

import pytest

from dq.scoring import compute_scores, rating, rule_score, score_trend, weighted_score


def res(rule_id, table, dim, pass_rate, status="PASS", severity="error", run_id="r1"):
    return {
        "run_id": run_id,
        "run_ts": datetime(2026, 1, 1),
        "suite": "s",
        "rule_id": rule_id,
        "table_name": table,
        "dimension": dim,
        "pass_rate": pass_rate,
        "status": status,
        "severity": severity,
    }


def test_rule_score():
    assert rule_score(res("a", "t", "validity", 0.95)) == pytest.approx(95.0)
    assert rule_score(res("a", "t", "validity", None, status="ERROR")) == 0.0


def test_weighted_score_uses_severity():
    rows = [res("a", "t", "v", 1.0, severity="error"), res("b", "t", "v", 0.7, severity="warning")]
    assert weighted_score(rows) == pytest.approx((100 * 2 + 70 * 1) / 3, abs=0.01)
    assert weighted_score(rows, {"error": 1, "warning": 1}) == pytest.approx(85.0)
    assert weighted_score([]) == 100.0


@pytest.mark.parametrize(
    "score, expected", [(99, "GOOD"), (95, "GOOD"), (90, "ATTENTION"), (10, "CRITICAL")]
)
def test_rating(score, expected):
    assert rating(score) == expected


def test_rating_custom_limits():
    assert rating(90, {"good": 85}) == "GOOD"


def test_compute_scores_all_scopes():
    rows = [
        res("c1", "customers", "completeness", 1.0),
        res("c2", "customers", "validity", 0.9, status="FAIL"),
        res("a1", "accounts", "validity", 0.5, status="WARN", severity="warning"),
    ]
    scores = {(s["scope"], s["table_name"], s["dimension"]): s for s in compute_scores(rows)}

    assert scores[("table", "customers", "ALL")]["score"] == pytest.approx(95.0)
    assert scores[("table", "customers", "ALL")]["rules_not_passed"] == 1
    assert scores[("table", "accounts", "ALL")]["rating"] == "CRITICAL"
    assert scores[("dimension", "ALL", "validity")]["score"] == pytest.approx(
        (90 * 2 + 50) / 3, abs=0.01
    )
    assert scores[("table_dimension", "customers", "completeness")]["score"] == 100.0
    suite = scores[("suite", "ALL", "ALL")]
    assert suite["rules_evaluated"] == 3
    assert suite["score"] == pytest.approx((100 * 2 + 90 * 2 + 50) / 5, abs=0.01)


def test_compute_scores_empty():
    assert compute_scores([]) == []


def test_score_trend_orders_by_time_and_limits():
    history = []
    for day in range(1, 15):
        history.append(
            {
                "run_id": f"r{day}",
                "run_ts": datetime(2026, 1, day),
                "scope": "table",
                "table_name": "customers",
                "dimension": "ALL",
                "score": 80 + day,
            }
        )
    history.reverse()
    trend = score_trend(history, last_n=5)["customers"]
    assert [p[1] for p in trend] == [90, 91, 92, 93, 94]
    assert score_trend(history, scope="suite") == {}
