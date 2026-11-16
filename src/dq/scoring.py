"""Quality indices: turn many rule results into a few numbers a business user can read.

How a score is calculated
-------------------------
* Every rule gets a score of ``pass_rate * 100`` (0-100). A rule that could not
  run (status ``ERROR``) scores 0, because "we do not know" is not "good".
* Rules are weighted by severity (default: error = 2, warning = 1).
* A score is the weighted average of the rule scores in its group:

  =================  =====================================================
  scope              group
  =================  =====================================================
  ``suite``          all rules of the run (one number for the whole suite)
  ``table``          all rules of one table (one number per dataset)
  ``dimension``      all rules of one dimension, across tables
  ``table_dimension`` one table x one dimension (the heat map in the report)
  =================  =====================================================

* A rating translates the score into words: ``GOOD`` (>= 95),
  ``ATTENTION`` (>= 80) or ``CRITICAL`` (below 80). Thresholds are configurable.

This module works on plain Python dicts so it is easy to test and reuse.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Sequence
from datetime import datetime
from typing import Any

from dq.config import DEFAULT_RATINGS, DEFAULT_WEIGHTS

ALL = "ALL"
NOT_OK_STATUSES = {"FAIL", "WARN", "ERROR"}

SCORE_COLUMNS = (
    "run_id",
    "run_ts",
    "suite",
    "scope",
    "table_name",
    "dimension",
    "score",
    "rating",
    "rules_evaluated",
    "rules_not_passed",
)


def rule_score(result: dict[str, Any]) -> float:
    """Score of one rule result on a 0-100 scale."""
    if result.get("status") == "ERROR" or result.get("pass_rate") is None:
        return 0.0
    return float(result["pass_rate"]) * 100.0


def rating(score: float, ratings: dict[str, float] | None = None) -> str:
    limits = {**DEFAULT_RATINGS, **(ratings or {})}
    if score >= limits["good"]:
        return "GOOD"
    if score >= limits["attention"]:
        return "ATTENTION"
    return "CRITICAL"


def weighted_score(
    results: Iterable[dict[str, Any]], weights: dict[str, float] | None = None
) -> float:
    """Weighted average of rule scores. Returns 100.0 for an empty group."""
    w = {**DEFAULT_WEIGHTS, **(weights or {})}
    total, weight_sum = 0.0, 0.0
    for r in results:
        weight = w.get(r.get("severity", "error"), 1.0)
        total += rule_score(r) * weight
        weight_sum += weight
    return round(total / weight_sum, 2) if weight_sum else 100.0


def compute_scores(
    results: Sequence[dict[str, Any]],
    weights: dict[str, float] | None = None,
    ratings: dict[str, float] | None = None,
) -> list[dict[str, Any]]:
    """Aggregate rule results into suite, table, dimension and table x dimension scores."""
    if not results:
        return []
    first = results[0]
    groups: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for r in results:
        table, dim = r["table_name"], r["dimension"]
        groups[("suite", ALL, ALL)].append(r)
        groups[("table", table, ALL)].append(r)
        groups[("dimension", ALL, dim)].append(r)
        groups[("table_dimension", table, dim)].append(r)

    scores = []
    for (scope, table, dim), members in groups.items():
        score = weighted_score(members, weights)
        scores.append(
            {
                "run_id": first["run_id"],
                "run_ts": first["run_ts"],
                "suite": first["suite"],
                "scope": scope,
                "table_name": table,
                "dimension": dim,
                "score": score,
                "rating": rating(score, ratings),
                "rules_evaluated": len(members),
                "rules_not_passed": sum(m["status"] in NOT_OK_STATUSES for m in members),
            }
        )
    order = {"suite": 0, "table": 1, "dimension": 2, "table_dimension": 3}
    scores.sort(key=lambda s: (order[s["scope"]], s["table_name"], s["dimension"]))
    return scores


def score_trend(
    score_history: Iterable[dict[str, Any]],
    scope: str = "table",
    last_n: int = 10,
) -> dict[str, list[tuple[datetime, float]]]:
    """Return ``{name: [(run_ts, score), ...]}`` for the last ``last_n`` runs, oldest first.

    ``name`` is the table name for ``scope="table"``, the dimension for
    ``scope="dimension"`` and ``"ALL"`` for ``scope="suite"``.
    """
    series: dict[str, dict[Any, tuple[datetime, float]]] = defaultdict(dict)
    for row in score_history:
        if row["scope"] != scope:
            continue
        key = row["dimension"] if scope == "dimension" else row["table_name"]
        # One point per run (if a run was stored twice, the last one wins).
        series[key][row["run_id"]] = (row["run_ts"], float(row["score"]))
    return {
        key: sorted(points.values(), key=lambda p: p[0])[-last_n:] for key, points in series.items()
    }
