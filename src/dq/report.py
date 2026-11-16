"""Generate a self-contained HTML report for one run (no JavaScript, no external files)."""

from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from jinja2 import Environment, PackageLoader, select_autoescape
from markupsafe import Markup

from dq.scoring import score_trend

if TYPE_CHECKING:  # pragma: no cover
    from dq.engine import RunResult

STATUS_SORT = {"ERROR": 0, "FAIL": 1, "WARN": 2, "PASS": 3}


def sparkline(
    points: Sequence[tuple[datetime, float]], width: int = 120, height: int = 28
) -> Markup:
    """Tiny inline SVG line chart of scores (0-100) over runs."""
    if len(points) < 2:
        return Markup('<span class="muted">first run</span>')
    scores = [p[1] for p in points]
    low = min(min(scores), 80.0)
    span = max(100.0 - low, 1.0)
    step = (width - 6) / (len(scores) - 1)
    coords = [
        (3 + i * step, 3 + (height - 6) * (1 - (s - low) / span)) for i, s in enumerate(scores)
    ]
    path = " ".join(f"{x:.1f},{y:.1f}" for x, y in coords)
    lx, ly = coords[-1]
    title = " → ".join(f"{s:.1f}" for s in scores)
    return Markup(
        f'<svg class="spark" width="{width}" height="{height}" viewBox="0 0 {width} {height}" '
        f'role="img" aria-label="Score trend {title}"><title>{title}</title>'
        f'<polyline points="{path}" fill="none" stroke="currentColor" stroke-width="1.5" '
        f'stroke-linejoin="round" stroke-linecap="round"/>'
        f'<circle cx="{lx:.1f}" cy="{ly:.1f}" r="2.5" fill="currentColor"/></svg>'
    )


def _pretty_json(text: str) -> str:
    try:
        return json.dumps(json.loads(text), ensure_ascii=False)
    except (TypeError, ValueError):
        return text


def build_report_context(
    result: RunResult, history: Sequence[dict[str, Any]] | None = None
) -> dict[str, Any]:
    """Prepare everything the template needs (kept separate so it can be tested)."""
    history = list(history or [])
    # Make sure the current run is part of the trend even if it was not stored.
    if not any(h["run_id"] == result.run_id for h in history):
        history.extend(result.score_rows)

    table_trends = score_trend(history, scope="table")
    suite_trend = score_trend(history, scope="suite").get("ALL", [])

    scores = result.score_rows
    tables = [s for s in scores if s["scope"] == "table"]
    dimensions = [s for s in scores if s["scope"] == "dimension"]
    cells = {
        (s["table_name"], s["dimension"]): s for s in scores if s["scope"] == "table_dimension"
    }
    dim_names = [d["dimension"] for d in dimensions]

    samples: dict[str, list[str]] = {}
    for s in result.sample_rows:
        samples.setdefault(s["rule_id"], []).append(_pretty_json(s["row_data"]))

    rules = sorted(
        result.result_rows,
        key=lambda r: (STATUS_SORT[r["status"]], r["table_name"], r["rule_id"]),
    )
    total_checked = sum(r["rows_checked"] for r in result.result_rows)
    total_failed = sum(r["rows_failed"] for r in result.result_rows)

    return {
        "suite": result.suite.name,
        "run_id": result.run_id,
        "run_ts": result.run_ts,
        "overall": next((s for s in scores if s["scope"] == "suite"), None),
        "suite_trend": sparkline(suite_trend, width=160, height=36),
        "counts": result.status_counts(),
        "total_checked": total_checked,
        "total_failed": total_failed,
        "tables": [
            {**t, "trend": sparkline(table_trends.get(t["table_name"], []))} for t in tables
        ],
        "dimensions": dimensions,
        "dim_names": dim_names,
        "matrix": [
            {
                "table": t["table_name"],
                "cells": [cells.get((t["table_name"], d)) for d in dim_names],
            }
            for t in tables
        ],
        "rules": rules,
        "samples": samples,
        "skipped": result.skipped_rules,
        "ratings": result.suite.scoring.ratings,
        "generated_at": datetime.now(),
    }


def render_html_report(
    result: RunResult,
    output_path: str | Path,
    history: Sequence[dict[str, Any]] | None = None,
) -> Path:
    """Write the HTML report and return its path."""
    env = Environment(
        loader=PackageLoader("dq", "templates"),
        autoescape=select_autoescape(["html"]),
        trim_blocks=True,
        lstrip_blocks=True,
    )
    env.filters["pct"] = lambda v: "-" if v is None else f"{v * 100:.2f}%"
    env.filters["num"] = lambda v: f"{v:,}"
    html = env.get_template("report.html").render(**build_report_context(result, history))
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(html, encoding="utf-8")
    return path
