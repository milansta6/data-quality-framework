"""data-quality-framework: config-driven data quality checks for PySpark and SQL."""

from dq.config import ConfigError, Rule, Suite, load_suite
from dq.engine import DataQualityError, DQEngine, RunResult
from dq.report import render_html_report
from dq.rules import CheckOutcome, RuleContext, available_rule_types, register_rule, row_level_check
from dq.scoring import compute_scores, score_trend
from dq.storage import ResultStore

__version__ = "0.1.0"

__all__ = [
    "CheckOutcome",
    "ConfigError",
    "DQEngine",
    "DataQualityError",
    "ResultStore",
    "Rule",
    "RuleContext",
    "RunResult",
    "Suite",
    "available_rule_types",
    "compute_scores",
    "load_suite",
    "register_rule",
    "render_html_report",
    "row_level_check",
    "score_trend",
]
