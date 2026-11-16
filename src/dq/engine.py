"""The engine: run a suite of rules against Spark DataFrames."""

from __future__ import annotations

import contextlib
import logging
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any

from dq.config import Rule, Suite, SuiteSource, load_suite
from dq.rules import RuleContext, get_rule_type
from dq.scoring import SCORE_COLUMNS, compute_scores

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)

STATUS_ORDER = {"ERROR": 0, "FAIL": 1, "WARN": 2, "PASS": 3}


class DataQualityError(RuntimeError):
    """Raised by :meth:`RunResult.raise_for_status` when blocking rules fail."""


def results_schema():
    from pyspark.sql import types as T

    return T.StructType(
        [
            T.StructField("run_id", T.StringType(), False),
            T.StructField("run_ts", T.TimestampType(), False),
            T.StructField("suite", T.StringType(), False),
            T.StructField("rule_id", T.StringType(), False),
            T.StructField("table_name", T.StringType(), False),
            T.StructField("rule_type", T.StringType(), False),
            T.StructField("dimension", T.StringType(), False),
            T.StructField("severity", T.StringType(), False),
            T.StructField("owner", T.StringType(), True),
            T.StructField("description", T.StringType(), True),
            T.StructField("rows_checked", T.LongType(), False),
            T.StructField("rows_failed", T.LongType(), False),
            T.StructField("pass_rate", T.DoubleType(), True),
            T.StructField("threshold", T.DoubleType(), False),
            T.StructField("status", T.StringType(), False),
            T.StructField("details", T.StringType(), True),
            T.StructField("error_message", T.StringType(), True),
            T.StructField("duration_seconds", T.DoubleType(), False),
        ]
    )


def samples_schema():
    from pyspark.sql import types as T

    return T.StructType(
        [
            T.StructField("run_id", T.StringType(), False),
            T.StructField("run_ts", T.TimestampType(), False),
            T.StructField("suite", T.StringType(), False),
            T.StructField("rule_id", T.StringType(), False),
            T.StructField("table_name", T.StringType(), False),
            T.StructField("row_number", T.IntegerType(), False),
            T.StructField("row_data", T.StringType(), False),
        ]
    )


def scores_schema():
    from pyspark.sql import types as T

    types = {
        "run_ts": T.TimestampType(),
        "score": T.DoubleType(),
        "rules_evaluated": T.IntegerType(),
        "rules_not_passed": T.IntegerType(),
    }
    return T.StructType(
        [T.StructField(c, types.get(c, T.StringType()), False) for c in SCORE_COLUMNS]
    )


@dataclass
class RunResult:
    """Everything produced by one run.

    The ``*_rows`` lists are small Python copies of the data (handy for reports and
    tests); the ``results`` / ``failed_samples`` / ``scores`` properties return
    Spark DataFrames with a fixed schema, ready to be stored.
    """

    spark: SparkSession
    suite: Suite
    run_id: str
    run_ts: datetime
    result_rows: list[dict[str, Any]]
    sample_rows: list[dict[str, Any]]
    score_rows: list[dict[str, Any]]
    skipped_rules: list[Rule] = field(default_factory=list)

    @property
    def results(self) -> DataFrame:
        return self.spark.createDataFrame(self.result_rows, results_schema())

    @property
    def failed_samples(self) -> DataFrame:
        return self.spark.createDataFrame(self.sample_rows, samples_schema())

    @property
    def scores(self) -> DataFrame:
        return self.spark.createDataFrame(self.score_rows, scores_schema())

    @property
    def overall_score(self) -> float:
        for row in self.score_rows:
            if row["scope"] == "suite":
                return row["score"]
        return 100.0

    def status_counts(self) -> dict[str, int]:
        counts = dict.fromkeys(STATUS_ORDER, 0)
        for row in self.result_rows:
            counts[row["status"]] += 1
        return counts

    def failed_rules(self, statuses: tuple[str, ...] = ("FAIL", "ERROR")) -> list[dict[str, Any]]:
        return [r for r in self.result_rows if r["status"] in statuses]

    def raise_for_status(self, fail_on: str = "error") -> None:
        """Stop a pipeline when quality is not good enough.

        ``fail_on="error"`` raises on FAIL / ERROR, ``fail_on="warning"`` also on WARN.
        """
        statuses = ("FAIL", "ERROR", "WARN") if fail_on == "warning" else ("FAIL", "ERROR")
        failed = self.failed_rules(statuses)
        if failed:
            ids = ", ".join(f"{r['rule_id']} ({r['status']})" for r in failed)
            raise DataQualityError(f"{len(failed)} data quality rule(s) not passed: {ids}")

    def summary(self) -> str:
        counts = self.status_counts()
        parts = ", ".join(f"{k}={v}" for k, v in counts.items() if v)
        return (
            f"Suite '{self.suite.name}' run {self.run_id}: score {self.overall_score:.1f} "
            f"({len(self.result_rows)} rules: {parts})"
        )


class DQEngine:
    """Run the active rules of a suite and collect the results.

    Example::

        engine = DQEngine(spark, "rules/customers.yml")
        result = engine.run({"customers": customers_df, "accounts": accounts_df})
        result.results.show()
    """

    def __init__(
        self,
        spark: SparkSession,
        suite: Suite | SuiteSource,
        sample_size: int | None = None,
        cache_tables: bool = True,
    ):
        self.spark = spark
        self.suite = suite if isinstance(suite, Suite) else load_suite(suite)
        self.sample_size = self.suite.output.sample_size if sample_size is None else sample_size
        self.cache_tables = cache_tables

    # ------------------------------------------------------------------ loading
    def load_tables(self) -> dict[str, DataFrame]:
        """Read the tables declared in the suite's ``tables`` section."""
        tables = {}
        for name in self.suite.table_names:
            source = self.suite.tables.get(name)
            if source is None:
                raise KeyError(f"Table '{name}' is used by a rule but not declared in 'tables'.")
            if source.table:
                tables[name] = self.spark.table(source.table)
            else:
                reader = self.spark.read.format(source.format).options(**source.options)
                tables[name] = reader.load(source.path)
        return tables

    # ------------------------------------------------------------------ running
    def run(
        self,
        tables: dict[str, DataFrame] | None = None,
        run_id: str | None = None,
        run_ts: datetime | None = None,
    ) -> RunResult:
        """Evaluate all active rules. A rule that crashes gets status ``ERROR``;
        the other rules still run."""
        tables = dict(tables) if tables is not None else self.load_tables()
        run_id = run_id or uuid.uuid4().hex[:12]
        # Microseconds are dropped so the timestamp survives every storage format unchanged.
        run_ts = (run_ts or datetime.now()).replace(microsecond=0)

        missing = [t for t in self.suite.table_names if t not in tables]
        if missing:
            logger.warning("Tables not provided, their rules will error: %s", missing)

        cached = []
        if self.cache_tables:
            for name, df in tables.items():
                try:
                    tables[name] = df.cache()
                    cached.append(tables[name])
                except Exception as exc:  # e.g. not supported on Databricks serverless
                    logger.info("Could not cache table %s (%s); running uncached", name, exc)
        for name, df in tables.items():
            df.createOrReplaceTempView(name)

        ctx = RuleContext(self.spark, tables, run_ts)
        result_rows: list[dict[str, Any]] = []
        sample_rows: list[dict[str, Any]] = []
        try:
            for rule in self.suite.active_rules:
                row, samples = self._run_rule(ctx, rule, run_id, run_ts)
                result_rows.append(row)
                sample_rows.extend(samples)
        finally:
            for df in cached:
                with contextlib.suppress(Exception):  # best-effort cleanup
                    df.unpersist()

        score_rows = compute_scores(
            result_rows, self.suite.scoring.weights, self.suite.scoring.ratings
        )
        result = RunResult(
            spark=self.spark,
            suite=self.suite,
            run_id=run_id,
            run_ts=run_ts,
            result_rows=result_rows,
            sample_rows=sample_rows,
            score_rows=score_rows,
            skipped_rules=self.suite.inactive_rules,
        )
        logger.info(result.summary())
        return result

    def _run_rule(
        self, ctx: RuleContext, rule: Rule, run_id: str, run_ts: datetime
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        base = {
            "run_id": run_id,
            "run_ts": run_ts,
            "suite": self.suite.name,
            "rule_id": rule.id,
            "table_name": rule.table,
            "rule_type": rule.type,
            "dimension": rule.dimension,
            "severity": rule.severity,
            "owner": rule.owner,
            "description": rule.description,
            "threshold": rule.threshold,
        }
        started = time.perf_counter()
        try:
            outcome = get_rule_type(rule.type).func(ctx, rule)
            samples = self._collect_samples(outcome.failed_rows, outcome.rows_failed)
        except Exception as exc:  # one broken rule must not stop the run
            logger.exception("Rule %s failed to run", rule.id)
            message = f"{type(exc).__name__}: {exc}".strip()
            row = {
                **base,
                "rows_checked": 0,
                "rows_failed": 0,
                "pass_rate": None,
                "status": "ERROR",
                "details": None,
                "error_message": message.splitlines()[0][:1000],
                "duration_seconds": round(time.perf_counter() - started, 3),
            }
            return row, []

        checked, failed = outcome.rows_checked, outcome.rows_failed
        if checked:
            pass_rate = round((checked - failed) / checked, 6)
        else:
            pass_rate = 0.0 if failed else 1.0
        passed = outcome.passed if outcome.passed is not None else pass_rate >= rule.threshold
        if passed:
            status = "PASS"
        else:
            status = "WARN" if rule.severity == "warning" else "FAIL"

        row = {
            **base,
            "rows_checked": int(checked),
            "rows_failed": int(failed),
            "pass_rate": pass_rate,
            "status": status,
            "details": outcome.details or None,
            "error_message": None,
            "duration_seconds": round(time.perf_counter() - started, 3),
        }
        sample_rows = [
            {
                "run_id": run_id,
                "run_ts": run_ts,
                "suite": self.suite.name,
                "rule_id": rule.id,
                "table_name": rule.table,
                "row_number": i + 1,
                "row_data": data,
            }
            for i, data in enumerate(samples)
        ]
        return row, sample_rows

    def _collect_samples(self, failed_rows: DataFrame | None, rows_failed: int) -> list[str]:
        """Return up to ``sample_size`` failing rows as JSON strings (one schema for all tables)."""
        if failed_rows is None or rows_failed == 0 or self.sample_size <= 0:
            return []
        from pyspark.sql import functions as F

        as_json = failed_rows.limit(self.sample_size).select(
            F.to_json(
                F.struct(*[F.col(f"`{c}`") for c in failed_rows.columns]),
                {"ignoreNullFields": "false"},  # show NULLs: they are often the problem
            ).alias("j")
        )
        return [r["j"] for r in as_json.collect()]
