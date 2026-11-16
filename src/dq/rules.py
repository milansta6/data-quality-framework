"""Rule registry and built-in rule implementations.

A rule implementation is a plain function ``(ctx, rule) -> CheckOutcome``.
Register it with :func:`register_rule` and it can be used from YAML right away::

    @register_rule("positive", required=("column",), dimension="validity")
    def check_positive(ctx: RuleContext, rule: Rule) -> CheckOutcome:
        df = ctx.table(rule.table, rule.param("filter"))
        return row_level_check(df, F.col(rule.require("column")) <= 0)

Conventions used by the built-in rules:

* ``rows_checked`` / ``rows_failed`` drive the pass rate (``1 - failed / checked``).
* ``failed_rows`` is a *lazy* DataFrame; the engine only samples it when needed.
* ``passed`` can override the default "pass rate >= threshold" decision
  (used by table-level checks with tolerances, such as reconciliations).
* Validity rules ignore NULL values unless ``ignore_nulls: false`` is set.
  Missing values are a *completeness* problem, measured by ``not_null``.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Callable

from dq.config import ConfigError, Rule

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import Column, DataFrame, SparkSession


@dataclass
class CheckOutcome:
    """The result of evaluating one rule."""

    rows_checked: int
    rows_failed: int
    failed_rows: DataFrame | None = None
    details: str = ""
    passed: bool | None = None


class RuleContext:
    """Everything a rule needs at evaluation time."""

    def __init__(self, spark: SparkSession, tables: dict[str, DataFrame], run_ts: datetime):
        self.spark = spark
        self.tables = tables
        self.run_ts = run_ts

    def table(self, name: str, where: str | None = None) -> DataFrame:
        """Return a registered table, optionally filtered with a SQL ``WHERE`` expression."""
        if name not in self.tables:
            raise KeyError(f"Table '{name}' was not provided. Available: {sorted(self.tables)}")
        df = self.tables[name]
        return df.where(where) if where else df


RuleFunc = Callable[[RuleContext, Rule], CheckOutcome]


@dataclass(frozen=True)
class RuleType:
    name: str
    func: RuleFunc
    required: tuple[str, ...]
    default_dimension: str

    def validate(self, rule: Rule) -> None:
        """Check that all mandatory parameters are present (called at load time)."""
        for param in self.required:
            if "|" in param:  # "a|b" means: at least one of a or b
                options = param.split("|")
                if all(rule.param(o) is None for o in options):
                    raise ConfigError(
                        f"Rule '{rule.id}' ({rule.type}) needs one of: {', '.join(options)}."
                    )
            else:
                rule.require(param)


_REGISTRY: dict[str, RuleType] = {}


def register_rule(
    name: str, *, required: tuple[str, ...] = (), dimension: str = "validity"
) -> Callable[[RuleFunc], RuleFunc]:
    """Decorator that makes a rule function available under ``type: <name>`` in YAML."""

    def decorator(func: RuleFunc) -> RuleFunc:
        _REGISTRY[name] = RuleType(name, func, tuple(required), dimension)
        return func

    return decorator


def get_rule_type(name: str) -> RuleType:
    try:
        return _REGISTRY[name]
    except KeyError:
        raise KeyError(
            f"Unknown rule type '{name}'. Available types: {', '.join(available_rule_types())}"
        ) from None


def available_rule_types() -> list[str]:
    return sorted(_REGISTRY)


# --------------------------------------------------------------------------- helpers


def row_level_check(df: DataFrame, fail_condition: Column, details: str = "") -> CheckOutcome:
    """Count total and failing rows in a single pass over ``df``.

    ``fail_condition`` is a boolean Column that is true for *bad* rows.
    A NULL result of the condition is treated as "not failed".
    """
    from pyspark.sql import functions as F

    condition = F.coalesce(fail_condition, F.lit(False))
    row = df.agg(
        F.count(F.lit(1)).alias("checked"),
        F.sum(F.when(condition, 1).otherwise(0)).alias("failed"),
    ).first()
    checked = int(row["checked"] or 0)
    failed = int(row["failed"] or 0)
    return CheckOutcome(checked, failed, df.where(condition), details)


def _null_guard(rule: Rule, col: Column, condition: Column) -> Column:
    """Apply the ``ignore_nulls`` convention to a validity condition."""
    if rule.param("ignore_nulls", True):
        return col.isNotNull() & condition
    return col.isNull() | condition


def _within_tolerance(diff: Decimal, base: Decimal, rule: Rule) -> bool:
    """Absolute ``tolerance`` and/or relative ``tolerance_pct`` (in percent)."""
    tolerance = Decimal(str(rule.param("tolerance", 0)))
    tolerance_pct = rule.param("tolerance_pct")
    if diff <= tolerance:
        return True
    if tolerance_pct is not None and base != 0:
        return diff / abs(base) * 100 <= Decimal(str(tolerance_pct))
    return False


# --------------------------------------------------------------------------- built-ins


@register_rule("not_null", required=("column|columns",), dimension="completeness")
def check_not_null(ctx: RuleContext, rule: Rule) -> CheckOutcome:
    """Fail rows where any of the columns is NULL (or blank, if ``treat_blank_as_null``)."""
    from pyspark.sql import functions as F

    df = ctx.table(rule.table, rule.param("filter"))
    condition = F.lit(False)
    for name in rule.columns():
        col = F.col(name)
        missing = col.isNull()
        if rule.param("treat_blank_as_null", False):
            missing = missing | (F.trim(col.cast("string")) == "")
        condition = condition | missing
    return row_level_check(df, condition)


@register_rule("unique", required=("column|columns",), dimension="uniqueness")
def check_unique(ctx: RuleContext, rule: Rule) -> CheckOutcome:
    """Fail every row whose key (one or more columns) appears more than once.

    Rows with a NULL in any key column are not treated as duplicates.
    """
    from pyspark.sql import functions as F

    df = ctx.table(rule.table, rule.param("filter"))
    keys = rule.columns()
    not_null_keys = F.lit(True)
    for k in keys:
        not_null_keys = not_null_keys & F.col(k).isNotNull()

    duplicates = df.where(not_null_keys).groupBy(*keys).count().where(F.col("count") > 1)
    stats = duplicates.agg(F.count(F.lit(1)).alias("keys"), F.sum("count").alias("rows")).first()
    dup_keys = int(stats["keys"] or 0)
    failed = int(stats["rows"] or 0)
    checked = df.count()
    failed_rows = df.join(duplicates.select(*keys), on=keys, how="left_semi")
    details = f"{dup_keys} duplicated key value(s) on ({', '.join(keys)})" if dup_keys else ""
    return CheckOutcome(checked, failed, failed_rows, details)


@register_rule("accepted_values", required=("column", "values"), dimension="validity")
def check_accepted_values(ctx: RuleContext, rule: Rule) -> CheckOutcome:
    """Fail rows whose value is not in the ``values`` list."""
    from pyspark.sql import functions as F

    df = ctx.table(rule.table, rule.param("filter"))
    values = rule.require("values")
    if not isinstance(values, list) or not values:
        raise ConfigError(f"Rule '{rule.id}': 'values' must be a non-empty list.")
    col = F.col(rule.require("column"))
    if rule.param("case_insensitive", False):
        col = F.upper(col.cast("string"))
        values = [str(v).upper() for v in values]
    return row_level_check(df, _null_guard(rule, col, ~col.isin(values)))


@register_rule("range", required=("column", "min|max"), dimension="validity")
def check_range(ctx: RuleContext, rule: Rule) -> CheckOutcome:
    """Fail rows outside ``[min, max]``. Set ``inclusive: false`` for an open interval.

    Works for numbers, dates and timestamps (use ISO strings for dates in YAML).
    """
    from pyspark.sql import functions as F

    df = ctx.table(rule.table, rule.param("filter"))
    col = F.col(rule.require("column"))
    inclusive = rule.param("inclusive", True)
    low, high = rule.param("min"), rule.param("max")
    out_of_range = F.lit(False)
    if low is not None:
        out_of_range = out_of_range | ((col < F.lit(low)) if inclusive else (col <= F.lit(low)))
    if high is not None:
        out_of_range = out_of_range | ((col > F.lit(high)) if inclusive else (col >= F.lit(high)))
    return row_level_check(df, _null_guard(rule, col, out_of_range))


@register_rule("regex", required=("column", "pattern"), dimension="validity")
def check_regex(ctx: RuleContext, rule: Rule) -> CheckOutcome:
    """Fail rows that do not match ``pattern`` (Java regex; anchor it with ``^...$``)."""
    from pyspark.sql import functions as F

    df = ctx.table(rule.table, rule.param("filter"))
    col = F.col(rule.require("column"))
    no_match = ~col.cast("string").rlike(str(rule.require("pattern")))
    return row_level_check(df, _null_guard(rule, col, no_match))


@register_rule(
    "referential_integrity", required=("column", "reference_table"), dimension="consistency"
)
def check_referential_integrity(ctx: RuleContext, rule: Rule) -> CheckOutcome:
    """Fail rows whose ``column`` value does not exist in ``reference_table``.

    ``reference_column`` defaults to ``column``. NULL keys are ignored.
    """
    from pyspark.sql import functions as F

    df = ctx.table(rule.table, rule.param("filter"))
    column = rule.require("column")
    ref_column = rule.param("reference_column", column)
    ref = ctx.table(rule.require("reference_table"), rule.param("reference_filter"))
    ref_keys = ref.select(F.col(ref_column).alias("__dq_ref_key")).distinct()

    orphans = df.where(F.col(column).isNotNull()).join(
        ref_keys, df[column] == ref_keys["__dq_ref_key"], "left_anti"
    )
    checked = df.count()
    failed = orphans.count()
    return CheckOutcome(checked, failed, orphans)


@register_rule("freshness", required=("column", "max_age_hours"), dimension="timeliness")
def check_freshness(ctx: RuleContext, rule: Rule) -> CheckOutcome:
    """Table-level check: the newest value of ``column`` must be younger than ``max_age_hours``.

    Counts as one check (1 row checked, 0 or 1 failed).
    """
    from pyspark.sql import functions as F

    df = ctx.table(rule.table, rule.param("filter"))
    column = rule.require("column")
    max_age = float(rule.require("max_age_hours"))
    latest = df.agg(F.max(column).alias("latest")).first()["latest"]

    if latest is None:
        return CheckOutcome(1, 1, None, f"No values in '{column}'", passed=False)
    if isinstance(latest, date) and not isinstance(latest, datetime):
        latest = datetime(latest.year, latest.month, latest.day)
    if latest.tzinfo is not None:
        latest = latest.astimezone().replace(tzinfo=None)

    age_hours = (ctx.run_ts - latest).total_seconds() / 3600
    passed = age_hours <= max_age
    details = f"latest={latest:%Y-%m-%d %H:%M:%S}, age={age_hours:.1f}h, limit={max_age:g}h"
    return CheckOutcome(1, 0 if passed else 1, None, details, passed=passed)


@register_rule("row_count_reconciliation", required=("target_table",), dimension="consistency")
def check_row_count_reconciliation(ctx: RuleContext, rule: Rule) -> CheckOutcome:
    """Compare the row count of ``table`` with ``target_table``.

    ``rows_checked`` is the source count and ``rows_failed`` the absolute difference,
    so the pass rate shows *how much* is missing. Pass/fail uses ``tolerance``
    (absolute rows) and/or ``tolerance_pct``.
    """
    source_count = ctx.table(rule.table, rule.param("filter")).count()
    target_count = ctx.table(rule.require("target_table"), rule.param("target_filter")).count()
    diff = abs(source_count - target_count)
    passed = _within_tolerance(Decimal(diff), Decimal(source_count), rule)
    details = f"source={source_count}, target={target_count}, diff={diff}"
    return CheckOutcome(source_count, min(diff, source_count), None, details, passed=passed)


@register_rule("sum_reconciliation", required=("column", "target_table"), dimension="consistency")
def check_sum_reconciliation(ctx: RuleContext, rule: Rule) -> CheckOutcome:
    """Compare ``SUM(column)`` of ``table`` with ``SUM(target_column)`` of ``target_table``.

    Table-level check (1 row checked). ``target_column`` defaults to ``column``.
    """
    from pyspark.sql import functions as F

    column = rule.require("column")
    target_column = rule.param("target_column", column)
    source = ctx.table(rule.table, rule.param("filter"))
    target = ctx.table(rule.require("target_table"), rule.param("target_filter"))
    source_sum = source.agg(F.sum(column)).first()[0] or 0
    target_sum = target.agg(F.sum(target_column)).first()[0] or 0

    s, t = Decimal(str(source_sum)), Decimal(str(target_sum))
    diff = abs(s - t)
    passed = _within_tolerance(diff, s, rule)
    details = f"source_sum={s}, target_sum={t}, diff={diff}"
    return CheckOutcome(1, 0 if passed else 1, None, details, passed=passed)


@register_rule("custom_sql", required=("sql",), dimension="validity")
def check_custom_sql(ctx: RuleContext, rule: Rule) -> CheckOutcome:
    """Run a SQL query that returns the *failing* rows.

    All tables are available as temporary views under their suite name,
    so the query can join several tables. ``rows_checked`` is the row count of
    ``table`` (after the optional ``filter``).
    """
    failed_rows = ctx.spark.sql(str(rule.require("sql")))
    checked = ctx.table(rule.table, rule.param("filter")).count()
    failed = failed_rows.count()
    return CheckOutcome(checked, min(failed, checked) if checked else failed, failed_rows)
