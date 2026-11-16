"""Unit tests for every built-in rule type on tiny, hand-made DataFrames."""

from datetime import date, datetime, timedelta
from decimal import Decimal

import pytest

from dq.config import load_suite
from dq.rules import RuleContext, get_rule_type
from tests.conftest import RUN_TS


def evaluate(spark, tables, **rule_fields):
    rule = {"id": "R", "table": "t", **rule_fields}
    parsed = load_suite({"rules": [rule]}).rules[0]
    for name, df in tables.items():
        df.createOrReplaceTempView(name)
    ctx = RuleContext(spark, tables, RUN_TS)
    return get_rule_type(parsed.type).func(ctx, parsed)


@pytest.fixture(scope="module")
def people(spark):
    return spark.createDataFrame(
        [
            (1, "anna@example.com", "DE", 30, "  "),
            (2, None, "AT", 17, "x"),
            (3, "bad-email", "XX", 45, None),
            (3, "carl@example.com", "de", 130, "y"),
            (None, "dora@example.com", None, None, "z"),
        ],
        "id INT, email STRING, country STRING, age INT, note STRING",
    )


def test_not_null_single_column(spark, people):
    out = evaluate(spark, {"t": people}, type="not_null", column="email")
    assert (out.rows_checked, out.rows_failed) == (5, 1)
    assert out.failed_rows.count() == 1


def test_not_null_multiple_columns(spark, people):
    out = evaluate(spark, {"t": people}, type="not_null", columns=["id", "email", "country"])
    assert out.rows_failed == 2


def test_not_null_blank_as_null(spark, people):
    out = evaluate(spark, {"t": people}, type="not_null", column="note", treat_blank_as_null=True)
    assert out.rows_failed == 2


def test_not_null_with_filter(spark, people):
    out = evaluate(spark, {"t": people}, type="not_null", column="email", filter="age < 18")
    assert (out.rows_checked, out.rows_failed) == (1, 1)


def test_unique_flags_all_duplicated_rows_and_ignores_null_keys(spark, people):
    out = evaluate(spark, {"t": people}, type="unique", column="id")
    assert (out.rows_checked, out.rows_failed) == (5, 2)
    assert sorted(r["email"] for r in out.failed_rows.collect()) == [
        "bad-email",
        "carl@example.com",
    ]
    assert "1 duplicated key" in out.details


def test_unique_composite_key(spark, people):
    out = evaluate(spark, {"t": people}, type="unique", columns=["id", "email"])
    assert out.rows_failed == 0


def test_accepted_values_ignores_nulls_by_default(spark, people):
    out = evaluate(
        spark, {"t": people}, type="accepted_values", column="country", values=["DE", "AT"]
    )
    assert out.rows_failed == 2  # XX and lower-case "de"


def test_accepted_values_case_insensitive_and_nulls(spark, people):
    out = evaluate(
        spark,
        {"t": people},
        type="accepted_values",
        column="country",
        values=["DE", "AT"],
        case_insensitive=True,
        ignore_nulls=False,
    )
    assert out.rows_failed == 2  # XX and NULL


def test_range_inclusive(spark, people):
    out = evaluate(spark, {"t": people}, type="range", column="age", min=18, max=120)
    assert out.rows_failed == 2  # 17 and 130


def test_range_exclusive_and_only_min(spark, people):
    out = evaluate(spark, {"t": people}, type="range", column="age", min=30, inclusive=False)
    assert out.rows_failed == 2  # 17 and 30


def test_range_on_dates(spark):
    df = spark.createDataFrame([(date(2020, 1, 1),), (date(2031, 1, 1),)], "d DATE")
    out = evaluate(spark, {"t": df}, type="range", column="d", max="2030-12-31")
    assert out.rows_failed == 1


def test_regex(spark, people):
    out = evaluate(
        spark, {"t": people}, type="regex", column="email", pattern=r"^[^@\s]+@[^@\s]+\.[a-z]{2,}$"
    )
    assert (out.rows_checked, out.rows_failed) == (5, 1)


def test_referential_integrity(spark):
    orders = spark.createDataFrame([(1, 10), (2, 11), (3, 99), (4, None)], "id INT, cust INT")
    customers = spark.createDataFrame([(10,), (11,)], "customer_id INT")
    out = evaluate(
        spark,
        {"t": orders, "customers": customers},
        type="referential_integrity",
        column="cust",
        reference_table="customers",
        reference_column="customer_id",
    )
    assert (out.rows_checked, out.rows_failed) == (4, 1)
    assert out.failed_rows.collect()[0]["id"] == 3
    # failed_rows keeps the original columns only
    assert out.failed_rows.columns == ["id", "cust"]


def test_freshness_pass_and_fail(spark):
    df = spark.createDataFrame([(RUN_TS - timedelta(hours=5),)], "ts TIMESTAMP")
    ok = evaluate(spark, {"t": df}, type="freshness", column="ts", max_age_hours=6)
    late = evaluate(spark, {"t": df}, type="freshness", column="ts", max_age_hours=4)
    assert (ok.passed, ok.rows_checked, ok.rows_failed) == (True, 1, 0)
    assert (late.passed, late.rows_failed) == (False, 1)
    assert "age=5.0h" in late.details


def test_freshness_with_date_column_and_empty_table(spark):
    dates = spark.createDataFrame([(date(2026, 1, 15),)], "d DATE")
    assert evaluate(spark, {"t": dates}, type="freshness", column="d", max_age_hours=24).passed
    empty = spark.createDataFrame([], "d DATE")
    out = evaluate(spark, {"t": empty}, type="freshness", column="d", max_age_hours=24)
    assert out.passed is False


@pytest.mark.parametrize(
    "params, passed",
    [
        ({}, False),
        ({"tolerance": 1}, True),
        ({"tolerance_pct": 25}, True),
        ({"tolerance_pct": 10}, False),
    ],
)
def test_row_count_reconciliation(spark, params, passed):
    src = spark.range(4).toDF("id")
    tgt = spark.range(3).toDF("id")
    out = evaluate(
        spark,
        {"t": src, "target": tgt},
        type="row_count_reconciliation",
        target_table="target",
        **params,
    )
    assert out.passed is passed
    assert (out.rows_checked, out.rows_failed) == (4, 1)
    assert "diff=1" in out.details


def test_sum_reconciliation(spark):
    src = spark.createDataFrame([(Decimal("10.00"),), (Decimal("5.50"),)], "amount DECIMAL(10,2)")
    tgt = spark.createDataFrame([(Decimal("15.40"),)], "total DECIMAL(10,2)")
    tables = {"t": src, "target": tgt}
    common = {"type": "sum_reconciliation", "column": "amount", "target_table": "target"}
    exact = evaluate(spark, tables, target_column="total", **common)
    tolerant = evaluate(spark, tables, target_column="total", tolerance=0.1, **common)
    assert exact.passed is False and exact.rows_failed == 1
    assert tolerant.passed is True and tolerant.rows_failed == 0
    assert "diff=0.10" in exact.details


def test_custom_sql_can_join_tables(spark):
    orders = spark.createDataFrame([(1, 10, 5.0), (2, 11, 50.0)], "id INT, cust INT, amount DOUBLE")
    limits = spark.createDataFrame([(10, 100.0), (11, 20.0)], "cust INT, max_amount DOUBLE")
    out = evaluate(
        spark,
        {"t": orders, "limits": limits},
        type="custom_sql",
        sql="SELECT o.* FROM t o JOIN limits l USING (cust) WHERE o.amount > l.max_amount",
    )
    assert (out.rows_checked, out.rows_failed) == (2, 1)


def test_unknown_table_raises(spark, people):
    with pytest.raises(KeyError, match="was not provided"):
        evaluate(
            spark, {"t": people}, type="referential_integrity", column="id", reference_table="x"
        )


def test_custom_rule_registration(spark, people):
    from pyspark.sql import functions as F

    from dq.rules import _REGISTRY, register_rule, row_level_check

    @register_rule("even_id", required=("column",), dimension="validity")
    def check_even(ctx, rule):
        df = ctx.table(rule.table)
        return row_level_check(df, F.col(rule.require("column")) % 2 != 0)

    try:
        out = evaluate(spark, {"t": people}, type="even_id", column="id")
        assert out.rows_failed == 3  # ids 1, 3, 3 (NULL ignored)
    finally:
        _REGISTRY.pop("even_id")


def test_row_level_check_treats_null_condition_as_pass(spark):
    from pyspark.sql import functions as F

    from dq.rules import row_level_check

    df = spark.createDataFrame([(1,), (None,)], "x INT")
    out = row_level_check(df, F.col("x") > 0)
    assert (out.rows_checked, out.rows_failed) == (2, 1)


def test_datetime_freshness_ignores_future_rows(spark):
    # A future timestamp means age is negative -> still fresh.
    df = spark.createDataFrame(
        [(RUN_TS + timedelta(hours=1),), (datetime(2020, 1, 1),)], "ts TIMESTAMP"
    )
    assert evaluate(spark, {"t": df}, type="freshness", column="ts", max_age_hours=1).passed


def test_readme_custom_rule_example(spark):
    """The 'Adding a new rule type' example from the README must work."""
    from pyspark.sql import functions as F

    from dq.rules import _REGISTRY, register_rule, row_level_check

    @register_rule("not_in_future", required=("column",), dimension="validity")
    def check_not_in_future(ctx, rule):
        df = ctx.table(rule.table, rule.param("filter"))
        column = F.col(rule.require("column"))
        return row_level_check(df, column > F.lit(ctx.run_ts))

    try:
        df = spark.createDataFrame(
            [(RUN_TS - timedelta(days=1),), (RUN_TS + timedelta(days=1),)], "ts TIMESTAMP"
        )
        out = evaluate(spark, {"t": df}, type="not_in_future", column="ts")
        assert (out.rows_checked, out.rows_failed) == (2, 1)
    finally:
        _REGISTRY.pop("not_in_future")
