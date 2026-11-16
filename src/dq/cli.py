"""Command line interface.

Examples::

    dq validate examples/demo_suite.yml
    dq run examples/demo_suite.yml --demo-data
    dq run my_suite.yml --output /data/dq --format delta --fail-on error
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from dq.config import ConfigError, load_suite
from dq.rules import available_rule_types


def _validate(args: argparse.Namespace) -> int:
    suite = load_suite(args.config)
    print(
        f"OK: suite '{suite.name}' has {len(suite.rules)} rules "
        f"({len(suite.active_rules)} active) on tables: {', '.join(suite.table_names)}"
    )
    return 0


def _run(args: argparse.Namespace) -> int:
    from dq.engine import DQEngine
    from dq.report import render_html_report
    from dq.spark import get_spark
    from dq.storage import ResultStore

    suite = load_suite(args.config)
    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")

    engine = DQEngine(spark, suite)
    if args.demo_data:
        from dq.demo import create_demo_tables

        tables = create_demo_tables(spark)
    else:
        tables = engine.load_tables()
    result = engine.run(tables)

    output = args.output or suite.output.path
    store = ResultStore(
        spark,
        path=output,
        format=args.format or suite.output.format,
        database=None if args.output else suite.output.database,
    )
    locations = store.save(result)
    history = store.score_history(suite.name)

    print(result.summary())
    for row in result.result_rows:
        print(
            f"  {row['status']:<5} {row['rule_id']:<12} {row['table_name']:<18} "
            f"{_fmt_rate(row['pass_rate'])}  {row['details'] or row['error_message'] or ''}"
        )
    for name, loc in locations.items():
        print(f"Saved {name:<18} -> {loc}")

    if suite.output.report and not args.no_report:
        default_report = Path(output or ".") / "reports" / f"dq_report_{result.run_id}.html"
        report_path = Path(args.report) if args.report else default_report
        render_html_report(result, report_path, history)
        print(f"HTML report             -> {report_path}")

    if args.fail_on != "never":
        blocking = ("FAIL", "ERROR", "WARN") if args.fail_on == "warning" else ("FAIL", "ERROR")
        if result.failed_rules(blocking):
            return 1
    return 0


def _fmt_rate(rate: float | None) -> str:
    return "pass_rate=   -   " if rate is None else f"pass_rate={rate * 100:7.2f}%"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="dq", description="Config-driven data quality checks.")
    parser.add_argument("-v", "--verbose", action="store_true", help="show debug logging")
    sub = parser.add_subparsers(dest="command", required=True)

    p_val = sub.add_parser("validate", help="validate a YAML suite (no Spark needed)")
    p_val.add_argument("config", help="path to the suite YAML file")
    p_val.set_defaults(func=_validate)

    p_run = sub.add_parser("run", help="run a suite and store the results")
    p_run.add_argument("config", help="path to the suite YAML file")
    p_run.add_argument("--output", help="output folder (overrides the YAML)")
    p_run.add_argument("--format", choices=["parquet", "delta"], help="storage format")
    p_run.add_argument("--report", help="path of the HTML report file")
    p_run.add_argument("--no-report", action="store_true", help="skip the HTML report")
    p_run.add_argument(
        "--demo-data", action="store_true", help="use the built-in synthetic demo tables"
    )
    p_run.add_argument(
        "--fail-on",
        choices=["error", "warning", "never"],
        default="error",
        help="exit with code 1 when rules of this severity fail (default: error)",
    )
    p_run.set_defaults(func=_run)

    sub.add_parser("rules", help="list available rule types").set_defaults(
        func=lambda _: print("\n".join(available_rule_types())) or 0
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.WARNING,
        format="%(levelname)s %(name)s: %(message)s",
    )
    try:
        return args.func(args)
    except ConfigError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
