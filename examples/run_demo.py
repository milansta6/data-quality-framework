"""Run the demo suite on synthetic data using the Python API.

    python examples/run_demo.py

This is the same as ``dq run examples/demo_suite.yml --demo-data`` but shows
how to use the framework from your own code or pipeline.
"""

from pathlib import Path

from dq import DQEngine, ResultStore, render_html_report
from dq.demo import create_demo_tables
from dq.spark import get_spark

HERE = Path(__file__).parent

spark = get_spark()
spark.sparkContext.setLogLevel("ERROR")

# 1. Your data: here synthetic DataFrames, in real life spark.table(...) or spark.read...
tables = create_demo_tables(spark)

# 2. Run the rules
engine = DQEngine(spark, HERE / "demo_suite.yml")
result = engine.run(tables)
print(result.summary())

# 3. Look at the results (Spark DataFrames)
result.results.select(
    "rule_id", "table_name", "dimension", "rows_checked", "rows_failed", "pass_rate", "status"
).show(truncate=False)
result.scores.where("scope IN ('suite', 'table')").show(truncate=False)

# 4. Store them (appends, so the trend grows with every run) and write the HTML report
store = ResultStore(spark, path="dq_output", format="parquet")
store.save(result)
report = render_html_report(
    result,
    f"dq_output/reports/dq_report_{result.run_id}.html",
    store.score_history(result.suite.name),
)
print(f"Report: {report.resolve()}")

# 5. In a pipeline you would stop here if blocking rules failed:
# result.raise_for_status(fail_on="error")
