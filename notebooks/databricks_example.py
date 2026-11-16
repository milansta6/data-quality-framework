# Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality Framework – Databricks example
# MAGIC
# MAGIC This notebook shows how to run config-driven data quality rules on Databricks:
# MAGIC
# MAGIC 1. install the framework
# MAGIC 2. load (synthetic) tables
# MAGIC 3. run the rules from YAML
# MAGIC 4. store results as **Delta tables** and look at the quality indices
# MAGIC 5. use the result as a **quality gate** in a Bronze → Silver → Gold pipeline
# MAGIC
# MAGIC Works on any cluster with Databricks Runtime 13+ (and on serverless).
# MAGIC Easiest setup: clone this repository as a **Git folder** (Repos) and open this notebook from there.

# COMMAND ----------

# MAGIC %md ## 1. Install
# MAGIC Install from GitHub (or from the cloned Git folder with `%pip install -e ..`).
# MAGIC PySpark is not installed again: the cluster's Spark is used.

# COMMAND ----------

# MAGIC %pip install git+https://github.com/milansta6/data-quality-framework.git

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md ## 2. Parameters

# COMMAND ----------

dbutils.widgets.text("results_schema", "main.data_quality", "Schema for DQ result tables")
dbutils.widgets.dropdown("fail_on", "never", ["never", "warning", "error"], "Stop the job on")

RESULTS_SCHEMA = dbutils.widgets.get("results_schema")
FAIL_ON = dbutils.widgets.get("fail_on")

# COMMAND ----------

# MAGIC %md ## 3. Load data
# MAGIC Here we create synthetic demo tables. In a real pipeline you would use
# MAGIC `spark.table("main.silver.customers")` etc.

# COMMAND ----------

from dq.demo import create_demo_tables

tables = create_demo_tables(spark)
# Real data instead:
# tables = {
#     "customers": spark.table("main.silver.customers"),
#     "accounts": spark.table("main.silver.accounts"),
#     "transactions_raw": spark.table("main.bronze.transactions"),
#     "transactions": spark.table("main.silver.transactions"),
# }
display(tables["customers"].limit(5))

# COMMAND ----------

# MAGIC %md ## 4. Define the rules
# MAGIC Rules live in YAML. In a Git folder you can point to the file in the repo;
# MAGIC you can also keep the YAML inline (handy for quick experiments).

# COMMAND ----------

import os

from dq import load_suite

suite_path = "../examples/demo_suite.yml"
if os.path.exists(suite_path):
    suite = load_suite(suite_path)
else:
    suite = load_suite("""
suite: retail_demo_inline
rules:
  - {id: CUST_001, table: customers, type: not_null, columns: [customer_id, email]}
  - {id: CUST_003, table: customers, type: unique, column: customer_id}
  - id: ACC_001
    table: accounts
    type: referential_integrity
    column: customer_id
    reference_table: customers
  - id: REC_001
    table: transactions_raw
    type: row_count_reconciliation
    target_table: transactions
    severity: warning
""")

print(f"{len(suite.active_rules)} active rules on {suite.table_names}")

# COMMAND ----------

# MAGIC %md ## 5. Run

# COMMAND ----------

from dq import DQEngine

result = DQEngine(spark, suite).run(tables)
print(result.summary())
display(
    result.results.select(
        "status", "rule_id", "table_name", "dimension", "rows_checked", "rows_failed", "pass_rate",
        "details",
    ).orderBy("status", "rule_id")
)

# COMMAND ----------

# MAGIC %md ## 6. Quality indices – one number per dataset

# COMMAND ----------

display(result.scores.where("scope IN ('suite', 'table')"))

# COMMAND ----------

display(
    result.scores.where("scope = 'table_dimension'")
    .groupBy("table_name")
    .pivot("dimension")
    .sum("score")
)

# COMMAND ----------

# MAGIC %md ## 7. Store results as Delta tables
# MAGIC Results are appended, so every run adds to the history (used for trends and dashboards).

# COMMAND ----------

from dq import ResultStore

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {RESULTS_SCHEMA}")
store = ResultStore(spark, database=RESULTS_SCHEMA, format="delta")
print(store.save(result))

# COMMAND ----------

# MAGIC %md ## 8. Trend over time

# COMMAND ----------

display(
    spark.table(f"{RESULTS_SCHEMA}.dq_scores")
    .where("scope = 'table'")
    .select("run_ts", "table_name", "score")
    .orderBy("run_ts")
)

# COMMAND ----------

# MAGIC %md ## 9. HTML report
# MAGIC Written to a Unity Catalog volume (or DBFS); download it or attach it to an e-mail alert.

# COMMAND ----------

from dq import render_html_report

report_dir = "/Volumes/main/data_quality/reports"  # create the volume once, or change the path
try:
    path = render_html_report(
        result, f"{report_dir}/dq_report_{result.run_id}.html", store.score_history(suite.name)
    )
    print(f"Report written to {path}")
except OSError as exc:
    print(f"Could not write report ({exc}); showing it inline instead.")
    path = render_html_report(result, f"/tmp/dq_report_{result.run_id}.html")

displayHTML(path.read_text())

# COMMAND ----------

# MAGIC %md ## 10. Quality gate
# MAGIC In a job, stop the pipeline before bad data reaches the next layer.

# COMMAND ----------

if FAIL_ON != "never":
    result.raise_for_status(fail_on=FAIL_ON)
