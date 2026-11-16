"""Store run results as Delta or Parquet, and read the history back for trends.

Three datasets are written (always appended, one row set per run):

* ``dq_results``        - one row per rule per run
* ``dq_failed_samples`` - up to N failing rows per rule per run (as JSON)
* ``dq_scores``         - quality indices per suite / table / dimension per run

Targets:

* a **path** (local folder, DBFS, S3, ADLS, ...) -> ``<path>/<dataset>``
* a **database / schema** (Databricks, Hive metastore, Unity Catalog)
  -> ``<database>.<dataset>`` managed tables

Windows note: plain local PySpark on Windows cannot write files without the
Hadoop ``winutils`` binaries. When those are missing, Parquet output is written
with pyarrow instead (the result tables are small), so local runs work
out of the box. Delta and Linux/Databricks always use Spark writers.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame, SparkSession

    from dq.engine import RunResult

logger = logging.getLogger(__name__)

RESULTS = "dq_results"
SAMPLES = "dq_failed_samples"
SCORES = "dq_scores"
DATASETS = (RESULTS, SAMPLES, SCORES)


def _windows_without_hadoop() -> bool:
    return os.name == "nt" and not os.environ.get("HADOOP_HOME")


class ResultStore:
    """Append run results to a folder or to catalog tables."""

    def __init__(
        self,
        spark: SparkSession,
        path: str | None = "dq_output",
        format: str = "parquet",
        database: str | None = None,
    ):
        if not path and not database:
            raise ValueError("ResultStore needs a 'path' or a 'database'.")
        self.spark = spark
        self.path = path
        self.format = format
        self.database = database

    @classmethod
    def from_suite(cls, spark: SparkSession, suite) -> ResultStore:
        out = suite.output
        return cls(spark, path=out.path, format=out.format, database=out.database)

    @property
    def use_local_fallback(self) -> bool:
        return (
            self.database is None
            and self.format == "parquet"
            and _windows_without_hadoop()
            and "://" not in str(self.path)
        )

    def location(self, dataset: str) -> str:
        if self.database:
            return f"{self.database}.{dataset}"
        return f"{str(self.path).rstrip('/')}/{dataset}"

    # ------------------------------------------------------------------ write
    def save(self, result: RunResult) -> dict[str, str]:
        """Append results, failed samples and scores of one run. Returns the locations."""
        frames = {
            RESULTS: (result.result_rows, result.results),
            SAMPLES: (result.sample_rows, result.failed_samples),
            SCORES: (result.score_rows, result.scores),
        }
        written = {}
        for dataset, (rows, df) in frames.items():
            if not rows:
                continue
            self._append(dataset, rows, df, result.run_id)
            written[dataset] = self.location(dataset)
        logger.info("Stored run %s in %s", result.run_id, written)
        return written

    def _append(self, dataset: str, rows: list[dict], df: DataFrame, run_id: str) -> None:
        if self.use_local_fallback:
            import pyarrow as pa
            import pyarrow.parquet as pq

            folder = Path(self.location(dataset))
            folder.mkdir(parents=True, exist_ok=True)
            pq.write_table(pa.Table.from_pylist(rows), folder / f"part-{run_id}.parquet")
            return
        writer = df.write.format(self.format).mode("append")
        if self.database:
            writer.saveAsTable(self.location(dataset))
        else:
            writer.save(self.location(dataset))

    # ------------------------------------------------------------------ read
    def read(self, dataset: str) -> DataFrame | None:
        """Read a stored dataset, or ``None`` if nothing was stored yet."""
        if self.use_local_fallback:
            folder = Path(self.location(dataset))
            files = sorted(folder.glob("*.parquet")) if folder.exists() else []
            if not files:
                return None
            import pyarrow.parquet as pq

            from dq.engine import results_schema, samples_schema, scores_schema

            schemas = {RESULTS: results_schema, SAMPLES: samples_schema, SCORES: scores_schema}
            rows = [row for f in files for row in pq.read_table(f).to_pylist()]
            return self.spark.createDataFrame(rows, schemas[dataset]())
        try:
            if self.database:
                return self.spark.table(self.location(dataset))
            return self.spark.read.format(self.format).load(self.location(dataset))
        except Exception as exc:  # table/path does not exist yet
            if "not found" in str(exc).lower() or "not exist" in str(exc).lower():
                return None
            if "PATH_NOT_FOUND" in str(exc) or "TABLE_OR_VIEW_NOT_FOUND" in str(exc):
                return None
            raise

    def score_history(self, suite: str | None = None) -> list[dict[str, Any]]:
        """All stored scores (optionally for one suite) as Python dicts."""
        df = self.read(SCORES)
        if df is None:
            return []
        if suite is not None:
            df = df.where(df["suite"] == suite)
        return [row.asDict() for row in df.collect()]
