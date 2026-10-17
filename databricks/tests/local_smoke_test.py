"""Run the pipeline's SQL with a local Spark, before deploying to Databricks.

Lakeflow syntax (STREAMING TABLE, read_files, expectations) only runs on
Databricks, so this script emulates it on the same files:
- each CREATE ... AS <query> in databricks/pipeline/*.sql runs as a batch query;
- read_files(...) is replaced by the local CSV read with the same options;
- each expectation is evaluated: DROP ROW filters the rows, WARN and FAIL
  UPDATE only count them (and FAIL makes the script exit non-zero);
- the queries of databricks/validation/validation.sql then run on the result.

It checks the SQL logic and the row counts, not the Databricks runtime.

Usage (from the repo root, Spark 3.5+):
    spark-submit databricks/tests/local_smoke_test.py \
        include/dataset/online_retail.csv \
        include/dbt/seeds/non_merchandise_stock_codes.csv
"""

import re
import sys
from pathlib import Path

from pyspark.sql import SparkSession

PIPELINE_DIR = Path(__file__).resolve().parents[1] / "pipeline"
VALIDATION_SQL = Path(__file__).resolve().parents[1] / "validation" / "validation.sql"
STATEMENT = re.compile(
    r"CREATE OR REFRESH (?:STREAMING TABLE|MATERIALIZED VIEW) (\S+)(.*?)\nAS (.*?);",
    re.DOTALL,
)
EXPECTATION = re.compile(
    r"CONSTRAINT (\w+) EXPECT \((.*?)\)\s*(?:ON VIOLATION (DROP ROW|FAIL UPDATE))?",
    re.DOTALL,
)
READ_FILES = re.compile(r"(?:STREAM )?read_files\(\s*'([^']+)'.*?\)", re.DOTALL)
STREAM_OF = re.compile(r"STREAM\((\S+?)\)")


def local_name(table: str) -> str:
    return table.replace(".", "_")


def main(sales_csv: str, codes_csv: str) -> int:
    spark = SparkSession.builder.appName("retail-smoke-test").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    landing = {"online_retail": sales_csv, "reference": codes_csv}
    failed_updates = 0

    for sql_file in sorted(PIPELINE_DIR.glob("*.sql")):
        text = re.sub(r"--[^\n]*", "", sql_file.read_text())
        for table, spec, query in STATEMENT.findall(text):
            query = re.sub(
                r"retail\.(\w+)\.(\w+)", r"retail_\1_\2", STREAM_OF.sub(r"\1", query)
            )
            for path in READ_FILES.findall(query):
                folder = next(key for key in landing if f"/{key}/" in path)
                spark.read.csv(
                    landing[folder], header=True, escape='"'
                ).createOrReplaceTempView(f"src_{folder}")
                query = READ_FILES.sub(f"src_{folder}", query, count=1)
            query = query.replace("_metadata.file_path", "'local'")

            source = spark.sql(query).cache()
            frame = source
            print(f"\n{table}: {source.count():,} rows before expectations")
            for name, condition, action in EXPECTATION.findall(spec):
                # Counted on all incoming rows: a row that breaks two rules
                # counts in both, as in the pipeline's data quality metrics.
                violations = source.filter(
                    f"NOT coalesce(({condition}), false)"
                ).count()
                label = action or "WARN"
                print(f"  {name:<24} {label:<11} {violations:>9,} violations")
                if action == "DROP ROW":
                    frame = frame.filter(f"coalesce(({condition}), false)")
                elif action == "FAIL UPDATE" and violations:
                    failed_updates += 1
            # Materialise, like the pipeline does: later queries read the result,
            # not the whole upstream plan.
            frame = frame.localCheckpoint()
            print(f"  -> {frame.count():,} rows written")
            frame.createOrReplaceTempView(local_name(table))

    # The validation queries run on Databricks, minus the event log (Databricks only).
    validation = re.sub(r"--[^\n]*", "", VALIDATION_SQL.read_text())
    for query in filter(str.strip, validation.split(";")):
        if "event_log(" in query:
            continue
        print()
        spark.sql(re.sub(r"retail\.(\w+)\.(\w+)", r"retail_\1_\2", query)).show(
            truncate=False
        )
    return 1 if failed_updates else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1], sys.argv[2]))
