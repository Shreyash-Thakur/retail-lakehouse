"""Shared helpers for the retail lakehouse notebooks.

Plain PySpark only, so everything here can be unit tested locally without a
Databricks workspace. Notebooks import it by putting ../src on sys.path.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


CATALOG = "lakehouse"
SCHEMA = "retail"
RAW_VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw"

CHANNELS = ("channel_a", "channel_b", "channel_c", "channel_d")


def fq(table: str) -> str:
    """Fully qualified Unity Catalog name for a table in the retail schema."""
    return f"{CATALOG}.{SCHEMA}.{table}"


# ---------------------------------------------------------------------------
# Bronze
# ---------------------------------------------------------------------------

# channel_c's meta column is JSON, which the csv module writes with doubled
# quotes. Spark's default escape is a backslash, so without this the JSON gets
# split across several columns.
CSV_OPTIONS = {"header": "true", "quote": '"', "escape": '"'}


def read_raw_csv(spark, path: str) -> DataFrame:
    """Read a landing CSV exactly as-is: every column a string, nothing renamed."""
    return spark.read.options(**CSV_OPTIONS).csv(path)


def add_ingest_metadata(df: DataFrame) -> DataFrame:
    """Bronze audit columns. The file path comes from the hidden _metadata column."""
    return (
        df.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingest_date", F.current_date())
    )


# ---------------------------------------------------------------------------
# Silver: conforming the four channels
# ---------------------------------------------------------------------------
