# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Bronze
# MAGIC
# MAGIC Land every source file in a Delta table exactly as it arrived.
# MAGIC
# MAGIC **Why Bronze stays raw.** No renames, no casts, no cleaning: every column is a string and every
# MAGIC bad row is kept. Bronze is the record of what the source actually sent. Any cleaning rule can
# MAGIC turn out to be wrong (a date format guessed wrong, a filter too aggressive), and if Bronze held
# MAGIC the cleaned version, the original would be gone.
# MAGIC
# MAGIC **What replaying from Bronze means.** Silver and Gold are derived. If a Silver rule changes, reset
# MAGIC the watermark and re-run Silver over Bronze. No re-export from the source is needed, and the
# MAGIC sources here could not re-send history anyway.
# MAGIC
# MAGIC Each row gets three audit columns: `_ingested_at`, `_source_file` and `_ingest_date`. The table is
# MAGIC partitioned by `_ingest_date` and only ever appended to.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath("../src"))

from lakehouse_utils import CHANNELS, RAW_VOLUME_PATH, add_ingest_metadata, fq, read_raw_csv

# COMMAND ----------

# MAGIC %md
# MAGIC Batch 1 is the initial 90 days. Batch 2 is the later `_batch2` files, for the incremental test in 02_silver.

# COMMAND ----------

dbutils.widgets.dropdown("batch", "1", ["1", "2"], "Source batch")
SUFFIX = "" if dbutils.widgets.get("batch") == "1" else "_batch2"

# COMMAND ----------

def ingest(source_name: str, table: str) -> int:
    """Append one landing file to its Bronze table, untouched apart from audit columns."""
    df = add_ingest_metadata(read_raw_csv(spark, f"{RAW_VOLUME_PATH}/{source_name}.csv"))
    (
        df.write.format("delta")
        .mode("append")
        .partitionBy("_ingest_date")
        .saveAsTable(fq(table))
    )
    return df.count()

for channel in CHANNELS:
    print(f"bronze_{channel}: {ingest(channel + SUFFIX, f'bronze_{channel}'):,} rows appended")
