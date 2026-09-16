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

# The SKU master is small and may change, so it is re-landed with every batch. Silver uses the newest row per code.
print(f"bronze_sku_master: {ingest('sku_master', 'bronze_sku_master'):,} rows appended")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Row counts

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'bronze_channel_a' AS table_name, count(*) AS row_count FROM lakehouse.retail.bronze_channel_a
# MAGIC UNION ALL SELECT 'bronze_channel_b', count(*) FROM lakehouse.retail.bronze_channel_b
# MAGIC UNION ALL SELECT 'bronze_channel_c', count(*) FROM lakehouse.retail.bronze_channel_c
# MAGIC UNION ALL SELECT 'bronze_channel_d', count(*) FROM lakehouse.retail.bronze_channel_d
# MAGIC UNION ALL SELECT 'bronze_sku_master', count(*) FROM lakehouse.retail.bronze_sku_master

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta metadata
# MAGIC
# MAGIC `DESCRIBE DETAIL` shows the format, location, partition columns and file count. The table is
# MAGIC Parquet files plus a `_delta_log` of JSON commits.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL lakehouse.retail.bronze_channel_a
