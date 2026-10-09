# Databricks notebook source
# MAGIC %md
# MAGIC # 05 Demos
# MAGIC
# MAGIC Three Delta features, each run against the real tables:
# MAGIC
# MAGIC 1. **Time travel**: read Silver as it was at an earlier version
# MAGIC 2. **Schema evolution**: a source adds a column, and Bronze takes it without a migration
# MAGIC 3. **Partition pruning**: a filter on the partition column skips files entirely
# MAGIC
# MAGIC Run 01 → 02 at least once (ideally with batch 2 as well) before this notebook.

# COMMAND ----------

import csv
import os
import sys

from pyspark.sql import functions as F

sys.path.append(os.path.abspath("../src"))

from lakehouse_utils import RAW_VOLUME_PATH, add_ingest_metadata, fq, read_raw_csv

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Time travel
# MAGIC
# MAGIC **What it proves:** every write to a Delta table creates a new version, and old versions stay readable
# MAGIC until they are vacuumed. Version 0 of `silver_orders` is the empty table 02_silver created. Later
# MAGIC versions are the MERGEs. Reading `VERSION AS OF` an old version gives the old count without any
# MAGIC backup. That is also how you recover from a bad write.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY lakehouse.retail.silver_orders

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) AS rows_at_version_0 FROM lakehouse.retail.silver_orders VERSION AS OF 0

# COMMAND ----------

latest = spark.sql("DESCRIBE HISTORY lakehouse.retail.silver_orders").agg(F.max("version")).first()[0]

counts = [
    (v, spark.sql(f"SELECT count(*) FROM lakehouse.retail.silver_orders VERSION AS OF {v}").first()[0])
    for v in range(latest + 1)
]
display(spark.createDataFrame(counts, "version long, row_count long"))

print(f"version 0: {counts[0][1]:,} rows, version {latest}: {counts[-1][1]:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Schema evolution
# MAGIC
# MAGIC **What it proves:** channel_a starts sending a `discount` column. With `mergeSchema` the append adds
# MAGIC the column to `bronze_channel_a`. Existing rows read it as null, and no ALTER TABLE or rewrite is needed.
# MAGIC Without `mergeSchema` the same append fails with a schema mismatch, which is the safe default.
# MAGIC
# MAGIC Silver is unaffected, because it selects named columns per channel and never sees `discount`.

# COMMAND ----------

import random

random.seed(11)

with open(f"{RAW_VOLUME_PATH}/channel_a_discount.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["order_id", "sku", "qty", "price", "order_date", "discount"])
    for i in range(200):
        qty = random.randint(1, 5)
        writer.writerow(
            [
                f"A{800_000 + i:07d}",
                f"A-{random.randint(1001, 1040)}",
                str(qty),
                f"{qty * 799:.2f}",
                f"2026-04-{random.randint(15, 20):02d}",
                random.choice(["0", "5", "10", "15"]),
            ]
        )

# COMMAND ----------

before_cols = spark.table(fq("bronze_channel_a")).columns
print("discount" in before_cols, before_cols)

# COMMAND ----------

(
    add_ingest_metadata(read_raw_csv(spark, f"{RAW_VOLUME_PATH}/channel_a_discount.csv"))
    .write.format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .partitionBy("_ingest_date")
    .saveAsTable(fq("bronze_channel_a"))
)

after_cols = spark.table(fq("bronze_channel_a")).columns
print("added:", [c for c in after_cols if c not in before_cols])

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Old files have no discount: those rows read it as null.
# MAGIC SELECT regexp_extract(_source_file, '[^/]+$', 0) AS source_file,
# MAGIC        count(*) AS rows,
# MAGIC        count(discount) AS rows_with_discount
# MAGIC FROM lakehouse.retail.bronze_channel_a
# MAGIC GROUP BY ALL
# MAGIC ORDER BY source_file

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Partition pruning
# MAGIC
# MAGIC **What it proves:** Bronze is partitioned by `_ingest_date`, so each load date is its own directory of
# MAGIC files. A filter on that column is resolved against the partition values before any file is opened,
# MAGIC and files from other dates are never read.
# MAGIC
# MAGIC In the physical plan, look at the scan node for **`PartitionFilters: [isnotnull(_ingest_date), (_ingest_date = ...)]`**.
# MAGIC A filter on an ordinary column shows up under `PushedFilters` / `DataFilters` instead, and that only
# MAGIC skips row groups inside files that still have to be opened.

# COMMAND ----------

latest_ingest = spark.table(fq("bronze_channel_a")).agg(F.max("_ingest_date")).first()[0]

pruned = spark.table(fq("bronze_channel_a")).filter(F.col("_ingest_date") == F.lit(latest_ingest))
pruned.explain(mode="formatted")

# COMMAND ----------

# For contrast: filtering on a non-partition column. No PartitionFilters, so every file is a candidate.
spark.table(fq("bronze_channel_a")).filter(F.col("sku") == "A-1001").explain(mode="formatted")
