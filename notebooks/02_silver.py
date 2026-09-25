# Databricks notebook source
# MAGIC %md
# MAGIC # 02 Silver
# MAGIC
# MAGIC The four Bronze tables become one conformed `silver_orders` table:
# MAGIC
# MAGIC 1. Normalise column names to `order_id, channel, sku_raw, quantity, amount, order_date, meta`
# MAGIC 2. Parse four date formats into one `DATE` with a `coalesce` over parse attempts, not a branch per channel
# MAGIC 3. Cast `quantity` to int and `amount` to `decimal(12,2)`
# MAGIC 4. Join the SKU master for `canonical_sku` and `category`. Unresolved SKUs are kept and flagged
# MAGIC 5. Deduplicate on `order_id`, keeping the latest row
# MAGIC 6. Stamp `_processed_at`
# MAGIC
# MAGIC The transformation code lives in `src/lakehouse_utils.py`, where it is unit tested.

# COMMAND ----------

# MAGIC %md
# MAGIC ## The dedup window
# MAGIC
# MAGIC ```python
# MAGIC Window.partitionBy("order_id").orderBy(col("_ingested_at").desc(), col("order_date").desc())
# MAGIC row_number().over(window) == 1
# MAGIC ```
# MAGIC
# MAGIC `partitionBy` groups every copy of one order. `orderBy ... desc` puts the newest first: the latest
# MAGIC Bronze load, and within one load the later order date (the generator's re-sent copies are nudged
# MAGIC forward). `row_number` numbers the copies 1, 2, 3 and only row 1 is kept.
# MAGIC
# MAGIC It is `row_number`, not `rank`, on purpose. `rank` gives tied rows the same number, so two identical
# MAGIC copies would both survive. `row_number` always produces exactly one row per key.
# MAGIC That matters for the next step.

# COMMAND ----------

# MAGIC %md
# MAGIC ## The MERGE
# MAGIC
# MAGIC ```sql
# MAGIC MERGE INTO silver_orders t USING batch s ON t.order_id = s.order_id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *
# MAGIC ```
# MAGIC
# MAGIC An append writes every incoming row, so processing the same data twice doubles it. MERGE upserts on
# MAGIC the key: an order already in Silver is updated in place, and a new one is inserted. Running the same
# MAGIC batch twice leaves the table as it was, which makes the job **idempotent** and so safe to retry.
# MAGIC
# MAGIC MERGE fails if two source rows match the same target row, which is why the dedup above has to
# MAGIC guarantee one row per `order_id`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## The watermark
# MAGIC
# MAGIC `_watermark` holds, per Bronze table, the latest `_ingested_at` Silver has processed. Each run reads
# MAGIC only Bronze rows newer than that, and moves the watermark forward **only after the MERGE commits**.
# MAGIC
# MAGIC If the job dies between the MERGE and the watermark update, the next run reads the same rows again.
# MAGIC Because the MERGE is idempotent, re-processing them changes nothing. Moving the watermark first would
# MAGIC be the dangerous order: a crash would then skip rows for good.

# COMMAND ----------

import os
import sys

from delta.tables import DeltaTable
from pyspark.sql import functions as F

sys.path.append(os.path.abspath("../src"))

from lakehouse_utils import (
    CHANNELS,
    ensure_watermark_table,
    fq,
    prepare_silver_batch,
    read_since_watermark,
    set_watermark,
    slice_stats,
)

SILVER = fq("silver_orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Read only what is new

# COMMAND ----------

ensure_watermark_table(spark)

bronze, new_rows, new_max = {}, {}, {}
for channel in CHANNELS:
    table = f"bronze_{channel}"
    bronze[channel] = read_since_watermark(spark, table)
    new_rows[table], new_max[table] = slice_stats(bronze[channel])
    print(f"{table}: {new_rows[table]:,} new rows")

if sum(new_rows.values()) == 0:
    dbutils.notebook.exit("0 new Bronze rows since the watermark. Nothing to process.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Conform, type, resolve SKUs, deduplicate

# COMMAND ----------

batch = prepare_silver_batch(bronze, spark.table(fq("bronze_sku_master")))
display(batch.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. MERGE into Silver

# COMMAND ----------

if not spark.catalog.tableExists(SILVER):
    # Version 0 is the empty table, which gives 05_demos a clean baseline for time travel.
    batch.limit(0).write.format("delta").saveAsTable(SILVER)

before = spark.table(SILVER).count()

(
    DeltaTable.forName(spark, SILVER)
    .alias("t")
    .merge(batch.alias("s"), "t.order_id = s.order_id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

after = spark.table(SILVER).count()
print(f"silver_orders: {before:,} -> {after:,} rows ({after - before:+,})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Advance the watermark
# MAGIC
# MAGIC Only now, after the MERGE has committed.

# COMMAND ----------

for table, max_ts in new_max.items():
    if max_ts is not None:
        set_watermark(spark, table, max_ts)

display(spark.table(fq("_watermark")).orderBy("table_name"))
