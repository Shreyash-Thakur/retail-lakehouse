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
