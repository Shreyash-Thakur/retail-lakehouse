# Databricks notebook source
# MAGIC %md
# MAGIC # 04 Gold
# MAGIC
# MAGIC Two aggregate tables built from `silver_orders`, rebuilt in full (overwrite) on every run:
# MAGIC
# MAGIC - `gold_daily_sales`: order_date, channel, order_count, total_units, total_revenue
# MAGIC - `gold_sku_performance`: canonical_sku, product_name, category, total_units, total_revenue, first_sold, last_sold
# MAGIC
# MAGIC Silver keeps the tolerated dirt. Gold counts only rows with a positive quantity and amount, and
# MAGIC the SKU table also leaves out SKUs that never resolved.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath("../src"))

from lakehouse_utils import build_daily_sales, fq

silver = spark.table(fq("silver_orders"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## gold_daily_sales

# COMMAND ----------

(
    build_daily_sales(silver)
    .write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(fq("gold_daily_sales"))
)
