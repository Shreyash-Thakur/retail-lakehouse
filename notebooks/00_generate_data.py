# Databricks notebook source
# MAGIC %md
# MAGIC # 00 Generate data
# MAGIC
# MAGIC Four channel exports that disagree on everything: column names, date formats and SKU codes.
# MAGIC Plus a SKU master that maps each channel's codes to one canonical SKU.
# MAGIC
# MAGIC | Source | Columns | Date | SKU |
# MAGIC |---|---|---|---|
# MAGIC | channel_a | order_id, sku, qty, price, order_date | `2026-03-14` | `A-1001` |
# MAGIC | channel_b | orderId, productCode, quantity, amount, date | `14/03/2026` | `PRD1001` |
# MAGIC | channel_c | id, item_sku, units, total, created_at, meta | `03-14-2026` | `itm_1001` |
# MAGIC | channel_d | ORDER_NO, SKU_CODE, QTY, AMT, DT | `14-Mar-2026` | `SKU1001` |
# MAGIC
# MAGIC The amount column is the line total in every channel, even where it is called `price`.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS lakehouse;
# MAGIC CREATE SCHEMA IF NOT EXISTS lakehouse.retail;
# MAGIC CREATE VOLUME IF NOT EXISTS lakehouse.retail.raw;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config
# MAGIC
# MAGIC One seed for the whole notebook, so every run writes byte-identical files. Batch 1 covers
# MAGIC 90 days; batch 2 is the two weeks after it and exists only to test the incremental load.

# COMMAND ----------

import csv
import json
import random
from datetime import date, datetime, timedelta

random.seed(42)

RAW_PATH = "/Volumes/lakehouse/retail/raw"

BATCH1_START, BATCH1_DAYS = date(2026, 1, 1), 90
BATCH2_START, BATCH2_DAYS = BATCH1_START + timedelta(days=BATCH1_DAYS), 14

# About 50,000 rows in batch 1 once duplicates are added, 5,000 in batch 2.
BATCH1_ROWS = {"channel_a": 15_000, "channel_b": 12_500, "channel_c": 11_500, "channel_d": 10_000}
BATCH2_ROWS = {channel: n // 10 for channel, n in BATCH1_ROWS.items()}
