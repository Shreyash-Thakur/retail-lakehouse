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
