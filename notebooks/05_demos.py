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
