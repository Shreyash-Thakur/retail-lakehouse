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
