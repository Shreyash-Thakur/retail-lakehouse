# Databricks notebook source
# MAGIC %md
# MAGIC # 03 Quality checks
# MAGIC
# MAGIC Checks that run on a Silver batch **before** it is merged, and fail the run if any check fails.
# MAGIC
# MAGIC | Check | Rule | Limit |
# MAGIC |---|---|---|
# MAGIC | Completeness | `order_id` not null | 0% |
# MAGIC | Completeness | `order_date` not null (also catches unparseable dates) | 0% |
# MAGIC | Completeness | `canonical_sku` not null | 2% |
# MAGIC | Validity | `quantity` not negative | 1% |
# MAGIC | Validity | `amount` present and > 0 | 2% |
# MAGIC | Validity | `order_date` not in the future | 0% |
# MAGIC | Consistency | every `sku_raw` resolves to a `canonical_sku` | 2% |
# MAGIC | Volume | Silver's total after this MERGE within ±50% of the last good run | 50% |

# COMMAND ----------

import csv
import os
import sys

sys.path.append(os.path.abspath("../src"))

from lakehouse_utils import (
    CHANNELS,
    RAW_VOLUME_PATH,
    add_ingest_metadata,
    fq,
    prepare_silver_batch,
    read_raw_csv,
    read_since_watermark,
    run_quality_checks,
)
