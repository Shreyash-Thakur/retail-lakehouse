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
# MAGIC
# MAGIC **Why some rules tolerate a little.** The generator puts about 1% unknown SKUs, 0.5% negative
# MAGIC quantities and 1% null prices into every batch on purpose. Silver keeps those rows, flagged, and the
# MAGIC limits sit just above those rates. A file that is broken in a new way blows straight through them.
# MAGIC Keys and dates tolerate nothing: a row with no order ID cannot be merged.
# MAGIC
# MAGIC **Why volume compares Silver's total, not the batch size.** An incremental batch is a tenth the size
# MAGIC of the first load, and a re-run with nothing new is zero, so comparing batch sizes would fail every
# MAGIC normal run. What should not happen is Silver suddenly halving or doubling.
# MAGIC
# MAGIC **Why fail the run instead of quarantining bad rows.** A failed run is loud, and a quarantine table
# MAGIC nobody reads is quiet. At this size, stopping and looking is cheaper than building the quarantine
# MAGIC flow. Every result, pass or fail, is written to `_quality_log` before anything is raised.

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checks on the pending batch
# MAGIC
# MAGIC Whatever 02_silver would process next: every Bronze row newer than the watermark. If the watermark
# MAGIC is up to date there is nothing to check.

# COMMAND ----------

sku_master = spark.table(fq("bronze_sku_master"))
pending = prepare_silver_batch(
    {channel: read_since_watermark(spark, f"bronze_{channel}") for channel in CHANNELS}, sku_master
)

if pending.isEmpty():
    print("Nothing pending: Silver is up to date with Bronze.")
else:
    for r in run_quality_checks(spark, pending):
        print(f"{'PASS' if r.passed else 'FAIL'}  {r.check}.{r.rule}: {r.detail}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT run_id, check_name, rule, passed, detail, checked_at
# MAGIC FROM lakehouse.retail._quality_log
# MAGIC ORDER BY checked_at DESC, check_name, rule
# MAGIC LIMIT 32

# COMMAND ----------

# MAGIC %md
# MAGIC ## A deliberately broken file
# MAGIC
# MAGIC A channel_a export where 10% of rows have no order ID and 15% have negative quantities. It goes
# MAGIC through the same read, the same Silver transformation and the same checks as a real file. It is not
# MAGIC appended to Bronze, so the real tables stay clean.

# COMMAND ----------

import random

random.seed(7)

BROKEN_PATH = f"{RAW_VOLUME_PATH}/broken"
os.makedirs(BROKEN_PATH, exist_ok=True)

with open(f"{BROKEN_PATH}/channel_a_broken.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["order_id", "sku", "qty", "price", "order_date"])
    for i in range(200):
        qty = random.randint(1, 5)
        writer.writerow(
            [
                "" if i % 10 == 0 else f"A{900_000 + i:07d}",
                f"A-{random.randint(1001, 1040)}",
                str(-qty if i % 7 == 1 else qty),
                f"{qty * 499:.2f}",
                f"2026-04-{random.randint(1, 14):02d}",
            ]
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run the pipeline on it
# MAGIC
# MAGIC Expected: `QualityCheckFailed` naming the failing rules with their counts. The run stops here, so the
# MAGIC MERGE never happens.

# COMMAND ----------

broken_bronze = add_ingest_metadata(read_raw_csv(spark, f"{BROKEN_PATH}/channel_a_broken.csv"))
broken_batch = prepare_silver_batch({"channel_a": broken_bronze}, sku_master)

run_quality_checks(spark, broken_batch)
