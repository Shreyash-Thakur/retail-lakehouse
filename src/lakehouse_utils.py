"""Shared helpers for the retail lakehouse notebooks.

Plain PySpark only, so everything here can be unit tested locally without a
Databricks workspace. Notebooks import it by putting ../src on sys.path.
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T


CATALOG = "lakehouse"
SCHEMA = "retail"
RAW_VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw"

CHANNELS = ("channel_a", "channel_b", "channel_c", "channel_d")


def fq(table: str) -> str:
    """Fully qualified Unity Catalog name for a table in the retail schema."""
    return f"{CATALOG}.{SCHEMA}.{table}"


# ---------------------------------------------------------------------------
# Bronze
# ---------------------------------------------------------------------------

# channel_c's meta column is JSON, which the csv module writes with doubled
# quotes. Spark's default escape is a backslash, so without this the JSON gets
# split across several columns.
CSV_OPTIONS = {"header": "true", "quote": '"', "escape": '"'}


def read_raw_csv(spark, path: str) -> DataFrame:
    """Read a landing CSV exactly as-is: every column a string, nothing renamed."""
    return spark.read.options(**CSV_OPTIONS).csv(path)


def add_ingest_metadata(df: DataFrame) -> DataFrame:
    """Bronze audit columns. The file path comes from the hidden _metadata column."""
    return (
        df.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingest_date", F.current_date())
    )


# ---------------------------------------------------------------------------
# Silver: conforming the four channels
# ---------------------------------------------------------------------------

# Raw column -> conformed column, per channel. Anything not listed is dropped,
# which is also why a new Bronze column (see 05_demos) cannot break Silver.
COLUMN_MAP = {
    "channel_a": {
        "order_id": "order_id",
        "sku": "sku_raw",
        "qty": "quantity",
        "price": "amount",
        "order_date": "order_date",
    },
    "channel_b": {
        "orderId": "order_id",
        "productCode": "sku_raw",
        "quantity": "quantity",
        "amount": "amount",
        "date": "order_date",
    },
    "channel_c": {
        "id": "order_id",
        "item_sku": "sku_raw",
        "units": "quantity",
        "total": "amount",
        "created_at": "order_date",
        "meta": "meta",
    },
    "channel_d": {
        "ORDER_NO": "order_id",
        "SKU_CODE": "sku_raw",
        "QTY": "quantity",
        "AMT": "amount",
        "DT": "order_date",
    },
}

CONFORMED_COLUMNS = ("order_id", "channel", "sku_raw", "quantity", "amount", "order_date", "meta")
BRONZE_AUDIT_COLUMNS = ("_ingested_at", "_source_file")


def normalise_columns(df: DataFrame, channel: str) -> DataFrame:
    """Rename one channel's raw columns to the conformed names.

    Columns a channel does not have (only channel_c has meta) become typed
    nulls, so the four frames can be unioned by name.
    """
    renamed = {conformed: raw for raw, conformed in COLUMN_MAP[channel].items()}
    cols = []
    for name in CONFORMED_COLUMNS:
        if name == "channel":
            cols.append(F.lit(channel).alias(name))
        elif name in renamed:
            cols.append(F.col(f"`{renamed[name]}`").alias(name))
        else:
            cols.append(F.lit(None).cast("string").alias(name))
    audit = [F.col(c) for c in BRONZE_AUDIT_COLUMNS if c in df.columns]
    return df.select(*cols, *audit)


# One pattern per channel. No string in the data parses under more than one of
# them, so the order only matters for speed.
DATE_PATTERNS = ("yyyy-MM-dd", "dd/MM/yyyy", "MM-dd-yyyy", "dd-MMM-yyyy")


def parse_order_date(col: Column) -> Column:
    """Coalesce over one parse attempt per pattern instead of branching per channel.

    try_to_timestamp rather than to_date: with ANSI mode on (the default on
    serverless) to_date raises on the first string that does not match, and in
    a coalesce every pattern but one is a mismatch.
    """
    attempts = [F.try_to_timestamp(col, F.lit(p)).cast("date") for p in DATE_PATTERNS]
    return F.coalesce(*attempts)


def try_cast(col_name: str, type_name: str) -> Column:
    """SQL try_cast: a bad string becomes null instead of failing the job under ANSI."""
    return F.expr(f"try_cast(`{col_name}` AS {type_name})")


def cast_measures(df: DataFrame) -> DataFrame:
    """quantity -> int, amount -> decimal(12,2), order_date -> date."""
    return (
        df.withColumn("quantity", try_cast("quantity", "int"))
        .withColumn("amount", try_cast("amount", "decimal(12,2)"))
        .withColumn("order_date", parse_order_date(F.col("order_date")))
    )


META_SCHEMA = "promo_code STRING, source STRING"


def parse_meta(df: DataFrame) -> DataFrame:
    """Pull promo_code and source out of channel_c's JSON meta column.

    Other channels have a null meta, so they get nulls here too.
    """
    parsed = F.from_json(F.col("meta"), META_SCHEMA)
    return df.withColumn("promo_code", parsed["promo_code"]).withColumn(
        "order_source", parsed["source"]
    )


def deduplicate_latest(
    df: DataFrame, key: str = "order_id", order_by: tuple[str, ...] = ("_ingested_at",)
) -> DataFrame:
    """Keep one row per key: the first when sorted by order_by, descending.

    row_number rather than rank, so a tie still yields exactly one row. MERGE
    needs that: a source with two rows for one key makes the MERGE fail.

    Rows with a null key pass through untouched. The window would otherwise put
    them all in one partition and keep one, hiding from the completeness check
    how many rows arrived without a key.
    """
    window = Window.partitionBy(key).orderBy(*[F.col(c).desc_nulls_last() for c in order_by])
    deduped = (
        df.filter(F.col(key).isNotNull())
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    return deduped.unionByName(df.filter(F.col(key).isNull()))


def latest_sku_master(sku_master: DataFrame) -> DataFrame:
    """bronze_sku_master is append-only, so keep the newest row per channel_sku."""
    return deduplicate_latest(sku_master, key="channel_sku").select(
        "channel_sku", "canonical_sku", "product_name", "category"
    )


def resolve_skus(orders: DataFrame, sku_master: DataFrame) -> DataFrame:
    """Left join to the SKU master. Rows that do not resolve are kept and flagged."""
    master = sku_master.select(
        F.col("channel_sku").alias("_channel_sku"), "canonical_sku", "product_name", "category"
    )
    return (
        orders.join(master, orders["sku_raw"] == master["_channel_sku"], "left")
        .drop("_channel_sku")
        .withColumn("sku_resolved", F.col("canonical_sku").isNotNull())
    )


SILVER_COLUMNS = (
    "order_id",
    "channel",
    "sku_raw",
    "canonical_sku",
    "product_name",
    "category",
    "sku_resolved",
    "quantity",
    "amount",
    "order_date",
    "meta",
    "promo_code",
    "order_source",
    "_ingested_at",
    "_source_file",
    "_processed_at",
)


def prepare_silver_batch(bronze: dict[str, DataFrame], sku_master: DataFrame) -> DataFrame:
    """Bronze slices, keyed by channel, -> one conformed, deduplicated batch."""
    conformed = [normalise_columns(df, channel) for channel, df in bronze.items()]
    unioned = conformed[0]
    for df in conformed[1:]:
        unioned = unioned.unionByName(df)
    typed = parse_meta(cast_measures(unioned))
    resolved = resolve_skus(typed, latest_sku_master(sku_master))
    # Duplicates re-sent within a channel carry a later date, so it breaks ties
    # between rows that arrived in the same Bronze load.
    deduped = deduplicate_latest(resolved, "order_id", ("_ingested_at", "order_date"))
    return deduped.withColumn("_processed_at", F.current_timestamp()).select(*SILVER_COLUMNS)


# ---------------------------------------------------------------------------
# Incremental load
# ---------------------------------------------------------------------------

WATERMARK_TABLE = "_watermark"
# Watermarks travel as strings formatted by Spark in the session time zone and
# are parsed back the same way, so the driver's Python time zone never gets a
# chance to shift them.
TS_FORMAT = "yyyy-MM-dd HH:mm:ss.SSSSSS"


def ensure_watermark_table(spark) -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {fq(WATERMARK_TABLE)} (
            table_name STRING,
            last_processed_ts TIMESTAMP
        ) USING DELTA
        """
    )


def get_watermark(spark, table_name: str) -> str | None:
    """Last _ingested_at already processed for a Bronze table, or None."""
    return (
        spark.table(fq(WATERMARK_TABLE))
        .filter(F.col("table_name") == table_name)
        .agg(F.date_format(F.max("last_processed_ts"), TS_FORMAT))
        .first()[0]
    )


def read_since_watermark(spark, table_name: str) -> DataFrame:
    """Only the Bronze rows that arrived after the watermark."""
    df = spark.table(fq(table_name))
    watermark = get_watermark(spark, table_name)
    if watermark is None:
        return df
    return df.filter(F.col("_ingested_at") > F.lit(watermark).cast("timestamp"))


def slice_stats(df: DataFrame) -> tuple[int, str | None]:
    """Row count and max _ingested_at of a Bronze slice, in one pass."""
    row = df.agg(F.count(F.lit(1)), F.date_format(F.max("_ingested_at"), TS_FORMAT)).first()
    return row[0], row[1]


def set_watermark(spark, table_name: str, last_processed_ts: str) -> None:
    """Upsert the watermark. Call only after the Silver MERGE has committed."""
    spark.sql(
        f"""
        MERGE INTO {fq(WATERMARK_TABLE)} AS t
        USING (
            SELECT :table_name AS table_name,
                   CAST(:ts AS TIMESTAMP) AS last_processed_ts
        ) AS s
        ON t.table_name = s.table_name
        WHEN MATCHED THEN UPDATE SET t.last_processed_ts = s.last_processed_ts
        WHEN NOT MATCHED THEN INSERT *
        """,
        args={"table_name": table_name, "ts": last_processed_ts},
    )


# ---------------------------------------------------------------------------
# Data quality
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CheckResult:
    check: str
    rule: str
    passed: bool
    failing_rows: int
    total_rows: int
    threshold: float
    detail: str


class QualityCheckFailed(Exception):
    """Raised so the run stops before the Silver MERGE touches anything."""


def row_rules() -> list[tuple[str, str, Column, float]]:
    """(check, rule, condition a failing row meets, max share of rows allowed to fail).

    The generator adds about 1% unknown SKUs, 0.5% negative quantities and 1%
    null prices on purpose, so those rules tolerate a little. Keys and dates
    tolerate nothing. Built in a function because Columns need a live session.
    """
    return [
        ("completeness", "order_id_not_null", F.col("order_id").isNull(), 0.0),
        ("completeness", "order_date_not_null", F.col("order_date").isNull(), 0.0),
        ("completeness", "canonical_sku_not_null", F.col("canonical_sku").isNull(), 0.02),
        ("validity", "quantity_not_negative", F.col("quantity") < 0, 0.01),
        ("validity", "amount_positive", F.col("amount").isNull() | (F.col("amount") <= 0), 0.02),
        ("validity", "order_date_not_future", F.col("order_date") > F.current_date(), 0.0),
        (
            "consistency",
            "sku_raw_resolves",
            F.col("sku_raw").isNotNull() & ~F.col("sku_resolved"),
            0.02,
        ),
    ]


def evaluate_row_rules(df: DataFrame) -> list[CheckResult]:
    """Every row rule in a single aggregation pass over the batch."""
    rules = row_rules()
    aggs = [F.count(F.lit(1)).alias("_total")]
    aggs += [F.sum(F.when(cond, 1).otherwise(0)).alias(rule) for _, rule, cond, _ in rules]
    row = df.agg(*aggs).first()
    total = row["_total"]
    results = []
    for check, rule, _, threshold in rules:
        failing = row[rule] or 0
        rate = failing / total if total else 0.0
        results.append(
            CheckResult(
                check=check,
                rule=rule,
                passed=rate <= threshold,
                failing_rows=failing,
                total_rows=total,
                threshold=threshold,
                detail=f"{failing} of {total} rows ({rate:.2%}), limit {threshold:.0%}",
            )
        )
    return results


VOLUME_RULE = "silver_total_within_band"


def check_volume(
    projected_total: int, previous_total: int | None, tolerance: float = 0.5
) -> CheckResult:
    """Silver's row count after this MERGE must stay within +/- tolerance of the last run.

    Compares the projected Silver total rather than the batch size, so a small
    incremental batch on top of a big table passes, and a batch that would
    wipe out or balloon the table does not.
    """
    if not previous_total:
        passed, detail = True, f"{projected_total} rows, no previous run to compare against"
    else:
        change = (projected_total - previous_total) / previous_total
        passed = abs(change) <= tolerance
        detail = (
            f"{projected_total} rows vs {previous_total} last run ({change:+.1%}), "
            f"limit +/-{tolerance:.0%}"
        )
    return CheckResult(
        check="volume",
        rule=VOLUME_RULE,
        passed=passed,
        failing_rows=0 if passed else 1,
        total_rows=projected_total,
        threshold=tolerance,
        detail=detail,
    )


def format_failures(results: list[CheckResult]) -> str:
    failed = [r for r in results if not r.passed]
    lines = [f"{len(failed)} quality check(s) failed, stopping before the Silver MERGE:"]
    lines += [f"  - {r.check}.{r.rule}: {r.detail}" for r in failed]
    return "\n".join(lines)


def assert_passed(results: list[CheckResult]) -> None:
    if not all(r.passed for r in results):
        raise QualityCheckFailed(format_failures(results))


QUALITY_LOG_TABLE = "_quality_log"
SILVER_TABLE = "silver_orders"

QUALITY_LOG_SCHEMA = T.StructType(
    [
        T.StructField("run_id", T.StringType()),
        T.StructField("check_name", T.StringType()),
        T.StructField("rule", T.StringType()),
        T.StructField("passed", T.BooleanType()),
        T.StructField("failing_rows", T.LongType()),
        T.StructField("total_rows", T.LongType()),
        T.StructField("threshold", T.DoubleType()),
        T.StructField("detail", T.StringType()),
    ]
)


def ensure_quality_log_table(spark) -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {fq(QUALITY_LOG_TABLE)} (
            run_id STRING,
            check_name STRING,
            rule STRING,
            passed BOOLEAN,
            failing_rows BIGINT,
            total_rows BIGINT,
            threshold DOUBLE,
            detail STRING,
            checked_at TIMESTAMP
        ) USING DELTA
        """
    )


def log_quality_results(spark, results: list[CheckResult], run_id: str) -> None:
    rows = [
        (run_id, r.check, r.rule, r.passed, r.failing_rows, r.total_rows, float(r.threshold), r.detail)
        for r in results
    ]
    (
        spark.createDataFrame(rows, QUALITY_LOG_SCHEMA)
        .withColumn("checked_at", F.current_timestamp())
        .write.mode("append")
        .saveAsTable(fq(QUALITY_LOG_TABLE))
    )


def previous_silver_total(spark) -> int | None:
    """Silver total recorded by the most recent run whose checks all passed."""
    log = spark.table(fq(QUALITY_LOG_TABLE))
    passed_runs = (
        log.groupBy("run_id")
        .agg(F.min(F.col("passed").cast("int")).alias("ok"), F.max("checked_at").alias("at"))
        .filter(F.col("ok") == 1)
    )
    row = (
        log.filter(F.col("rule") == VOLUME_RULE)
        .join(passed_runs, "run_id")
        .orderBy(F.col("at").desc())
        .select("total_rows")
        .first()
    )
    return row[0] if row else None


def projected_silver_total(spark, batch: DataFrame) -> int:
    """How many rows Silver will hold once this batch is merged on order_id."""
    keys = batch.select("order_id")
    if spark.catalog.tableExists(fq(SILVER_TABLE)):
        keys = keys.unionByName(spark.table(fq(SILVER_TABLE)).select("order_id"))
    return keys.distinct().count()


def run_quality_checks(spark, batch: DataFrame, run_id: str | None = None) -> list[CheckResult]:
    """Evaluate every check, log all results, then raise if any failed.

    Results are logged before raising so a failed run still leaves a record.
    """
    ensure_quality_log_table(spark)
    run_id = run_id or str(uuid.uuid4())
    results = evaluate_row_rules(batch)
    results.append(check_volume(projected_silver_total(spark, batch), previous_silver_total(spark)))
    log_quality_results(spark, results, run_id)
    assert_passed(results)
    return results
