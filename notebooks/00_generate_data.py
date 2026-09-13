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

DUPLICATE_RATE = 0.02
NULL_PRICE_RATE = 0.01
NEGATIVE_QTY_RATE = 0.005
UNKNOWN_SKU_RATE = 0.01

# COMMAND ----------

# MAGIC %md
# MAGIC ## Products
# MAGIC
# MAGIC 40 canonical SKUs, `SKU-1001` to `SKU-1040`, each with a category and a unit price.

# COMMAND ----------

CATEGORY_ITEMS = {
    "Apparel": ["Tee", "Hoodie", "Chinos", "Jacket"],
    "Footwear": ["Sneaker", "Loafer", "Sandal"],
    "Home": ["Mug", "Throw", "Lamp"],
    "Electronics": ["Earbuds", "Charger", "Speaker"],
}
PRICE_RANGE = {"Apparel": (499, 2999), "Footwear": (999, 4999), "Home": (299, 1999), "Electronics": (799, 5999)}
ADJECTIVES = ["Classic", "Urban", "Everyday", "Premium", "Lite"]

products = []
for i in range(40):
    category = random.choice(list(CATEGORY_ITEMS))
    low, high = PRICE_RANGE[category]
    products.append(
        {
            "canonical_sku": f"SKU-{1001 + i}",
            "product_name": f"{random.choice(ADJECTIVES)} {random.choice(CATEGORY_ITEMS[category])}",
            "category": category,
            "unit_price": round(random.uniform(low, high), 2),
        }
    )

products[:5]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Per-channel formats
# MAGIC
# MAGIC Each channel gets its own headers, order ID style, date format and SKU code. The SKU codes all
# MAGIC carry the same number, so `A-1001`, `PRD1001`, `itm_1001` and `SKU1001` are all `SKU-1001`.

# COMMAND ----------

HEADERS = {
    "channel_a": ["order_id", "sku", "qty", "price", "order_date"],
    "channel_b": ["orderId", "productCode", "quantity", "amount", "date"],
    "channel_c": ["id", "item_sku", "units", "total", "created_at", "meta"],
    "channel_d": ["ORDER_NO", "SKU_CODE", "QTY", "AMT", "DT"],
}
ORDER_ID_FORMATS = {
    "channel_a": "A{n:07d}",
    "channel_b": "ORD-B-{n:06d}",
    "channel_c": "c_{n}",
    "channel_d": "D/{n:06d}",
}
DATE_FORMATS = {
    "channel_a": "%Y-%m-%d",
    "channel_b": "%d/%m/%Y",
    "channel_c": "%m-%d-%Y",
    "channel_d": "%d-%b-%Y",
}
SKU_FORMATS = {
    "channel_a": "A-{n}",
    "channel_b": "PRD{n}",
    "channel_c": "itm_{n}",
    "channel_d": "SKU{n}",
}

def channel_sku(channel: str, canonical_sku: str) -> str:
    return SKU_FORMATS[channel].format(n=canonical_sku.split("-")[1])

# COMMAND ----------

# MAGIC %md
# MAGIC ## SKU master
# MAGIC
# MAGIC One row per channel code: 40 products x 4 channels = 160 rows.

# COMMAND ----------

sku_master_rows = [
    [channel_sku(channel, p["canonical_sku"]), p["canonical_sku"], p["product_name"], p["category"]]
    for channel in HEADERS
    for p in products
]
len(sku_master_rows), sku_master_rows[:4]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Orders
# MAGIC
# MAGIC One order is one SKU line. The dirt is applied per row at fixed rates:
# MAGIC
# MAGIC - **unknown SKU** (1%): a code in the 9000s, which the SKU master does not have
# MAGIC - **negative quantity** (0.5%): the sign flips, the amount stays positive, like a bad keying
# MAGIC - **null price** (1%): the amount column is left empty
# MAGIC
# MAGIC channel_c also gets a `meta` column holding JSON with `promo_code` and `source`.

# COMMAND ----------

PROMO_CODES = ["SAVE10", "FESTIVE20", "WELCOME5", None, None, None]
SOURCES = ["web", "app", "marketplace"]

def make_orders(channel: str, n_rows: int, start: date, days: int, first_id: int) -> list[list[str]]:
    rows = []
    for i in range(n_rows):
        product = random.choice(products)
        qty = random.randint(1, 5)
        amount = round(qty * product["unit_price"], 2)
        order_date = start + timedelta(days=random.randrange(days))

        sku = channel_sku(channel, product["canonical_sku"])
        if random.random() < UNKNOWN_SKU_RATE:
            sku = channel_sku(channel, f"SKU-{random.randint(9001, 9020)}")
        if random.random() < NEGATIVE_QTY_RATE:
            qty = -qty
        price = "" if random.random() < NULL_PRICE_RATE else f"{amount:.2f}"

        row = [
            ORDER_ID_FORMATS[channel].format(n=first_id + i),
            sku,
            str(qty),
            price,
            order_date.strftime(DATE_FORMATS[channel]),
        ]
        if channel == "channel_c":
            row.append(json.dumps({"promo_code": random.choice(PROMO_CODES), "source": random.choice(SOURCES)}))
        rows.append(row)
    return rows

# COMMAND ----------

# MAGIC %md
# MAGIC ## Duplicates
# MAGIC
# MAGIC About 2% of orders are sent twice within a channel. The re-sent copy keeps the order ID but
# MAGIC its date is nudged by up to a day, the way a source system re-exports an edited order.
# MAGIC Silver has to keep exactly one of them.

# COMMAND ----------

DATE_COLUMN = 4

def add_duplicates(rows: list[list[str]], channel: str) -> list[list[str]]:
    fmt = DATE_FORMATS[channel]
    dupes = []
    for row in random.sample(rows, int(len(rows) * DUPLICATE_RATE)):
        dupe = list(row)
        original = datetime.strptime(row[DATE_COLUMN], fmt).date()
        dupe[DATE_COLUMN] = (original + timedelta(days=random.choice([0, 1]))).strftime(fmt)
        dupes.append(dupe)
    combined = rows + dupes
    random.shuffle(combined)
    return combined
