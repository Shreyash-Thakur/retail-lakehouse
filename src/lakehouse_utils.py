"""Shared helpers for the retail lakehouse notebooks.

Plain PySpark only, so everything here can be unit tested locally without a
Databricks workspace. Notebooks import it by putting ../src on sys.path.
"""
from __future__ import annotations


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
