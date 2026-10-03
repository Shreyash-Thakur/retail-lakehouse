from datetime import date
from decimal import Decimal

import pytest

from lakehouse_utils import build_daily_sales


@pytest.fixture
def silver(spark):
    return spark.createDataFrame(
        [
            ("o1", "channel_a", "SKU-1001", "Classic Tee", "Apparel", True, 2, Decimal("998.00"), date(2026, 3, 14)),
            ("o2", "channel_a", "SKU-1001", "Classic Tee", "Apparel", True, 1, Decimal("499.00"), date(2026, 3, 14)),
            ("o3", "channel_b", "SKU-1002", "Urban Mug", "Home", True, 3, Decimal("900.00"), date(2026, 3, 14)),
            ("o4", "channel_a", "SKU-1001", "Classic Tee", "Apparel", True, -1, Decimal("499.00"), date(2026, 3, 14)),
            ("o5", "channel_a", "SKU-1001", "Classic Tee", "Apparel", True, 1, None, date(2026, 3, 14)),
            ("o6", "channel_a", None, None, None, False, 1, Decimal("250.00"), date(2026, 3, 20)),
        ],
        "order_id string, channel string, canonical_sku string, product_name string, category string, "
        "sku_resolved boolean, quantity int, amount decimal(12,2), order_date date",
    )


def test_daily_sales_aggregates_valid_rows_only(silver):
    rows = {(r["order_date"], r["channel"]): r for r in build_daily_sales(silver).collect()}
    a = rows[(date(2026, 3, 14), "channel_a")]
    assert (a["order_count"], a["total_units"], a["total_revenue"]) == (2, 3, Decimal("1497.00"))
    assert rows[(date(2026, 3, 14), "channel_b")]["total_units"] == 3


def test_unresolved_skus_still_count_towards_daily_sales(silver):
    rows = {(r["order_date"], r["channel"]): r for r in build_daily_sales(silver).collect()}
    assert rows[(date(2026, 3, 20), "channel_a")]["total_revenue"] == Decimal("250.00")
