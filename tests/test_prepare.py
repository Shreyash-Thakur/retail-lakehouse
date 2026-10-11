from datetime import date

import pytest

from lakehouse_utils import SILVER_COLUMNS, prepare_silver_batch


@pytest.fixture
def bronze(spark):
    a = spark.sql(
        """
        SELECT * FROM VALUES
          ('A0000001', 'A-1001', '2', '998.00', '2026-03-14', TIMESTAMP'2026-09-20 10:00:00', 'a.csv'),
          ('A0000001', 'A-1001', '2', '998.00', '2026-03-15', TIMESTAMP'2026-09-20 10:00:00', 'a.csv'),
          ('A0000002', 'A-9001', '1', '499.00', '2026-03-14', TIMESTAMP'2026-09-20 10:00:00', 'a.csv')
        AS t(order_id, sku, qty, price, order_date, _ingested_at, _source_file)
        """
    )
    c = spark.sql(
        """
        SELECT * FROM VALUES
          ('c_1', 'itm_1001', '1', '499.00', '03-14-2026', '{"promo_code": "SAVE10", "source": "app"}',
           TIMESTAMP'2026-09-20 10:00:00', 'c.csv')
        AS t(id, item_sku, units, total, created_at, meta, _ingested_at, _source_file)
        """
    )
    return {"channel_a": a, "channel_c": c}


@pytest.fixture
def sku_master(spark):
    return spark.sql(
        """
        SELECT * FROM VALUES
          ('A-1001', 'SKU-1001', 'Classic Tee', 'Apparel', TIMESTAMP'2026-09-20 10:00:00'),
          ('itm_1001', 'SKU-1001', 'Classic Tee', 'Apparel', TIMESTAMP'2026-09-20 10:00:00')
        AS t(channel_sku, canonical_sku, product_name, category, _ingested_at)
        """
    )


def test_batch_has_the_silver_columns(bronze, sku_master):
    assert tuple(prepare_silver_batch(bronze, sku_master).columns) == SILVER_COLUMNS


def test_duplicates_collapse_to_the_later_order_date(bronze, sku_master):
    rows = prepare_silver_batch(bronze, sku_master).filter("order_id = 'A0000001'").collect()
    assert len(rows) == 1
    assert rows[0]["order_date"] == date(2026, 3, 15)


def test_unknown_sku_is_flagged_not_dropped(bronze, sku_master):
    row = prepare_silver_batch(bronze, sku_master).filter("order_id = 'A0000002'").first()
    assert row["sku_resolved"] is False


def test_channel_c_promo_code_comes_through(bronze, sku_master):
    row = prepare_silver_batch(bronze, sku_master).filter("order_id = 'c_1'").first()
    assert (row["promo_code"], row["order_source"]) == ("SAVE10", "app")
