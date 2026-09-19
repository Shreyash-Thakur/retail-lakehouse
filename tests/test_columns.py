from lakehouse_utils import CONFORMED_COLUMNS, normalise_columns


def test_channel_b_columns_are_renamed(spark):
    df = spark.createDataFrame(
        [("ORD-B-000001", "PRD1001", "2", "998.00", "14/03/2026")],
        "orderId string, productCode string, quantity string, amount string, date string",
    )
    row = normalise_columns(df, "channel_b").first()
    assert row["order_id"] == "ORD-B-000001"
    assert row["sku_raw"] == "PRD1001"
    assert row["quantity"] == "2"
    assert row["amount"] == "998.00"
    assert row["order_date"] == "14/03/2026"
    assert row["channel"] == "channel_b"


def test_channels_without_meta_get_null_meta(spark):
    df = spark.createDataFrame(
        [("D/000001", "SKU1001", "1", "499.00", "14-Mar-2026")],
        "ORDER_NO string, SKU_CODE string, QTY string, AMT string, DT string",
    )
    out = normalise_columns(df, "channel_d")
    assert tuple(out.columns) == CONFORMED_COLUMNS
    assert out.first()["meta"] is None
