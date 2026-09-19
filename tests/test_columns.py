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
