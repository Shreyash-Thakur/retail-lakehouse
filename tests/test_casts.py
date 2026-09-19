from datetime import date
from decimal import Decimal

from lakehouse_utils import cast_measures


def test_measures_are_typed(spark):
    df = spark.createDataFrame([("3", "1497.50", "14/03/2026")], "quantity string, amount string, order_date string")
    out = cast_measures(df)
    assert dict(out.dtypes) == {"quantity": "int", "amount": "decimal(12,2)", "order_date": "date"}
    row = out.first()
    assert (row["quantity"], row["amount"], row["order_date"]) == (3, Decimal("1497.50"), date(2026, 3, 14))


def test_empty_price_becomes_null_and_negative_quantity_survives(spark):
    df = spark.createDataFrame([("-2", None, "2026-03-14")], "quantity string, amount string, order_date string")
    row = cast_measures(df).first()
    assert row["quantity"] == -2
    assert row["amount"] is None
