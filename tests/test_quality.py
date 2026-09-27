from datetime import date
from decimal import Decimal

from lakehouse_utils import evaluate_row_rules

SCHEMA = (
    "order_id string, sku_raw string, canonical_sku string, sku_resolved boolean, "
    "quantity int, amount decimal(12,2), order_date date"
)


def batch(spark, rows):
    return spark.createDataFrame(rows, SCHEMA)


def good_rows(n):
    return [
        (f"A{i:07d}", "A-1001", "SKU-1001", True, 1, Decimal("499.00"), date(2026, 3, 14))
        for i in range(n)
    ]


def with_null_order_id(row):
    return (None,) + row[1:]


def with_quantity(row, quantity):
    return row[:4] + (quantity,) + row[5:]


def by_rule(results):
    return {r.rule: r for r in results}


def test_clean_batch_passes_every_rule(spark):
    results = evaluate_row_rules(batch(spark, good_rows(100)))
    assert all(r.passed for r in results)
    assert {r.check for r in results} == {"completeness", "validity", "consistency"}


def test_a_single_null_order_id_fails_completeness(spark):
    rows = good_rows(100)
    rows[0] = with_null_order_id(rows[0])
    result = by_rule(evaluate_row_rules(batch(spark, rows)))["order_id_not_null"]
    assert not result.passed
    assert result.failing_rows == 1
