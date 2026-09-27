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


def test_negative_quantities_within_tolerance_pass(spark):
    rows = good_rows(200)
    rows[0] = with_quantity(rows[0], -1)  # 0.5%, the generator's rate
    assert by_rule(evaluate_row_rules(batch(spark, rows)))["quantity_not_negative"].passed


def test_negative_quantities_over_tolerance_fail(spark):
    rows = [with_quantity(r, -1) if i < 5 else r for i, r in enumerate(good_rows(100))]  # 5%
    result = by_rule(evaluate_row_rules(batch(spark, rows)))["quantity_not_negative"]
    assert not result.passed
    assert result.failing_rows == 5


def test_future_dates_fail_validity(spark):
    rows = good_rows(10)
    rows[0] = rows[0][:6] + (date(2999, 1, 1),)
    assert not by_rule(evaluate_row_rules(batch(spark, rows)))["order_date_not_future"].passed


def test_unresolved_skus_over_two_percent_fail_consistency(spark):
    rows = good_rows(100)
    for i in range(3):
        rows[i] = rows[i][:1] + ("A-9001", None, False) + rows[i][4:]
    result = by_rule(evaluate_row_rules(batch(spark, rows)))["sku_raw_resolves"]
    assert not result.passed
    assert "3 of 100 rows" in result.detail
