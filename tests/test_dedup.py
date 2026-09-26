from lakehouse_utils import deduplicate_latest


def test_keeps_the_latest_ingested_row(spark):
    df = spark.sql(
        """
        SELECT * FROM VALUES
          ('A1', 100, TIMESTAMP'2026-09-20 10:00:00', DATE'2026-03-14'),
          ('A1', 200, TIMESTAMP'2026-09-21 10:00:00', DATE'2026-03-14'),
          ('A2', 300, TIMESTAMP'2026-09-20 10:00:00', DATE'2026-03-14')
        AS t(order_id, amount, _ingested_at, order_date)
        """
    )
    out = {r["order_id"]: r["amount"] for r in deduplicate_latest(df).collect()}
    assert out == {"A1": 200, "A2": 300}


def test_same_load_duplicates_fall_back_to_the_later_order_date(spark):
    df = spark.sql(
        """
        SELECT * FROM VALUES
          ('A1', 100, TIMESTAMP'2026-09-20 10:00:00', DATE'2026-03-15'),
          ('A1', 200, TIMESTAMP'2026-09-20 10:00:00', DATE'2026-03-14')
        AS t(order_id, amount, _ingested_at, order_date)
        """
    )
    rows = deduplicate_latest(df, order_by=("_ingested_at", "order_date")).collect()
    assert [r["amount"] for r in rows] == [100]


def test_full_tie_still_returns_exactly_one_row(spark):
    df = spark.sql(
        """
        SELECT * FROM VALUES
          ('A1', 100, TIMESTAMP'2026-09-20 10:00:00', DATE'2026-03-14'),
          ('A1', 100, TIMESTAMP'2026-09-20 10:00:00', DATE'2026-03-14')
        AS t(order_id, amount, _ingested_at, order_date)
        """
    )
    assert deduplicate_latest(df).count() == 1
