from lakehouse_utils import parse_meta


def test_promo_code_and_source_are_extracted(spark):
    df = spark.createDataFrame(
        [('{"promo_code": "SAVE10", "source": "app"}',), ('{"promo_code": null, "source": "web"}',), (None,)],
        "meta string",
    )
    rows = parse_meta(df).collect()
    assert [(r["promo_code"], r["order_source"]) for r in rows] == [
        ("SAVE10", "app"),
        (None, "web"),
        (None, None),
    ]
