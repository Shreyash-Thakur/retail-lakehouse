from lakehouse_utils import latest_sku_master, resolve_skus

MASTER_SCHEMA = "channel_sku string, canonical_sku string, product_name string, category string"


def master(spark):
    return spark.createDataFrame(
        [
            ("A-1001", "SKU-1001", "Classic Tee", "Apparel"),
            ("PRD1001", "SKU-1001", "Classic Tee", "Apparel"),
            ("itm_1001", "SKU-1001", "Classic Tee", "Apparel"),
            ("SKU1001", "SKU-1001", "Classic Tee", "Apparel"),
        ],
        MASTER_SCHEMA,
    )


def test_every_channel_code_resolves_to_one_canonical_sku(spark):
    orders = spark.createDataFrame(
        [("o1", "A-1001"), ("o2", "PRD1001"), ("o3", "itm_1001"), ("o4", "SKU1001")],
        "order_id string, sku_raw string",
    )
    out = resolve_skus(orders, master(spark)).collect()
    assert {r["canonical_sku"] for r in out} == {"SKU-1001"}
    assert all(r["sku_resolved"] for r in out)


def test_unknown_sku_is_kept_and_flagged(spark):
    orders = spark.createDataFrame([("o1", "A-9001")], "order_id string, sku_raw string")
    out = resolve_skus(orders, master(spark)).collect()
    assert len(out) == 1
    assert out[0]["canonical_sku"] is None
    assert out[0]["sku_resolved"] is False
