import csv
import json

from lakehouse_utils import add_ingest_metadata, read_raw_csv


def write_channel_c(path):
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "item_sku", "units", "total", "created_at", "meta"])
        writer.writerow(["c_1", "itm_1001", "2", "998.00", "03-14-2026", json.dumps({"promo_code": "SAVE10", "source": "app"})])


def test_json_meta_stays_in_one_column(spark, tmp_path):
    path = tmp_path / "channel_c.csv"
    write_channel_c(path)
    df = read_raw_csv(spark, str(path))
    assert df.columns == ["id", "item_sku", "units", "total", "created_at", "meta"]
    assert json.loads(df.first()["meta"]) == {"promo_code": "SAVE10", "source": "app"}


def test_everything_is_read_as_string(spark, tmp_path):
    path = tmp_path / "channel_c.csv"
    write_channel_c(path)
    assert {t for _, t in read_raw_csv(spark, str(path)).dtypes} == {"string"}


def test_ingest_metadata_records_the_source_file(spark, tmp_path):
    path = tmp_path / "channel_c.csv"
    write_channel_c(path)
    row = add_ingest_metadata(read_raw_csv(spark, str(path))).first()
    assert row["_source_file"].endswith("channel_c.csv")
    assert row["_ingested_at"] is not None
    assert row["_ingest_date"] is not None
