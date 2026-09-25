from datetime import date

import pytest
from pyspark.sql import functions as F

from lakehouse_utils import parse_order_date


@pytest.mark.parametrize(
    "raw",
    ["2026-03-14", "14/03/2026", "03-14-2026", "14-Mar-2026"],
    ids=["channel_a", "channel_b", "channel_c", "channel_d"],
)
def test_every_channel_format_parses_to_the_same_date(spark, raw):
    df = spark.createDataFrame([(raw,)], "d string")
    assert df.select(parse_order_date(F.col("d"))).first()[0] == date(2026, 3, 14)


def test_unparseable_date_is_null_not_an_error(spark):
    df = spark.createDataFrame([("not a date",), (None,)], "d string")
    assert [r[0] for r in df.select(parse_order_date(F.col("d"))).collect()] == [None, None]
