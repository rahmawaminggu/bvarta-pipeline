"""Unit tests for the Gold layer."""

import datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from pipeline.gold import run as gold_run


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SILVER_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("user_id", StringType()),
    StructField("event_type", StringType()),
    StructField("event_ts", TimestampType()),
    StructField("value", DoubleType()),
    StructField("event_date", DateType()),
    StructField("country", StringType()),
    StructField("signup_date", DateType()),
    StructField("is_purchase", BooleanType()),
    StructField("days_since_signup", LongType()),
])


def make_silver_df(spark, rows):
    return spark.createDataFrame(rows, schema=SILVER_SCHEMA)


BASE_ROWS = [
    {
        "event_id": "e1", "user_id": "u1", "event_type": "CLICK",
        "event_ts": datetime.datetime(2025, 1, 1, 10, 0),
        "value": 3.0, "event_date": datetime.date(2025, 1, 1),
        "country": "ID", "signup_date": datetime.date(2024, 12, 1),
        "is_purchase": False, "days_since_signup": 31,
    },
    {
        "event_id": "e3", "user_id": "u1", "event_type": "PURCHASE",
        "event_ts": datetime.datetime(2025, 1, 1, 10, 10),
        "value": 25.0, "event_date": datetime.date(2025, 1, 1),
        "country": "ID", "signup_date": datetime.date(2024, 12, 1),
        "is_purchase": True, "days_since_signup": 31,
    },
    {
        "event_id": "e2", "user_id": "u2", "event_type": "VIEW",
        "event_ts": datetime.datetime(2025, 1, 1, 10, 5),
        "value": None, "event_date": datetime.date(2025, 1, 1),
        "country": "US", "signup_date": datetime.date(2024, 12, 15),
        "is_purchase": False, "days_since_signup": 17,
    },
]


# ---------------------------------------------------------------------------
# Aggregation correctness
# ---------------------------------------------------------------------------

class TestGoldAggregation:
    def test_total_events_per_country(self, spark, tmp_path):
        df = make_silver_df(spark, BASE_ROWS)
        result = gold_run(spark, df, str(tmp_path))

        id_row = result.filter(F.col("country") == "ID").collect()[0]
        us_row = result.filter(F.col("country") == "US").collect()[0]

        assert id_row["total_events"] == 2
        assert us_row["total_events"] == 1

    def test_total_purchases_counted_correctly(self, spark, tmp_path):
        df = make_silver_df(spark, BASE_ROWS)
        result = gold_run(spark, df, str(tmp_path))

        id_row = result.filter(F.col("country") == "ID").collect()[0]
        assert id_row["total_purchases"] == 1

    def test_total_value_sums_correctly(self, spark, tmp_path):
        df = make_silver_df(spark, BASE_ROWS)
        result = gold_run(spark, df, str(tmp_path))

        id_row = result.filter(F.col("country") == "ID").collect()[0]
        assert id_row["total_value"] == pytest.approx(28.0)

    def test_null_value_excluded_from_sum(self, spark, tmp_path):
        df = make_silver_df(spark, BASE_ROWS)
        result = gold_run(spark, df, str(tmp_path))

        us_row = result.filter(F.col("country") == "US").collect()[0]
        # value is null for e2
        assert us_row["total_value"] is None

    def test_unique_users_counted_correctly(self, spark, tmp_path):
        df = make_silver_df(spark, BASE_ROWS)
        result = gold_run(spark, df, str(tmp_path))

        id_row = result.filter(F.col("country") == "ID").collect()[0]
        assert id_row["unique_users"] == 1

    def test_output_has_required_columns(self, spark, tmp_path):
        df = make_silver_df(spark, BASE_ROWS)
        result = gold_run(spark, df, str(tmp_path))

        expected_cols = {"event_date", "country", "total_events",
                         "total_value", "total_purchases", "unique_users"}
        assert expected_cols.issubset(set(result.columns))

    def test_idempotent_rerun_does_not_duplicate(self, spark, tmp_path):
        """Running gold twice on same data should produce identical row count."""
        df = make_silver_df(spark, BASE_ROWS)
        gold_run(spark, df, str(tmp_path))
        gold_run(spark, df, str(tmp_path))

        result = spark.read.parquet(f"{tmp_path}/gold/daily_metrics")
        assert result.count() == 2  # ID + US, not 4

    def test_late_event_updates_aggregate(self, spark, tmp_path):
        """
        A late-arriving event for 2024-12-31 should correctly update
        the gold partition for that date when the pipeline re-runs.
        """
        early_rows = [
            {
                "event_id": "e10", "user_id": "u1", "event_type": "CLICK",
                "event_ts": datetime.datetime(2024, 12, 31, 9, 0),
                "value": 1.0, "event_date": datetime.date(2024, 12, 31),
                "country": "ID", "signup_date": datetime.date(2024, 12, 1),
                "is_purchase": False, "days_since_signup": 30,
            }
        ]
        late_rows = [
            {
                "event_id": "e11", "user_id": "u2", "event_type": "PURCHASE",
                "event_ts": datetime.datetime(2024, 12, 31, 23, 50),
                "value": 50.0, "event_date": datetime.date(2024, 12, 31),
                "country": "US", "signup_date": datetime.date(2024, 12, 15),
                "is_purchase": True, "days_since_signup": 16,
            }
        ]

        df_first = make_silver_df(spark, early_rows)
        gold_run(spark, df_first, str(tmp_path))

        df_second = make_silver_df(spark, early_rows + late_rows)
        gold_run(spark, df_second, str(tmp_path))

        result = spark.read.parquet(f"{tmp_path}/gold/daily_metrics")
        dec_31 = result.filter(F.col("event_date") == datetime.date(2024, 12, 31))
        assert dec_31.count() == 2  # ID + US rows both present