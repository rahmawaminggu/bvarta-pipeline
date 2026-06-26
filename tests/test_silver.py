"""Unit tests for the Silver layer."""

import datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from pipeline.silver import _add_derived_fields, read_users


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

BRONZE_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("user_id", StringType()),
    StructField("event_type", StringType()),
    StructField("event_ts", TimestampType()),
    StructField("value", DoubleType()),
    StructField("event_date", DateType()),
])

USER_ROWS = [
    {"user_id": "u1", "country": "ID", "signup_date": "2024-12-01"},
    {"user_id": "u2", "country": "US", "signup_date": "2024-12-15"},
    {"user_id": "u3", "country": "SG", "signup_date": "invalid_date"},
    {"user_id": "u4", "country": "",   "signup_date": "2024-11-01"},
]


def make_bronze_df(spark, rows):
    return spark.createDataFrame(rows, schema=BRONZE_SCHEMA)


# ---------------------------------------------------------------------------
# read_users
# ---------------------------------------------------------------------------

class TestReadUsers:
    def test_invalid_signup_date_becomes_null(self, spark, tmp_path):
        csv_path = tmp_path / "users.csv"
        csv_path.write_text("user_id,country,signup_date\nu3,SG,invalid_date\n")
        df = read_users(spark, str(csv_path))
        row = df.filter(F.col("user_id") == "u3").collect()[0]
        assert row["signup_date"] is None

    def test_empty_country_becomes_null(self, spark, tmp_path):
        csv_path = tmp_path / "users.csv"
        csv_path.write_text("user_id,country,signup_date\nu4,,2024-11-01\n")
        df = read_users(spark, str(csv_path))
        row = df.filter(F.col("user_id") == "u4").collect()[0]
        assert row["country"] is None

    def test_valid_user_parsed_correctly(self, spark, tmp_path):
        csv_path = tmp_path / "users.csv"
        csv_path.write_text("user_id,country,signup_date\nu1,ID,2024-12-01\n")
        df = read_users(spark, str(csv_path))
        row = df.collect()[0]
        assert row["user_id"] == "u1"
        assert row["country"] == "ID"
        assert row["signup_date"] == datetime.date(2024, 12, 1)


# ---------------------------------------------------------------------------
# _add_derived_fields
# ---------------------------------------------------------------------------

class TestAddDerivedFields:
    def _make_enriched(self, spark, event_type, event_date, signup_date):
        rows = [{
            "event_id": "e1",
            "user_id": "u1",
            "event_type": event_type,
            "event_ts": datetime.datetime(2025, 1, 1, 10, 0, 0),
            "value": 10.0,
            "event_date": event_date,
        }]
        df = make_bronze_df(spark, rows)
        # Manually add signup_date column (simulates post-join)
        df = df.withColumn("signup_date", F.lit(signup_date).cast(DateType()))
        return df

    def test_is_purchase_true_for_purchase(self, spark):
        df = self._make_enriched(
            spark, "PURCHASE", datetime.date(2025, 1, 1), datetime.date(2024, 12, 1)
        )
        result = _add_derived_fields(df)
        row = result.collect()[0]
        assert row["is_purchase"] is True

    def test_is_purchase_false_for_click(self, spark):
        df = self._make_enriched(
            spark, "CLICK", datetime.date(2025, 1, 1), datetime.date(2024, 12, 1)
        )
        result = _add_derived_fields(df)
        row = result.collect()[0]
        assert row["is_purchase"] is False

    def test_days_since_signup_calculated_correctly(self, spark):
        df = self._make_enriched(
            spark, "CLICK", datetime.date(2025, 1, 1), datetime.date(2024, 12, 1)
        )
        result = _add_derived_fields(df)
        row = result.collect()[0]
        # 2025-01-01 - 2024-12-01 = 31 days
        assert row["days_since_signup"] == 31

    def test_days_since_signup_null_when_no_signup_date(self, spark):
        rows = [{
            "event_id": "e1",
            "user_id": "u3",
            "event_type": "CLICK",
            "event_ts": datetime.datetime(2025, 1, 1, 10, 0, 0),
            "value": 1.0,
            "event_date": datetime.date(2025, 1, 1),
        }]
        df = make_bronze_df(spark, rows)
        df = df.withColumn("signup_date", F.lit(None).cast(DateType()))
        result = _add_derived_fields(df)
        row = result.collect()[0]
        assert row["days_since_signup"] is None

    def test_event_without_matching_user_retained(self, spark, tmp_path):
        """Events for unknown users must survive the LEFT JOIN."""
        csv_path = tmp_path / "users.csv"
        csv_path.write_text("user_id,country,signup_date\nu1,ID,2024-12-01\n")

        rows = [
            {"event_id": "e1", "user_id": "u1", "event_type": "CLICK",
             "event_ts": datetime.datetime(2025, 1, 1), "value": 1.0,
             "event_date": datetime.date(2025, 1, 1)},
            {"event_id": "e_unknown", "user_id": "u_ghost", "event_type": "VIEW",
             "event_ts": datetime.datetime(2025, 1, 1), "value": 2.0,
             "event_date": datetime.date(2025, 1, 1)},
        ]
        bronze_df = make_bronze_df(spark, rows)

        from pipeline.silver import read_users
        users_df = read_users(spark, str(csv_path))
        enriched = bronze_df.join(users_df, on="user_id", how="left")
        enriched = _add_derived_fields(enriched)

        assert enriched.count() == 2
        ghost = enriched.filter(F.col("user_id") == "u_ghost").collect()[0]
        assert ghost["country"] is None