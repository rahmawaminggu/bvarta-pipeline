"""Unit tests for the output validator."""

import datetime

import pytest
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

from pipeline.output_validator import (
    OutputValidationError,
    validate_bronze,
    validate_silver,
    validate_gold,
)


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

BRONZE_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("user_id", StringType()),
    StructField("event_type", StringType()),
    StructField("event_ts", TimestampType()),
    StructField("value", DoubleType()),
    StructField("event_date", DateType()),
])

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

GOLD_SCHEMA = StructType([
    StructField("event_date", DateType()),
    StructField("country", StringType()),
    StructField("total_events", LongType()),
    StructField("total_value", DoubleType()),
    StructField("total_purchases", LongType()),
    StructField("unique_users", LongType()),
])


# ---------------------------------------------------------------------------
# Bronze validation
# ---------------------------------------------------------------------------

class TestValidateBronze:
    def test_valid_bronze_passes(self, spark):
        df = spark.createDataFrame([
            ("e1", "u1", "CLICK", datetime.datetime(2025, 1, 1), 1.0, datetime.date(2025, 1, 1)),
        ], schema=BRONZE_SCHEMA)
        validate_bronze(df)  # should not raise

    def test_empty_bronze_raises(self, spark):
        df = spark.createDataFrame([], schema=BRONZE_SCHEMA)
        with pytest.raises(OutputValidationError, match="empty"):
            validate_bronze(df)

    def test_null_event_id_raises(self, spark):
        df = spark.createDataFrame([
            (None, "u1", "CLICK", datetime.datetime(2025, 1, 1), 1.0, datetime.date(2025, 1, 1)),
        ], schema=BRONZE_SCHEMA)
        with pytest.raises(OutputValidationError, match="event_id"):
            validate_bronze(df)


# ---------------------------------------------------------------------------
# Silver validation
# ---------------------------------------------------------------------------

class TestValidateSilver:
    def test_valid_silver_passes(self, spark):
        df = spark.createDataFrame([
            ("e1", "u1", "CLICK", datetime.datetime(2025, 1, 1), 1.0,
             datetime.date(2025, 1, 1), "ID", datetime.date(2024, 12, 1), False, 31),
        ], schema=SILVER_SCHEMA)
        validate_silver(df)

    def test_null_event_date_raises(self, spark):
        df = spark.createDataFrame([
            ("e1", "u1", "CLICK", datetime.datetime(2025, 1, 1), 1.0,
             None, "ID", datetime.date(2024, 12, 1), False, 31),
        ], schema=SILVER_SCHEMA)
        with pytest.raises(OutputValidationError, match="event_date"):
            validate_silver(df)


# ---------------------------------------------------------------------------
# Gold validation
# ---------------------------------------------------------------------------

class TestValidateGold:
    def test_valid_gold_passes(self, spark):
        df = spark.createDataFrame([
            (datetime.date(2025, 1, 1), "ID", 5, 50.0, 2, 3),
        ], schema=GOLD_SCHEMA)
        validate_gold(df)

    def test_empty_gold_raises(self, spark):
        df = spark.createDataFrame([], schema=GOLD_SCHEMA)
        with pytest.raises(OutputValidationError, match="empty"):
            validate_gold(df)

    def test_null_event_date_raises(self, spark):
        df = spark.createDataFrame([
            (None, "ID", 5, 50.0, 2, 3),
        ], schema=GOLD_SCHEMA)
        with pytest.raises(OutputValidationError, match="event_date"):
            validate_gold(df)

    def test_negative_total_events_raises(self, spark):
        df = spark.createDataFrame([
            (datetime.date(2025, 1, 1), "ID", -1, 50.0, 2, 3),
        ], schema=GOLD_SCHEMA)
        with pytest.raises(OutputValidationError, match="total_events"):
            validate_gold(df)