"""Unit tests for the Bronze layer."""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from pipeline.bronze import (
    _deduplicate,
    _normalize,
    _tag_rejection_reason,
    _cast_timestamp,
    RAW_SCHEMA,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_raw_df(spark: SparkSession, rows: list[dict]):
    return spark.createDataFrame(rows, schema=RAW_SCHEMA)


# ---------------------------------------------------------------------------
# _tag_rejection_reason
# ---------------------------------------------------------------------------

class TestTagRejectionReason:
    def test_valid_record_has_no_rejection(self, spark):
        df = make_raw_df(spark, [
            {"event_id": "e1", "user_id": "u1", "event_type": "CLICK",
             "event_ts": "2025-01-01T10:00:00Z", "value": 3.0},
        ])
        result = _tag_rejection_reason(df)
        row = result.collect()[0]
        assert row["rejection_reason"] is None

    def test_null_user_id_is_rejected(self, spark):
        df = make_raw_df(spark, [
            {"event_id": "e4", "user_id": None, "event_type": "CLICK",
             "event_ts": "2025-01-01T10:00:00Z", "value": 1.0},
        ])
        result = _tag_rejection_reason(df)
        row = result.collect()[0]
        assert row["rejection_reason"] is not None
        assert "user_id" in row["rejection_reason"]

    def test_empty_string_user_id_is_rejected(self, spark):
        df = make_raw_df(spark, [
            {"event_id": "e14", "user_id": "", "event_type": "CLICK",
             "event_ts": "2025-01-02T10:00:00Z", "value": 1.0},
        ])
        result = _tag_rejection_reason(df)
        row = result.collect()[0]
        assert row["rejection_reason"] is not None

    def test_null_event_type_is_rejected(self, spark):
        df = make_raw_df(spark, [
            {"event_id": "e15", "user_id": "u1", "event_type": None,
             "event_ts": "2025-01-02T10:05:00Z", "value": 3.0},
        ])
        result = _tag_rejection_reason(df)
        row = result.collect()[0]
        assert row["rejection_reason"] is not None
        assert "event_type" in row["rejection_reason"]

    def test_null_event_ts_is_rejected(self, spark):
        df = make_raw_df(spark, [
            {"event_id": "e7", "user_id": "u2", "event_type": "VIEW",
             "event_ts": None, "value": 4.0},
        ])
        result = _tag_rejection_reason(df)
        row = result.collect()[0]
        assert row["rejection_reason"] is not None
        assert "event_ts" in row["rejection_reason"]

    def test_invalid_timestamp_string_is_rejected(self, spark):
        df = make_raw_df(spark, [
            {"event_id": "e4b", "user_id": "u1", "event_type": "CLICK",
             "event_ts": "invalid_ts", "value": 1.0},
        ])
        result = _tag_rejection_reason(df)
        row = result.collect()[0]
        assert row["rejection_reason"] is not None
        assert "timestamp" in row["rejection_reason"]

    def test_impossible_date_is_rejected(self, spark):
        # month 13 is invalid
        df = make_raw_df(spark, [
            {"event_id": "e16", "user_id": "u1", "event_type": "PURCHASE",
             "event_ts": "2025-13-01T00:00:00Z", "value": 5.0},
        ])
        result = _tag_rejection_reason(df)
        row = result.collect()[0]
        assert row["rejection_reason"] is not None

    def test_multiple_records_split_correctly(self, spark):
        df = make_raw_df(spark, [
            {"event_id": "e1", "user_id": "u1", "event_type": "CLICK",
             "event_ts": "2025-01-01T10:00:00Z", "value": 1.0},
            {"event_id": "e2", "user_id": None, "event_type": "VIEW",
             "event_ts": "2025-01-01T10:05:00Z", "value": 2.0},
            {"event_id": "e3", "user_id": "u2", "event_type": "PURCHASE",
             "event_ts": "bad_date", "value": 3.0},
        ])
        result = _tag_rejection_reason(df)
        valid = result.filter(F.col("rejection_reason").isNull()).count()
        rejected = result.filter(F.col("rejection_reason").isNotNull()).count()
        assert valid == 1
        assert rejected == 2


# ---------------------------------------------------------------------------
# _normalize
# ---------------------------------------------------------------------------

class TestNormalize:
    def test_lowercase_event_type_uppercased(self, spark):
        df = make_raw_df(spark, [
            {"event_id": "e6", "user_id": "u1", "event_type": "click",
             "event_ts": "2025-01-01T11:05:00Z", "value": 2.0},
        ])
        result = _normalize(df)
        row = result.collect()[0]
        assert row["event_type"] == "CLICK"

    def test_mixed_case_event_type_uppercased(self, spark):
        df = make_raw_df(spark, [
            {"event_id": "e_x", "user_id": "u1", "event_type": "Purchase",
             "event_ts": "2025-01-01T12:00:00Z", "value": 10.0},
        ])
        result = _normalize(df)
        row = result.collect()[0]
        assert row["event_type"] == "PURCHASE"

    def test_whitespace_trimmed_from_user_id(self, spark):
        df = make_raw_df(spark, [
            {"event_id": "e_y", "user_id": "  u2  ", "event_type": "VIEW",
             "event_ts": "2025-01-01T12:00:00Z", "value": 1.0},
        ])
        result = _normalize(df)
        row = result.collect()[0]
        assert row["user_id"] == "u2"


# ---------------------------------------------------------------------------
# _deduplicate
# ---------------------------------------------------------------------------

class TestDeduplicate:
    def test_exact_duplicate_removed(self, spark):
        df = make_raw_df(spark, [
            {"event_id": "e3", "user_id": "u1", "event_type": "PURCHASE",
             "event_ts": "2025-01-01T10:10:00Z", "value": 25.0},
            {"event_id": "e3", "user_id": "u1", "event_type": "PURCHASE",
             "event_ts": "2025-01-01T10:10:00Z", "value": 25.0},
        ])
        result = _deduplicate(df)
        assert result.count() == 1

    def test_different_event_ids_kept(self, spark):
        df = make_raw_df(spark, [
            {"event_id": "e1", "user_id": "u1", "event_type": "CLICK",
             "event_ts": "2025-01-01T10:00:00Z", "value": 1.0},
            {"event_id": "e2", "user_id": "u2", "event_type": "VIEW",
             "event_ts": "2025-01-01T10:05:00Z", "value": 2.0},
        ])
        result = _deduplicate(df)
        assert result.count() == 2

    def test_duplicate_keeps_latest_timestamp(self, spark):
        df = make_raw_df(spark, [
            {"event_id": "e3", "user_id": "u1", "event_type": "PURCHASE",
             "event_ts": "2025-01-01T10:10:00Z", "value": 25.0},
            {"event_id": "e3", "user_id": "u1", "event_type": "PURCHASE",
             "event_ts": "2025-01-02T09:00:00Z", "value": 25.0},
        ])
        result = _deduplicate(df)
        assert result.count() == 1
        row = result.collect()[0]
        assert row["event_ts"] == "2025-01-02T09:00:00Z"


# ---------------------------------------------------------------------------
# _cast_timestamp
# ---------------------------------------------------------------------------

class TestCastTimestamp:
    def test_valid_iso_timestamp_parsed(self, spark):
        df = make_raw_df(spark, [
            {"event_id": "e1", "user_id": "u1", "event_type": "CLICK",
             "event_ts": "2025-01-01T10:00:00Z", "value": 1.0},
        ])
        result = _cast_timestamp(df)
        row = result.collect()[0]
        assert row["event_ts"] is not None

    def test_timestamp_type_after_cast(self, spark):
        from pyspark.sql.types import TimestampType
        df = make_raw_df(spark, [
            {"event_id": "e1", "user_id": "u1", "event_type": "CLICK",
             "event_ts": "2025-01-01T10:00:00Z", "value": 1.0},
        ])
        result = _cast_timestamp(df)
        ts_field = [f for f in result.schema.fields if f.name == "event_ts"][0]
        assert isinstance(ts_field.dataType, TimestampType)