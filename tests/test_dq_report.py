"""Unit tests for the DQ report module."""

import datetime

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from pipeline.dq_report import build, print_report


RAW_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("user_id", StringType()),
    StructField("event_type", StringType()),
    StructField("event_ts", StringType()),
    StructField("value", DoubleType()),
])

CLEAN_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("user_id", StringType()),
    StructField("event_type", StringType()),
    StructField("event_ts", TimestampType()),
    StructField("value", DoubleType()),
])

REJECTED_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("user_id", StringType()),
    StructField("event_type", StringType()),
    StructField("event_ts", StringType()),
    StructField("value", DoubleType()),
    StructField("rejection_reason", StringType()),
    StructField("ingested_at", TimestampType()),
])


class TestBuildReport:
    def test_rejection_rate_calculated(self, spark):
        raw = spark.createDataFrame([("e1", "u1", "CLICK", "2025-01-01T10:00:00Z", 1.0)], schema=RAW_SCHEMA)
        clean = spark.createDataFrame([], schema=CLEAN_SCHEMA)
        rejected = spark.createDataFrame(
            [("e1", None, "CLICK", "2025-01-01T10:00:00Z", 1.0, "user_id is null or empty", None)],
            schema=REJECTED_SCHEMA,
        )
        report = build(raw, clean, rejected)
        assert report["total_raw"] == 1
        assert report["clean"] == 0
        assert report["rejected"] == 1
        assert report["rejection_rate_pct"] == 100.0

    def test_zero_rejections(self, spark):
        raw = spark.createDataFrame([("e1", "u1", "CLICK", "2025-01-01T10:00:00Z", 1.0)], schema=RAW_SCHEMA)
        clean = spark.createDataFrame(
            [("e1", "u1", "CLICK", datetime.datetime(2025, 1, 1), 1.0)], schema=CLEAN_SCHEMA
        )
        rejected = spark.createDataFrame([], schema=REJECTED_SCHEMA)
        report = build(raw, clean, rejected)
        assert report["rejection_rate_pct"] == 0.0
        assert report["breakdown"] == {}

    def test_breakdown_counts_by_reason(self, spark):
        raw = spark.createDataFrame([
            ("e1", None,  "CLICK", "2025-01-01T10:00:00Z", 1.0),
            ("e2", None,  "VIEW",  "2025-01-01T10:01:00Z", 2.0),
            ("e3", "u1",  None,    "2025-01-01T10:02:00Z", 3.0),
        ], schema=RAW_SCHEMA)
        clean = spark.createDataFrame([], schema=CLEAN_SCHEMA)
        rejected = spark.createDataFrame([
            ("e1", None, "CLICK", "2025-01-01T10:00:00Z", 1.0, "user_id is null or empty", None),
            ("e2", None, "VIEW",  "2025-01-01T10:01:00Z", 2.0, "user_id is null or empty", None),
            ("e3", "u1", None,    "2025-01-01T10:02:00Z", 3.0, "event_type is null or empty", None),
        ], schema=REJECTED_SCHEMA)
        report = build(raw, clean, rejected)
        assert report["breakdown"]["user_id is null or empty"] == 2
        assert report["breakdown"]["event_type is null or empty"] == 1

    def test_print_report_does_not_raise(self, spark):
        raw = spark.createDataFrame([("e1", "u1", "CLICK", "2025-01-01T10:00:00Z", 1.0)], schema=RAW_SCHEMA)
        clean = spark.createDataFrame(
            [("e1", "u1", "CLICK", datetime.datetime(2025, 1, 1), 1.0)], schema=CLEAN_SCHEMA
        )
        rejected = spark.createDataFrame([], schema=REJECTED_SCHEMA)
        report = build(raw, clean, rejected)
        print_report(report)  # should not raise