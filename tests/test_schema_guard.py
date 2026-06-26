"""Unit tests for the schema evolution guard."""

from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from pipeline.schema_guard import check, SchemaCheckResult


EXPECTED = StructType([
    StructField("event_id", StringType()),
    StructField("user_id", StringType()),
    StructField("event_type", StringType()),
    StructField("event_ts", StringType()),
    StructField("value", DoubleType()),
])


class TestSchemaGuard:
    def test_matching_schema_is_clean(self, spark):
        df = spark.createDataFrame(
            [("e1", "u1", "CLICK", "2025-01-01T10:00:00Z", 1.0)],
            schema=EXPECTED,
        )
        result = check(df, EXPECTED)
        assert result.is_clean

    def test_missing_column_detected(self, spark):
        partial_schema = StructType([
            StructField("event_id", StringType()),
            StructField("user_id", StringType()),
        ])
        df = spark.createDataFrame([("e1", "u1")], schema=partial_schema)
        result = check(df, EXPECTED)
        assert "event_type" in result.missing_columns
        assert "event_ts" in result.missing_columns
        assert "value" in result.missing_columns

    def test_extra_column_detected(self, spark):
        extra_schema = StructType(EXPECTED.fields + [
            StructField("new_mystery_column", StringType()),
        ])
        df = spark.createDataFrame(
            [("e1", "u1", "CLICK", "2025-01-01T10:00:00Z", 1.0, "extra")],
            schema=extra_schema,
        )
        result = check(df, EXPECTED)
        assert "new_mystery_column" in result.extra_columns

    def test_type_mismatch_detected(self, spark):
        wrong_type_schema = StructType([
            StructField("event_id", StringType()),
            StructField("user_id", StringType()),
            StructField("event_type", StringType()),
            StructField("event_ts", StringType()),
            StructField("value", LongType()),   # should be DoubleType
        ])
        df = spark.createDataFrame(
            [("e1", "u1", "CLICK", "2025-01-01T10:00:00Z", 1)],
            schema=wrong_type_schema,
        )
        result = check(df, EXPECTED)
        assert len(result.type_mismatches) == 1
        assert "value" in result.type_mismatches[0]

    def test_no_false_positives_on_exact_match(self, spark):
        df = spark.createDataFrame(
            [("e1", "u1", "CLICK", "2025-01-01T10:00:00Z", 1.0)],
            schema=EXPECTED,
        )
        result = check(df, EXPECTED)
        assert result.missing_columns == []
        assert result.extra_columns == []
        assert result.type_mismatches == []