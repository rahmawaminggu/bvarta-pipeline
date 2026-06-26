from __future__ import annotations

import logging
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)

from pipeline import schema_guard

log = logging.getLogger("bvarta.bronze")

RAW_SCHEMA = StructType([
    StructField("event_id", StringType(), nullable=True),
    StructField("user_id", StringType(), nullable=True),
    StructField("event_type", StringType(), nullable=True),
    StructField("event_ts", StringType(), nullable=True),
    StructField("value", DoubleType(), nullable=True),
])

REJECTION_REASONS = {
    "null_event_id": "event_id is null or empty",
    "null_user_id": "user_id is null or empty",
    "null_event_type": "event_type is null or empty",
    "null_event_ts": "event_ts is null or empty",
    "invalid_event_ts": "event_ts cannot be parsed as a valid timestamp",
}


def _resolve_input_paths(base_path: str, date_filter: str | None) -> list[str]:
    if date_filter:
        target = os.path.join(base_path, f"day_{date_filter}.jsonl")
        if not os.path.exists(target):
            raise FileNotFoundError(
                f"No raw file found for date {date_filter}: {target}"
            )
        log.info("Date filter active — processing only: %s", target)
        return [target]

    paths = [
        os.path.join(base_path, f)
        for f in sorted(os.listdir(base_path))
        if f.endswith(".jsonl")
    ]
    if not paths:
        raise FileNotFoundError(f"No .jsonl files found in {base_path}")
    log.info("Processing %d file(s) from %s", len(paths), base_path)
    return paths


def read_raw_events(spark: SparkSession, path: str | list[str]) -> DataFrame:
    return (
        spark.read
        .schema(RAW_SCHEMA)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .json(path)
    )


def _tag_rejection_reason(df: DataFrame) -> DataFrame:
    ts_parsed = F.try_to_timestamp(F.col("event_ts"))

    return df.withColumn(
        "rejection_reason",
        F.when(F.col("event_id").isNull() | (F.trim(F.col("event_id")) == ""), F.lit(REJECTION_REASONS["null_event_id"]))
        .when(F.col("user_id").isNull() | (F.trim(F.col("user_id")) == ""), F.lit(REJECTION_REASONS["null_user_id"]))
        .when(F.col("event_type").isNull() | (F.trim(F.col("event_type")) == ""), F.lit(REJECTION_REASONS["null_event_type"]))
        .when(F.col("event_ts").isNull() | (F.trim(F.col("event_ts")) == ""), F.lit(REJECTION_REASONS["null_event_ts"]))
        .when(ts_parsed.isNull(), F.lit(REJECTION_REASONS["invalid_event_ts"]))
        .otherwise(F.lit(None).cast(StringType()))
    )


def _normalize(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("event_id", F.trim(F.col("event_id")))
        .withColumn("user_id", F.trim(F.col("user_id")))
        .withColumn("event_type", F.upper(F.trim(F.col("event_type"))))
        .withColumn("event_ts", F.trim(F.col("event_ts")))
    )


def _deduplicate(df: DataFrame) -> DataFrame:
    from pyspark.sql.window import Window

    window = Window.partitionBy("event_id").orderBy(F.col("event_ts").desc())
    return (
        df.withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )


def _cast_timestamp(df: DataFrame) -> DataFrame:
    return df.withColumn("event_ts", F.try_to_timestamp(F.col("event_ts")))


def run(
    spark: SparkSession,
    raw_path: str,
    output_path: str,
    date_filter: str | None = None,
) -> tuple[DataFrame, DataFrame]:
    input_paths = _resolve_input_paths(raw_path, date_filter)
    raw_df = read_raw_events(spark, input_paths)

    schema_guard.check(raw_df, RAW_SCHEMA)

    tagged_df = _tag_rejection_reason(raw_df)

    rejected_df = (
        tagged_df
        .filter(F.col("rejection_reason").isNotNull())
        .withColumn("ingested_at", F.current_timestamp())
    )

    clean_df = (
        tagged_df
        .filter(F.col("rejection_reason").isNull())
        .drop("rejection_reason")
    )

    clean_df = _normalize(clean_df)
    clean_df = _deduplicate(clean_df)
    clean_df = _cast_timestamp(clean_df)
    clean_df = clean_df.withColumn("event_date", F.to_date(F.col("event_ts")))

    (
        clean_df
        .write
        .mode("overwrite")
        .partitionBy("event_date")
        .parquet(f"{output_path}/bronze/events")
    )

    (
        rejected_df
        .write
        .mode("overwrite")
        .parquet(f"{output_path}/bronze/rejected")
    )

    return clean_df, rejected_df
