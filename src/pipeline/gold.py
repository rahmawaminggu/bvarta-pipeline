from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def run(spark: SparkSession, silver_df: DataFrame, output_path: str) -> DataFrame:
    daily_metrics_df = (
        silver_df
        .groupBy("event_date", "country")
        .agg(
            F.count("event_id").alias("total_events"),
            F.sum("value").alias("total_value"),
            F.sum(F.col("is_purchase").cast("long")).alias("total_purchases"),
            F.countDistinct("user_id").alias("unique_users"),
        )
        .orderBy("event_date", "country")
    )

    (
        daily_metrics_df
        .write
        .mode("overwrite")
        .partitionBy("event_date")
        .parquet(f"{output_path}/gold/daily_metrics")
    )

    return daily_metrics_df