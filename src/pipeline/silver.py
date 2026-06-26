from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, StringType, StructField, StructType


USER_SCHEMA = StructType([
    StructField("user_id", StringType(), nullable=True),
    StructField("country", StringType(), nullable=True),
    StructField("signup_date", StringType(), nullable=True),
])


def read_users(spark: SparkSession, path: str) -> DataFrame:
    df = (
        spark.read
        .schema(USER_SCHEMA)
        .option("header", "true")
        .csv(path)
    )
    df = df.withColumn(
        "signup_date",
        F.try_to_timestamp(F.col("signup_date"), F.lit("yyyy-MM-dd")).cast(DateType())
    )
    df = (
        df
        .withColumn("user_id", F.trim(F.col("user_id")))
        .withColumn("country", F.when(
            F.col("country").isNull() | (F.trim(F.col("country")) == ""),
            F.lit(None).cast(StringType())
        ).otherwise(F.trim(F.col("country"))))
    )
    return df


def _add_derived_fields(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("is_purchase", F.col("event_type") == "PURCHASE")
        .withColumn(
            "days_since_signup",
            F.when(
                F.col("signup_date").isNotNull(),
                F.datediff(F.col("event_date"), F.col("signup_date"))
            ).otherwise(F.lit(None))
        )
    )


def run(
    spark: SparkSession,
    bronze_df: DataFrame,
    users_path: str,
    output_path: str,
) -> DataFrame:
    users_df = read_users(spark, users_path)

    enriched_df = bronze_df.join(users_df, on="user_id", how="left")
    enriched_df = _add_derived_fields(enriched_df)

    (
        enriched_df
        .write
        .mode("overwrite")
        .partitionBy("event_date")
        .parquet(f"{output_path}/silver/events")
    )

    return enriched_df