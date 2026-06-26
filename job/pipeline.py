import argparse
import logging
import sys
from pathlib import Path

import yaml
from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from pipeline import bronze, silver, gold, dq_report, output_validator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
)
log = logging.getLogger("bvarta.pipeline")


def load_config(path: str) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def get_spark(app_name: str = "bvarta-de-pipeline") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def main(config_path: str, layer: str = "all", date_filter: str | None = None) -> None:
    config = load_config(config_path)
    paths = config["paths"]

    raw_events_path = paths["raw_events"]
    users_path = paths["users"]
    output_path = paths["output"]

    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    try:
        if layer in ("all", "bronze"):
            log.info("Starting Bronze layer%s …", f" [date={date_filter}]" if date_filter else "")
            bronze_df, rejected_df = bronze.run(spark, raw_events_path, output_path, date_filter=date_filter)

            raw_df = bronze.read_raw_events(spark, bronze._resolve_input_paths(raw_events_path, date_filter))
            report = dq_report.build(raw_df, bronze_df, rejected_df)
            dq_report.print_report(report)

            output_validator.validate_bronze(bronze_df)

        if layer in ("all", "silver"):
            if layer == "silver":
                log.info("Reading Bronze output from disk for Silver-only run …")
                bronze_df = spark.read.parquet(f"{output_path}/bronze/events")
                if date_filter:
                    bronze_df = bronze_df.filter(bronze_df["event_date"] == date_filter)
            log.info("Starting Silver layer …")
            silver_df = silver.run(spark, bronze_df, users_path, output_path)
            output_validator.validate_silver(silver_df)

        if layer in ("all", "gold"):
            if layer == "gold":
                log.info("Reading Silver output from disk for Gold-only run …")
                silver_df = spark.read.parquet(f"{output_path}/silver/events")
                if date_filter:
                    silver_df = silver_df.filter(silver_df["event_date"] == date_filter)
            log.info("Starting Gold layer …")
            gold_df = gold.run(spark, silver_df, output_path)
            output_validator.validate_gold(gold_df)

            log.info("Gold preview:")
            gold_df.show(truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bvarta batch data pipeline")
    parser.add_argument("--config", required=True, help="Path to pipeline.yaml")
    parser.add_argument(
        "--layer",
        default="all",
        choices=["all", "bronze", "silver", "gold"],
        help="Run a specific layer only (default: all)",
    )
    parser.add_argument(
        "--date",
        default=None,
        metavar="YYYY-MM-DD",
        help="Process only the file for this date (incremental run)",
    )
    args = parser.parse_args()
    main(args.config, args.layer, args.date)