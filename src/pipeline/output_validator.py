from __future__ import annotations

import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

log = logging.getLogger("bvarta.output_validator")


class OutputValidationError(Exception):
    pass


def _assert_no_nulls(df: DataFrame, columns: list[str], layer: str) -> None:
    for col in columns:
        null_count = df.filter(F.col(col).isNull()).count()
        if null_count > 0:
            raise OutputValidationError(
                f"[{layer}] Column '{col}' has {null_count} unexpected null(s)."
            )


def _assert_has_rows(df: DataFrame, layer: str) -> None:
    if df.count() == 0:
        raise OutputValidationError(
            f"[{layer}] Output is empty — no rows written."
        )


def validate_bronze(clean_df: DataFrame) -> None:
    log.info("[VALIDATOR] Validating Bronze output …")
    _assert_has_rows(clean_df, "Bronze")
    _assert_no_nulls(clean_df, ["event_id", "event_ts", "event_type", "user_id"], "Bronze")
    log.info("[VALIDATOR] Bronze OK — %d clean rows.", clean_df.count())


def validate_silver(silver_df: DataFrame) -> None:
    log.info("[VALIDATOR] Validating Silver output …")
    _assert_has_rows(silver_df, "Silver")
    _assert_no_nulls(silver_df, ["event_id", "event_date", "event_type", "user_id"], "Silver")
    log.info("[VALIDATOR] Silver OK — %d rows.", silver_df.count())


def validate_gold(gold_df: DataFrame) -> None:
    log.info("[VALIDATOR] Validating Gold output …")
    _assert_has_rows(gold_df, "Gold")
    _assert_no_nulls(gold_df, ["event_date"], "Gold")

    for metric in ("total_events", "total_purchases", "unique_users"):
        negative = gold_df.filter(F.col(metric) < 0).count()
        if negative > 0:
            raise OutputValidationError(
                f"[Gold] Column '{metric}' has {negative} negative value(s)."
            )

    log.info("[VALIDATOR] Gold OK — %d aggregate rows.", gold_df.count())