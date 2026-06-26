from __future__ import annotations

import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

log = logging.getLogger("bvarta.dq_report")


def build(raw_df: DataFrame, clean_df: DataFrame, rejected_df: DataFrame) -> dict:
    total = raw_df.count()
    clean = clean_df.count()
    rejected = rejected_df.count()
    rate = (rejected / total * 100) if total > 0 else 0.0

    breakdown_rows = (
        rejected_df
        .groupBy("rejection_reason")
        .agg(F.count("*").alias("count"))
        .orderBy(F.col("count").desc())
        .collect()
    )
    breakdown = {row["rejection_reason"]: row["count"] for row in breakdown_rows}

    return {
        "total_raw": total,
        "clean": clean,
        "rejected": rejected,
        "rejection_rate_pct": round(rate, 1),
        "breakdown": breakdown,
    }


def print_report(report: dict) -> None:
    W = 46
    border = "═" * W

    def row(label: str, value: str) -> str:
        content = f"  {label:<26}: {value:>10}"
        return f"║{content:<{W}}║"

    lines = [
        f"╔{border}╗",
        f"║{'  DATA QUALITY REPORT':<{W}}║",
        f"╠{border}╣",
        row("Total raw records", str(report["total_raw"])),
        row("Clean records", str(report["clean"])),
        row("Rejected records", str(report["rejected"])),
        row("Rejection rate", f"{report['rejection_rate_pct']}%"),
        f"╠{border}╣",
        f"║{'  REJECTION BREAKDOWN':<{W}}║",
    ]

    if report["breakdown"]:
        for reason, count in report["breakdown"].items():
            short = reason[:24] if len(reason) > 24 else reason
            lines.append(row(short, str(count)))
    else:
        lines.append(f"║{'  (none)':<{W}}║")

    lines.append(f"╚{border}╝")
    print("\n".join(lines))
    log.info("DQ report: total=%d clean=%d rejected=%d rate=%.1f%%",
             report["total_raw"], report["clean"],
             report["rejected"], report["rejection_rate_pct"])