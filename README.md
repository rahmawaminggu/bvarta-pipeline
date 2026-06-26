# Bvarta Data Engineer – Batch Pipeline (PySpark)

A production-style batch ETL pipeline that ingests raw user event data, cleans it, enriches it with reference data, and produces analytics-ready aggregates following the **Medallion Architecture** (Bronze → Silver → Gold).

---

## Pipeline Design

```
data/raw/events/*.jsonl
        │
        ▼
┌────────────────────────────────────────────────────┐
│  BRONZE LAYER  (src/pipeline/bronze.py)            │
│  • Schema evolution guard (warn on drift)          │
│  • Explicit schema on read (PERMISSIVE mode)       │
│  • Tag & isolate rejected records                  │
│  • Normalize: uppercase event_type, trim strings   │
│  • Deduplicate by event_id                         │
│  • DQ report printed after completion              │
│  • Output: bronze/events/ + bronze/rejected/       │
└──────────────────┬─────────────────────────────────┘
                   │
                   ▼
┌────────────────────────────────────────────────────┐
│  SILVER LAYER  (src/pipeline/silver.py)            │
│  • LEFT JOIN with users.csv reference              │
│  • Coerce invalid signup_date → null               │
│  • Derived: event_date, is_purchase,               │
│    days_since_signup                               │
│  • Output: silver/events/                          │
└──────────────────┬─────────────────────────────────┘
                   │
                   ▼
┌────────────────────────────────────────────────────┐
│  GOLD LAYER  (src/pipeline/gold.py)                │
│  • Daily × country aggregation                     │
│  • total_events, total_value, total_purchases,     │
│    unique_users                                    │
│  • Output validation after write                   │
│  • Output: gold/daily_metrics/                     │
└────────────────────────────────────────────────────┘
```

All outputs are written in **Parquet**, partitioned by `event_date`.

---

## Data Quality Rules

| Check | Field | Action |
|---|---|---|
| Null or empty | `event_id` | Quarantine |
| Null or empty | `user_id` | Quarantine |
| Null or empty | `event_type` | Quarantine |
| Null or empty | `event_ts` | Quarantine |
| Unparseable timestamp | `event_ts` | Quarantine |
| Lowercase / mixed-case | `event_type` | Normalize → UPPER |
| Surrounding whitespace | all string fields | Trim |
| Duplicate `event_id` | — | Keep record with latest `event_ts` |
| Invalid `signup_date` in reference | users.csv | Coerce to null |
| Empty `country` in reference | users.csv | Coerce to null |
| `value` type mismatch (string instead of double) | — | PERMISSIVE read → null |

Rejected records are written to `output/bronze/rejected/` with an `ingested_at` timestamp and a human-readable `rejection_reason` column for auditability.

---

## Incremental & Late Data Strategy

**Strategy: Partition overwrite (dynamic)**

- All outputs are partitioned by `event_date`.
- Spark is configured with `spark.sql.sources.partitionOverwriteMode = dynamic`.
- Each pipeline run overwrites only the partitions it touches — other partitions remain intact.

**Why overwrite instead of merge/upsert?**

For a batch pipeline over flat files, partition overwrite is simpler, cheaper, and equally correct:
- Re-processing the same input file produces identical output (**idempotent**).
- A late event for `2024-12-31` appearing in a later file batch will re-compute and overwrite only the `event_date=2024-12-31` partition in Silver and Gold — no stale aggregates, no duplicates.
- A Delta Lake MERGE would be justified if we needed row-level streaming upserts, which this exercise does not require.

---

## Project Structure

```
bvarta_pipeline/
├── src/
│   └── pipeline/
│       ├── __init__.py
│       ├── bronze.py            # Ingestion, schema, cleaning
│       ├── silver.py            # Enrichment, derived fields
│       ├── gold.py              # Aggregations
│       ├── dq_report.py         # Data quality summary report
│       ├── schema_guard.py      # Schema evolution detection
│       └── output_validator.py  # Post-write sanity checks
├── job/
│   └── pipeline.py              # CLI entry point
├── tests/
│   ├── conftest.py              # Shared SparkSession fixture
│   ├── test_bronze.py           # 16 tests
│   ├── test_silver.py           # 8 tests
│   ├── test_gold.py             # 8 tests
│   ├── test_dq_report.py        # 4 tests
│   ├── test_schema_guard.py     # 5 tests
│   ├── test_output_validator.py # 9 tests
│   └── test_date_filter.py      # 5 tests
├── config/
│   └── pipeline.yaml            # Paths and layer configuration
├── data/
│   ├── raw/events/              # Input JSONL files (one per day)
│   └── reference/              # users.csv
├── output/                      # Generated at runtime
├── Dockerfile
├── docker-compose.yml
├── pyproject.toml
└── README.md
```

---

## Prerequisites

- Python 3.12+ and Java 11/17/21 (required by PySpark)

```bash
# macOS – install Java via Homebrew (if not already installed)
brew install openjdk@21
export JAVA_HOME=/opt/homebrew/opt/openjdk@21
export PATH="$JAVA_HOME/bin:$PATH"
```

---

## Setup

```bash
# 1. Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate

# 2. Install runtime + dev dependencies
pip install -e ".[dev]"
```

---

## Running the Pipeline

```bash
# Run the full pipeline (Bronze → Silver → Gold)
python job/pipeline.py --config config/pipeline.yaml

# Run a specific layer only
python job/pipeline.py --config config/pipeline.yaml --layer bronze
python job/pipeline.py --config config/pipeline.yaml --layer silver
python job/pipeline.py --config config/pipeline.yaml --layer gold

# Incremental run — process only a specific date's file
python job/pipeline.py --config config/pipeline.yaml --date 2025-01-01

# Combine: run only Bronze for a specific date
python job/pipeline.py --config config/pipeline.yaml --layer bronze --date 2025-01-01
```

Outputs written to `output/`:

```
output/
├── bronze/
│   ├── events/          # Clean events, partitioned by event_date
│   └── rejected/        # Quarantined records with rejection_reason
├── silver/
│   └── events/          # Enriched events, partitioned by event_date
└── gold/
    └── daily_metrics/   # Daily country-level aggregates, partitioned by event_date
```

After Bronze completes, a **Data Quality Report** is printed to stdout:

```
╔══════════════════════════════════════════════╗
║  DATA QUALITY REPORT                         ║
╠══════════════════════════════════════════════╣
║  Total raw records       :             17    ║
║  Clean records           :             10    ║
║  Rejected records        :              7    ║
║  Rejection rate          :          41.2%    ║
╠══════════════════════════════════════════════╣
║  REJECTION BREAKDOWN                         ║
║  event_ts cannot be pars :              3    ║
║  user_id is null or empt :              2    ║
║  event_type is null or e :              1    ║
║  event_id is null or emp :              1    ║
╚══════════════════════════════════════════════╝
```

---

## Running Tests

```bash
# Run all 55 unit tests
python -m pytest tests/ -v

# With coverage report
python -m pytest tests/ --cov=src/pipeline --cov-report=term-missing
```

---

## Running with Docker

```bash
# Build and run the full pipeline
docker compose up pipeline

# Run tests inside the container
docker compose up tests
```

---

## Assumptions Made

1. **Deduplication key**: `event_id` is the natural key. When the same `event_id` appears in multiple files (late re-delivery), the record with the **latest `event_ts`** is kept.
2. **Unknown users**: Events for users not found in `users.csv` are **retained** in Silver/Gold with `country = null`. Dropping them would lose valid business events.
3. **`value = null`**: A null event value is preserved (not defaulted to 0) because the distinction between "no value" and "zero value" is semantically meaningful for aggregation.
4. **Timezone**: All timestamps treated as UTC. Spark session timezone is set to UTC.
5. **`country = null` in Gold**: Included so analysts can measure the volume of events from unknown/unmatched users.
6. **File naming convention**: Incremental `--date` filter expects files named `day_YYYY-MM-DD.jsonl`.

---

## Additional Initiatives

| Feature | Module | Description |
|---|---|---|
| **`--layer` flag** | `job/pipeline.py` | Run Bronze/Silver/Gold independently for debugging or partial re-runs |
| **`--date` filter** | `bronze.py` | Process only a specific day's file — simulates production scheduler incremental runs |
| **DQ Report** | `dq_report.py` | Post-Bronze summary: total/clean/rejected counts, rejection rate, breakdown by reason |
| **Schema Guard** | `schema_guard.py` | Detects missing columns, extra columns, and type mismatches before processing begins |
| **Output Validator** | `output_validator.py` | Post-write checks: no empty outputs, no unexpected nulls, no negative metrics |
| **Docker support** | `Dockerfile`, `docker-compose.yml` | Zero-dependency local run via `docker compose up pipeline` |