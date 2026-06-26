from __future__ import annotations

import logging
from dataclasses import dataclass, field

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

log = logging.getLogger("bvarta.schema_guard")


@dataclass
class SchemaCheckResult:
    missing_columns: list[str] = field(default_factory=list)
    extra_columns: list[str] = field(default_factory=list)
    type_mismatches: list[str] = field(default_factory=list)

    @property
    def is_clean(self) -> bool:
        return not (self.missing_columns or self.extra_columns or self.type_mismatches)


def check(df: DataFrame, expected_schema: StructType) -> SchemaCheckResult:
    actual_fields = {f.name: f.dataType for f in df.schema.fields}
    expected_fields = {f.name: f.dataType for f in expected_schema.fields}

    result = SchemaCheckResult()

    for col_name, expected_type in expected_fields.items():
        if col_name not in actual_fields:
            result.missing_columns.append(col_name)
            log.warning("[SCHEMA GUARD] Missing expected column '%s' (%s) in raw data.", col_name, expected_type)
        else:
            actual_type = actual_fields[col_name]
            if type(actual_type) != type(expected_type):
                mismatch = f"{col_name}: expected={expected_type} got={actual_type}"
                result.type_mismatches.append(mismatch)
                log.warning("[SCHEMA GUARD] Type mismatch for '%s': expected=%s, got=%s", col_name, expected_type, actual_type)

    for col_name in actual_fields:
        if col_name not in expected_fields:
            result.extra_columns.append(col_name)
            log.warning("[SCHEMA GUARD] Unexpected column '%s' found in raw data (schema drift?).", col_name)

    if result.is_clean:
        log.info("[SCHEMA GUARD] Schema check passed — no issues detected.")
    else:
        log.warning("[SCHEMA GUARD] Schema issues found: missing=%s extra=%s type_mismatches=%s",
                    result.missing_columns, result.extra_columns, result.type_mismatches)

    return result