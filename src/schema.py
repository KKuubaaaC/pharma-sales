"""Schema definitions for raw and transformed data. Used for validation in Extract and tests."""
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from src.constants import (
    COL_CATEGORY_NAME,
    COL_DAY_OF_WEEK,
    COL_DRUG_CATEGORY,
    COL_DATUM,
    COL_IS_WEEKEND,
    COL_MONTH,
    COL_QUARTER,
    COL_SALES,
    COL_SEASON,
    COL_YEAR,
    RAW_ATC_COLUMNS,
)


def get_raw_expected_columns() -> list[str]:
    """Expected column names in raw CSV (wide format)."""
    return ["datum"] + RAW_ATC_COLUMNS + ["Year", "Month", "Hour", "Weekday Name"]


def validate_raw_columns(actual_columns: list[str]) -> None:
    """Raise ValueError if actual columns are missing any required columns."""
    expected = set(get_raw_expected_columns())
    actual = set(actual_columns)
    missing = expected - actual
    if missing:
        raise ValueError(
            f"Schema validation failed: missing columns {sorted(missing)}. "
            f"Expected (subset): {sorted(expected)}."
        )


# Transformed (long) schema - main columns we expect after transform
TRANSFORMED_STRUCT = StructType(
    [
        StructField(COL_DATUM, StringType(), True),
        StructField(COL_DRUG_CATEGORY, StringType(), True),
        StructField(COL_SALES, DoubleType(), True),
        StructField(COL_YEAR, IntegerType(), True),
        StructField(COL_QUARTER, IntegerType(), True),
        StructField(COL_MONTH, IntegerType(), True),
        StructField(COL_DAY_OF_WEEK, IntegerType(), True),
        StructField(COL_IS_WEEKEND, IntegerType(), True),
        StructField(COL_CATEGORY_NAME, StringType(), True),
        StructField(COL_SEASON, StringType(), True),
    ]
)


def get_transformed_expected_columns() -> list[str]:
    """Column names expected in transformed (long) DataFrame."""
    return [f.name for f in TRANSFORMED_STRUCT.fields]
