"""Transform: unpivot wide to long, clean, add time/season features, and aggregations."""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql.window import Window

from src.constants import (
    ATC_CODE_TO_NAME,
    COL_CATEGORY_NAME,
    COL_DATUM,
    COL_DAY_OF_WEEK,
    COL_DRUG_CATEGORY,
    COL_IS_WEEKEND,
    COL_MONTH,
    COL_QUARTER,
    COL_SALES,
    COL_SEASON,
    COL_YEAR,
    RAW_ATC_COLUMNS,
)


def _unpivot_wide_to_long(df: DataFrame) -> DataFrame:
    """Convert wide format (one column per ATC) to long (datum, drug_category, sales)."""
    # Build stack expression: stack(8, 'M01AB', M01AB, 'M01AE', M01AE, ...)
    stack_parts = []
    for col_name in RAW_ATC_COLUMNS:
        stack_parts.append(f"'{col_name}', `{col_name}`")
    stack_expr = "stack(" + str(len(RAW_ATC_COLUMNS)) + ", " + ", ".join(stack_parts) + ") as (drug_category, sales)"

    long_df = (
        df.select(
            F.col("datum"),
            F.expr(stack_expr),
            F.col("Year").cast(IntegerType()).alias("year"),
            F.col("Month").cast(IntegerType()).alias("month"),
        )
        .withColumn("sales", F.col("sales").cast(DoubleType()))
    )
    return long_df


def _parse_datum_to_date(df: DataFrame) -> DataFrame:
    """Parse datum (e.g. 1/2/2014) to date and add year/month/quarter from it for consistency."""
    # datum can be "1/2/2014" (M/D/YYYY) or already date-like
    df = df.withColumn(
        "_parsed_date",
        F.to_date(F.col(COL_DATUM), "M/d/yyyy"),
    )
    df = df.withColumn(COL_YEAR, F.year(F.col("_parsed_date")))
    df = df.withColumn(COL_MONTH, F.month(F.col("_parsed_date")))
    df = df.withColumn(COL_QUARTER, F.quarter(F.col("_parsed_date")))
    df = df.withColumn(COL_DAY_OF_WEEK, F.dayofweek(F.col("_parsed_date")))
    df = df.withColumn(
        COL_IS_WEEKEND,
        F.when(F.col(COL_DAY_OF_WEEK).isin(1, 7), 1).otherwise(0),
    )
    return df.drop("_parsed_date")


def _add_category_name(df: DataFrame) -> DataFrame:
    """Map ATC code to full category name."""
    mapping_expr = F.create_map(
        [F.lit(c) for pair in ATC_CODE_TO_NAME.items() for c in pair]
    )
    return df.withColumn(COL_CATEGORY_NAME, mapping_expr[F.col(COL_DRUG_CATEGORY)])


def _add_season(df: DataFrame) -> DataFrame:
    """Add season: winter (12,1,2), spring (3,4,5), summer (6,7,8), autumn (9,10,11)."""
    return df.withColumn(
        COL_SEASON,
        F.when(F.col(COL_MONTH).isin(12, 1, 2), "winter")
        .when(F.col(COL_MONTH).isin(3, 4, 5), "spring")
        .when(F.col(COL_MONTH).isin(6, 7, 8), "summer")
        .otherwise("autumn"),
    )


def transform_raw_data(spark: SparkSession, raw_df: DataFrame) -> DataFrame:
    """
    Full transform: unpivot, clean, add time columns, ATC names, season.
    Keeps daily grain; idempotent and deterministic.
    """
    # 1. Unpivot
    df = _unpivot_wide_to_long(raw_df)
    # 2. Parse date and add time columns (overwrite year/month from CSV for consistency)
    df = _parse_datum_to_date(df)
    # 3. Drop duplicates (datum + drug_category)
    df = df.dropDuplicates([COL_DATUM, COL_DRUG_CATEGORY])
    # 4. Handle nulls and zeros: treat 0 as null, then fill sales nulls with median per drug_category
    df = df.withColumn(
        COL_SALES,
        F.when(F.col(COL_SALES) == 0, F.lit(None)).otherwise(F.col(COL_SALES)),
    )
    median_df = (
        df.groupBy(COL_DRUG_CATEGORY)
        .agg(F.percentile_approx(COL_SALES, 0.5, 100000).alias("_med"))
    )
    df = df.join(median_df, COL_DRUG_CATEGORY, "left").withColumn(
        COL_SALES,
        F.coalesce(F.col(COL_SALES), F.col("_med")),
    ).drop("_med")
    # Drop rows that are still null (e.g. category with no non-null sales)
    df = df.dropna(subset=[COL_SALES])
    # 5. Filter valid date range (e.g. 2014–2019)
    df = df.filter((F.col(COL_YEAR) >= 2014) & (F.col(COL_YEAR) <= 2019))
    # 6. Category name and season
    df = _add_category_name(df)
    df = _add_season(df)
    return df


def add_rolling_metrics(df: DataFrame, window_days: int = 7) -> DataFrame:
    """Add rolling average of sales per drug_category over window_days (optional)."""
    w = (
        Window.partitionBy(COL_DRUG_CATEGORY)
        .orderBy(F.col(COL_DATUM))
        .rowsBetween(-(window_days - 1), 0)
    )
    return df.withColumn(
        f"rolling_avg_{window_days}d",
        F.avg(COL_SALES).over(w),
    )


def add_monthly_rank(df: DataFrame) -> DataFrame:
    """Add rank of drug_category by total sales within each year-month (optional)."""
    w = (
        Window.partitionBy(COL_YEAR, COL_MONTH)
        .orderBy(F.sum(COL_SALES).desc(), COL_DRUG_CATEGORY)
    )
    monthly_totals = (
        df.groupBy(COL_YEAR, COL_MONTH, COL_DRUG_CATEGORY)
        .agg(F.sum(COL_SALES).alias("_monthly_sales"))
        .withColumn("rank_in_month", F.row_number().over(w))
    )
    df = df.join(
        monthly_totals,
        [COL_YEAR, COL_MONTH, COL_DRUG_CATEGORY],
        "left",
    ).drop("_monthly_sales")
    return df
