"""Tests for transform module."""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.constants import (
    COL_DATUM,
    COL_DRUG_CATEGORY,
    COL_SALES,
    COL_YEAR,
    COL_QUARTER,
    COL_MONTH,
    COL_IS_WEEKEND,
    COL_CATEGORY_NAME,
    COL_SEASON,
    RAW_ATC_COLUMNS,
)
from src.extract import extract_raw_data
from src.transform import transform_raw_data


def test_transform_unpivot_has_expected_columns(
    spark: SparkSession, sample_data_path: str
):
    """After transform, DataFrame has long-format columns."""
    raw = extract_raw_data(spark, sample_data_path)
    df = transform_raw_data(spark, raw)
    required = {
        COL_DATUM,
        COL_DRUG_CATEGORY,
        COL_SALES,
        COL_YEAR,
        COL_QUARTER,
        COL_MONTH,
        COL_IS_WEEKEND,
        COL_CATEGORY_NAME,
        COL_SEASON,
    }
    assert required.issubset(set(df.columns))


def test_transform_no_duplicates_on_datum_drug_category(
    spark: SparkSession, sample_data_path: str
):
    """No duplicate (datum, drug_category) rows after transform."""
    raw = extract_raw_data(spark, sample_data_path)
    df = transform_raw_data(spark, raw)
    n = df.count()
    n_distinct = df.dropDuplicates([COL_DATUM, COL_DRUG_CATEGORY]).count()
    assert n == n_distinct


def test_transform_drug_categories_are_atc_codes(
    spark: SparkSession, sample_data_path: str
):
    """drug_category values are only from RAW_ATC_COLUMNS."""
    raw = extract_raw_data(spark, sample_data_path)
    df = transform_raw_data(spark, raw)
    distinct_cats = [r[COL_DRUG_CATEGORY] for r in df.select(COL_DRUG_CATEGORY).distinct().collect()]
    assert set(distinct_cats).issubset(set(RAW_ATC_COLUMNS))


def test_transform_sales_numeric_and_non_negative(
    spark: SparkSession, sample_data_path: str
):
    """Sales column is numeric and has no negative values."""
    raw = extract_raw_data(spark, sample_data_path)
    df = transform_raw_data(spark, raw)
    assert df.filter(F.col(COL_SALES) < 0).count() == 0
    assert df.filter(F.col(COL_SALES).isNull()).count() == 0


def test_transform_year_quarter_month_in_range(
    spark: SparkSession, sample_data_path: str
):
    """Year 2014-2019, month 1-12, quarter 1-4."""
    raw = extract_raw_data(spark, sample_data_path)
    df = transform_raw_data(spark, raw)
    assert df.filter((F.col(COL_YEAR) < 2014) | (F.col(COL_YEAR) > 2019)).count() == 0
    assert df.filter((F.col(COL_MONTH) < 1) | (F.col(COL_MONTH) > 12)).count() == 0
    assert df.filter((F.col(COL_QUARTER) < 1) | (F.col(COL_QUARTER) > 4)).count() == 0
