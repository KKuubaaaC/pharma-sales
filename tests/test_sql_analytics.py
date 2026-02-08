"""Tests for SQL analytics: queries return expected columns and types."""
import pytest
from pyspark.sql import SparkSession

from src.extract import extract_raw_data
from src.transform import transform_raw_data
from src.sql_analytics import run_all_analytics


def test_sql_top10_returns_data_and_columns(
    spark: SparkSession, sample_data_path: str
):
    """Top10 query returns DataFrame with drug_category, total_sales."""
    raw = extract_raw_data(spark, sample_data_path)
    cleaned = transform_raw_data(spark, raw)
    analytics = run_all_analytics(spark, cleaned)
    df = analytics["top10_categories"]
    assert "drug_category" in df.columns
    assert "total_sales" in df.columns
    assert df.count() <= 10
    assert df.count() >= 1


def test_sql_quarterly_trend_has_prev_quarter(
    spark: SparkSession, sample_data_path: str
):
    """Quarterly trend has year, quarter, quarterly_sales, prev_quarter."""
    raw = extract_raw_data(spark, sample_data_path)
    cleaned = transform_raw_data(spark, raw)
    analytics = run_all_analytics(spark, cleaned)
    df = analytics["quarterly_trend"]
    assert "year" in df.columns
    assert "quarter" in df.columns
    assert "quarterly_sales" in df.columns
    assert "prev_quarter" in df.columns
    assert df.count() >= 1


def test_sql_yoy_growth_has_yoy_column(
    spark: SparkSession, sample_data_path: str
):
    """YoY growth has drug_category, year, yearly_sales, yoy_growth_pct."""
    raw = extract_raw_data(spark, sample_data_path)
    cleaned = transform_raw_data(spark, raw)
    analytics = run_all_analytics(spark, cleaned)
    df = analytics["yoy_growth"]
    assert "drug_category" in df.columns
    assert "year" in df.columns
    assert "yearly_sales" in df.columns
    assert "yoy_growth_pct" in df.columns
    assert df.count() >= 1


def test_sql_anomalies_returns_dataframe(
    spark: SparkSession, sample_data_path: str
):
    """Anomalies query returns DataFrame with expected columns (may be empty)."""
    raw = extract_raw_data(spark, sample_data_path)
    cleaned = transform_raw_data(spark, raw)
    analytics = run_all_analytics(spark, cleaned)
    df = analytics["anomalies"]
    assert "datum" in df.columns
    assert "drug_category" in df.columns
    assert "sales" in df.columns
