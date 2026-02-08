"""Tests for extract module."""
import pytest
from pyspark.sql import SparkSession

from src.extract import extract_raw_data, ExtractError
from src.schema import get_raw_expected_columns


def test_extract_loads_sample_and_has_schema(spark: SparkSession, sample_data_path: str):
    """Extract from sample CSV returns DataFrame with expected columns and rows."""
    df = extract_raw_data(spark, sample_data_path)
    expected = set(get_raw_expected_columns())
    assert set(df.columns) == expected
    assert df.count() > 0


def test_extract_fails_on_missing_file(spark: SparkSession):
    """Extract raises ExtractError when path does not exist."""
    with pytest.raises(ExtractError, match="does not exist"):
        extract_raw_data(spark, "/nonexistent/path/sales.csv")
