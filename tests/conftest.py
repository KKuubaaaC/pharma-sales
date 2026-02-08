"""Pytest fixtures: SparkSession and sample data path."""
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Session-scoped SparkSession for tests."""
    return (
        SparkSession.builder.appName("PharmaSalesTests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


@pytest.fixture(scope="session")
def sample_data_path():
    """Path to sample CSV (relative to project root)."""
    from pathlib import Path
    root = Path(__file__).resolve().parent.parent
    return str(root / "data" / "sample" / "salesdaily_sample.csv")
