"""Extract: load raw CSV into Spark DataFrame with schema validation and logging."""
import logging
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from src.schema import validate_raw_columns

logger = logging.getLogger(__name__)


class ExtractError(Exception):
    """Raised when extraction or validation fails."""

    pass


def extract_raw_data(spark: SparkSession, path: str) -> DataFrame:
    """
    Load CSV from path into a DataFrame. Validates schema and logs row count.

    Args:
        spark: SparkSession.
        path: Path to CSV file (or directory of CSVs).

    Returns:
        DataFrame with raw wide-format data.

    Raises:
        ExtractError: If file is missing or schema validation fails.
    """
    path_obj = Path(path)
    if not path_obj.exists():
        raise ExtractError(f"Input path does not exist: {path}")

    logger.info("Loading CSV from %s", path)
    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(path)
    )

    actual_columns = df.columns
    try:
        validate_raw_columns(actual_columns)
    except ValueError as e:
        raise ExtractError(str(e)) from e

    n = df.count()
    logger.info("Loaded %d rows, columns: %s", n, actual_columns)
    return df
