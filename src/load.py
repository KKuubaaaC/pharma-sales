"""Load: write cleaned DataFrame and analytics results to Parquet/CSV."""
import json
import logging
from pathlib import Path
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


def write_cleaned_parquet(df: DataFrame, output_base: str) -> str:
    """
    Write cleaned DataFrame partitioned by year, month. Idempotent overwrite.

    Returns:
        Path where data was written.
    """
    path = str(Path(output_base) / "cleaned_data")
    logger.info("Writing cleaned data to %s (partitioned by year, month)", path)
    df.write.mode("overwrite").partitionBy("year", "month").parquet(path)
    n = df.count()
    logger.info("Wrote %d rows to %s", n, path)
    return path


def write_analytics_results(
    analytics: dict[str, DataFrame],
    output_base: str,
    write_csv: bool = False,
) -> None:
    """Write each analytics DataFrame to output_base/analytics/<name> (parquet, optionally CSV)."""
    base = Path(output_base) / "analytics"
    base.mkdir(parents=True, exist_ok=True)
    for name, df in analytics.items():
        path = base / name
        df.write.mode("overwrite").parquet(str(path))
        n = df.count()
        logger.info("Wrote analytics %s: %d rows to %s", name, n, path)
        if write_csv:
            csv_path = base / f"{name}.csv"
            df.coalesce(1).write.mode("overwrite").option("header", "true").csv(str(csv_path))
            logger.info("Wrote %s CSV to %s", name, csv_path)


def write_run_metadata(output_base: str, mode: str, row_count: int) -> None:
    """Write a small JSON with run timestamp, mode, and row count."""
    path = Path(output_base) / "run_metadata.json"
    data = {
        "run_at": datetime.utcnow().isoformat() + "Z",
        "mode": mode,
        "cleaned_row_count": row_count,
    }
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    logger.info("Wrote metadata to %s", path)
