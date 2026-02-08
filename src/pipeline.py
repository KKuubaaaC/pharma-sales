"""Pipeline orchestration: Extract -> Transform -> SQL Analytics -> Load."""
import logging
import time
from pyspark.sql import SparkSession

from src.config import get_data_path, get_output_base, load_config
from src.extract import extract_raw_data
from src.load import write_analytics_results, write_cleaned_parquet, write_run_metadata
from src.sql_analytics import run_all_analytics
from src.transform import transform_raw_data

logger = logging.getLogger(__name__)


def create_spark_session(config: dict) -> SparkSession:
    """Create SparkSession with optional config (e.g. shuffle partitions)."""
    builder = (
        SparkSession.builder.appName("PharmaSalesETL")
        .config("spark.sql.adaptive.enabled", "true")
    )
    shuffle = config.get("spark", {}).get("shuffle_partitions")
    if shuffle is not None:
        builder = builder.config("spark.sql.shuffle.partitions", str(shuffle))
    return builder.getOrCreate()


def run_pipeline(mode: str = "sample", config_path: str | None = None) -> None:
    """
    Run full ETL: extract from CSV, transform, run SQL analytics, load to Parquet.

    Args:
        mode: 'sample' (data/sample) or 'full' (data/raw).
        config_path: Optional path to YAML config; uses defaults if not provided.
    """
    config = load_config(config_path)
    data_path = get_data_path(config, mode)
    output_base = get_output_base(config)

    spark = create_spark_session(config)

    t0 = time.perf_counter()
    logger.info("Extract: %s", data_path)
    raw_df = extract_raw_data(spark, data_path)
    t1 = time.perf_counter()
    logger.info("Extract finished in %.2f s", t1 - t0)

    logger.info("Transform: unpivot, clean, time columns, season")
    cleaned_df = transform_raw_data(spark, raw_df)
    t2 = time.perf_counter()
    logger.info("Transform finished in %.2f s", t2 - t1)

    logger.info("SQL Analytics")
    analytics = run_all_analytics(spark, cleaned_df)
    t3 = time.perf_counter()
    logger.info("SQL Analytics finished in %.2f s", t3 - t2)

    logger.info("Load: Parquet and analytics")
    write_cleaned_parquet(cleaned_df, output_base)
    write_analytics_results(analytics, output_base)
    row_count = cleaned_df.count()
    write_run_metadata(output_base, mode, row_count)
    t4 = time.perf_counter()
    logger.info("Load finished in %.2f s", t4 - t3)
    logger.info("Total pipeline time: %.2f s", t4 - t0)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    run_pipeline(mode="sample")
