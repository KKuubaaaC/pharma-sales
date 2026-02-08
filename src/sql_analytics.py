"""SQL analytics: register pharma_sales view and run analytical queries via spark.sql()."""
from pyspark.sql import DataFrame, SparkSession

from src.constants import PHARMA_SALES_VIEW


def register_pharma_sales_view(spark: SparkSession, df: DataFrame) -> None:
    """Register the transformed DataFrame as temp view for SQL queries."""
    df.createOrReplaceTempView(PHARMA_SALES_VIEW)


def run_top10_categories(spark: SparkSession) -> DataFrame:
    """Top 10 drug categories by total sales."""
    return spark.sql(
        f"""
        SELECT drug_category, SUM(sales) AS total_sales
        FROM {PHARMA_SALES_VIEW}
        GROUP BY drug_category
        ORDER BY total_sales DESC
        LIMIT 10
        """
    )


def run_quarterly_trend(spark: SparkSession) -> DataFrame:
    """Quarter-over-quarter trend: quarterly sales with previous quarter (LAG)."""
    return spark.sql(
        f"""
        SELECT year, quarter, quarterly_sales,
               LAG(quarterly_sales) OVER (ORDER BY year, quarter) AS prev_quarter
        FROM (
            SELECT year, quarter, SUM(sales) AS quarterly_sales
            FROM {PHARMA_SALES_VIEW}
            GROUP BY year, quarter
        ) t
        ORDER BY year, quarter
        """
    )


def run_seasonality_best_month(spark: SparkSession) -> DataFrame:
    """Best month per category by average monthly sales (seasonality)."""
    return spark.sql(
        f"""
        SELECT drug_category, month,
               AVG(sales) AS avg_monthly_sales
        FROM {PHARMA_SALES_VIEW}
        GROUP BY drug_category, month
        ORDER BY drug_category, avg_monthly_sales DESC
        """
    )


def run_yoy_growth(spark: SparkSession) -> DataFrame:
    """Year-over-year growth per category (percent change)."""
    return spark.sql(
        f"""
        SELECT drug_category, year, yearly_sales,
               ROUND(
                   (yearly_sales - LAG(yearly_sales) OVER (PARTITION BY drug_category ORDER BY year))
                   / NULLIF(LAG(yearly_sales) OVER (PARTITION BY drug_category ORDER BY year), 0) * 100,
                   2
               ) AS yoy_growth_pct
        FROM (
            SELECT drug_category, year, SUM(sales) AS yearly_sales
            FROM {PHARMA_SALES_VIEW}
            GROUP BY drug_category, year
        ) t
        ORDER BY drug_category, year
        """
    )


def run_anomalies(spark: SparkSession) -> DataFrame:
    """Days with sales > 2 standard deviations above mean (per category)."""
    return spark.sql(
        f"""
        WITH stats AS (
            SELECT drug_category,
                   AVG(sales) AS avg_sales,
                   STDDEV(sales) AS std_sales
            FROM {PHARMA_SALES_VIEW}
            GROUP BY drug_category
        )
        SELECT p.datum, p.drug_category, p.sales,
               s.avg_sales, s.std_sales,
               (p.sales - s.avg_sales) / NULLIF(s.std_sales, 0) AS z_score
        FROM {PHARMA_SALES_VIEW} p
        JOIN stats s ON p.drug_category = s.drug_category
        WHERE s.std_sales > 0 AND p.sales > s.avg_sales + 2 * s.std_sales
        ORDER BY z_score DESC
        """
    )


def run_all_analytics(spark: SparkSession, df: DataFrame) -> dict[str, DataFrame]:
    """Register given DataFrame as pharma_sales view, then run all analytics."""
    register_pharma_sales_view(spark, df)
    return {
        "top10_categories": run_top10_categories(spark),
        "quarterly_trend": run_quarterly_trend(spark),
        "seasonality_best_month": run_seasonality_best_month(spark),
        "yoy_growth": run_yoy_growth(spark),
        "anomalies": run_anomalies(spark),
    }
