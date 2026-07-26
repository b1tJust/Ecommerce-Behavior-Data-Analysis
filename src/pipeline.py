import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, when, split, to_date, hour, dayofweek

from utils.spark_session import get_spark_session
from utils.config import RAW_DATA_PATH, PROCESSED_DATA_DIR, AGGREGATES_DIR, JDBC_URL, JDBC_PROPERTIES
from utils.schema import ec_schema

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def extract(spark: SparkSession) -> DataFrame:
    logger.info(f"Reading raw data from {RAW_DATA_PATH}")
    try:
        df = (
            spark.read
            .option("header", True)
            .schema(ec_schema)
            .csv(RAW_DATA_PATH)
        )
        row_count = df.count()
        logger.info(f"Read {row_count:,} rows")
        return df
    except Exception:
        logger.exception(f"Failed to read raw data from {RAW_DATA_PATH}")
        raise


def transform(df: DataFrame) -> DataFrame:
    logger.info("Starting transform step")

    before = df.count()
    df = df.dropDuplicates()
    after = df.count()
    logger.info(f"Dropped {before - after:,} duplicate rows ({before:,} -> {after:,})")

    df = df.withColumn("is_price_missing", when(col("price") == 0, True).otherwise(False))

    category_parts = split(col("category_code"), r"\.")
    df = (
        df
        .withColumn("category_l1", category_parts.getItem(0))
        .withColumn("category_l2", category_parts.getItem(1))
        .withColumn("category_l3", category_parts.getItem(2))
        .withColumn("category_l4", category_parts.getItem(3))
    )

    df = (
        df
        .withColumn("event_date", to_date(col("event_time")))
        .withColumn("event_hour", hour(col("event_time")))
        .withColumn("day_of_week", ((dayofweek(col("event_time")) + 5) % 7) + 1)
    )

    output_path = f"{PROCESSED_DATA_DIR}/events_clean.parquet"
    logger.info(f"Writing cleaned data to {output_path}")
    try:
        df.write.mode("overwrite").partitionBy("event_date").parquet(output_path)
    except Exception:
        logger.exception(f"Failed to write cleaned data to {output_path}")
        raise

    return df


def build_aggregates(spark: SparkSession, df: DataFrame) -> dict[str, DataFrame]:
    logger.info("Building aggregates")
    df.createOrReplaceTempView("events")

    funnel_daily = spark.sql("""
        SELECT
            event_date,
            event_type,
            COUNT(*) AS event_count
        FROM events
        GROUP BY event_date, event_type
        ORDER BY event_date, event_type
    """)

    category_sales = spark.sql("""
        SELECT
            category_l1,
            category_l2,
            brand,
            COUNT(*) AS purchase_count,
            ROUND(SUM(price), 2) AS total_revenue,
            ROUND(AVG(price), 2) AS avg_price,
            ROUND(PERCENTILE_APPROX(price, 0.5), 2) AS median_price
        FROM events
        WHERE event_type = 'purchase'
          AND is_price_missing = false
        GROUP BY category_l1, category_l2, brand
        ORDER BY total_revenue DESC
    """)

    user_metrics = spark.sql("""
        SELECT
            user_id,
            COUNT(DISTINCT user_session) AS session_count,
            SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchase_count,
            ROUND(SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0 END), 2) AS total_spent,
            ROUND(AVG(CASE WHEN event_type = 'purchase'
                AND is_price_missing = false THEN price END),
            2) AS avg_spent,
            ROUND(PERCENTILE_APPROX(
                CASE WHEN event_type = 'purchase'
                AND is_price_missing = false THEN price END, 0.5),
            2) AS median_spent,
            MAX(event_date) AS last_active_date
        FROM events
        GROUP BY user_id
    """)

    cart_abandonment = spark.sql("""
        WITH cart_events AS (
            SELECT DISTINCT user_session, product_id, category_l1, brand
            FROM events
            WHERE event_type = 'cart'
        ),
        purchase_events AS (
            SELECT DISTINCT user_session, product_id
            FROM events
            WHERE event_type = 'purchase'
        )
        SELECT
            c.category_l1,
            c.brand,
            COUNT(*) AS abandoned_count
        FROM cart_events c
        LEFT ANTI JOIN purchase_events p
            ON c.user_session = p.user_session AND c.product_id = p.product_id
        GROUP BY c.category_l1, c.brand
        ORDER BY abandoned_count DESC
    """)

    time_patterns = spark.sql("""
        SELECT
            day_of_week,
            event_hour,
            event_type,
            COUNT(*) AS event_count
        FROM events
        GROUP BY day_of_week, event_hour, event_type
        ORDER BY day_of_week, event_hour
    """)

    daily_summary = spark.sql("""
        SELECT
            event_date,
            COUNT(*) AS total_events,
            COUNT(DISTINCT user_id) AS unique_users,
            COUNT(DISTINCT user_session) AS unique_sessions,
            SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchase_count,
            ROUND(SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0 END), 2) AS revenue,
            ROUND(
                SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0 END)
                / NULLIF(SUM(CASE WHEN event_type = 'purchase' AND is_price_missing = false THEN 1 ELSE 0 END), 0),
            2) AS avg_check
        FROM events
        GROUP BY event_date
        ORDER BY event_date
    """)

    aggregates = {
        "funnel_daily": funnel_daily,
        "category_sales": category_sales,
        "user_metrics": user_metrics,
        "cart_abandonment": cart_abandonment,
        "time_patterns": time_patterns,
        "daily_summary": daily_summary,
    }

    for mart_name, mart_df in aggregates.items():
        output_path = f"{AGGREGATES_DIR}/{mart_name}.parquet"
        logger.info(f"Writing {mart_name} to {output_path}")
        try:
            mart_df.write.mode("overwrite").parquet(output_path)
        except Exception:
            logger.exception(f"Failed to write {mart_name} to {output_path}")
            raise

    return aggregates


def load_to_postgres(aggregates: dict[str, DataFrame]) -> None:
    logger.info("Loading marts into PostgreSQL")

    for mart_name, mart_df in aggregates.items():
        try:
            row_count = mart_df.count()
            mart_df.write.jdbc(url=JDBC_URL, table=mart_name, mode="overwrite", properties=JDBC_PROPERTIES)
            logger.info(f"{mart_name}: wrote {row_count:,} rows")
        except Exception:
            logger.exception(f"Failed to load {mart_name} into PostgreSQL")
            raise

    for mart_name, mart_df in aggregates.items():
        expected = mart_df.count()
        spark = mart_df.sparkSession
        actual = spark.read.jdbc(url=JDBC_URL, table=mart_name, properties=JDBC_PROPERTIES).count()
        status = "OK" if expected == actual else "MISMATCH"
        logger.info(f"{mart_name:<20} expected: {expected:>10,}  postgres: {actual:>10,}  [{status}]")
        if status == "MISMATCH":
            logger.error(f"Row count mismatch for {mart_name} — investigate before trusting downstream reports")


def run_pipeline() -> None:
    spark = get_spark_session()
    try:
        raw_df = extract(spark)
        clean_df = transform(raw_df)
        aggregates = build_aggregates(spark, clean_df)
        #load_to_postgres(aggregates)
        logger.info("Pipeline completed successfully")
    except Exception:
        logger.exception("Pipeline failed")
        raise
    finally:
        spark.stop()