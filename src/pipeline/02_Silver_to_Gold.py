import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from pyspark.sql import SparkSession, DataFrame
from config import OUTPUT_DIR_GOLD, OUTPUT_DIR_SILVER
import pyspark.sql.functions as F
import logging


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def build_by_neighborhood(df_silver: DataFrame, min_listings: int = 2) -> DataFrame:
    """Aggregate prices by neighborhood with a configurable minimum sample size."""
    if not isinstance(min_listings, int) or isinstance(min_listings, bool) or min_listings < 1:
        raise ValueError('min_listings must be a positive integer')
    gold_by_neighborhood = df_silver.filter(
        F.col("Neighborhood").isNotNull()
    ).groupby(
        F.coalesce(F.col("City_Sector"), F.lit("Ilfov / Necunoscut")).alias("Sector"),
        F.col("Neighborhood")
    ).agg(
        F.count("listing_id").alias("Numar_Anunturi"),
        F.round(F.avg("Price_EUR"), 0).alias("Pret_Mediu_EUR"),
        F.round(F.avg("Price_per_sqm"), 0).alias("Pret_Mediu_MP_EUR"),
        F.round(F.min("Price_EUR"), 0).alias("Pret_Min_EUR"),
        F.round(F.max("Price_EUR"), 0).alias("Pret_Max_EUR")
    ).filter(
        F.col("Numar_Anunturi") >= min_listings
    ).orderBy(
        F.col("Pret_Mediu_MP_EUR").desc()
    )

    return gold_by_neighborhood


def build_rooms_distribution(df_silver: DataFrame) -> DataFrame:
    """Aggregates listing count and avg price per number of rooms, ordered ascending."""
    gold_rooms_distribution = (df_silver.filter(
        F.col("rooms").isNotNull()
    ).groupby(
        F.col("rooms")
    ).agg(
        F.count("listing_id").alias("Numar_Anunturi"),
        F.round(F.avg("Price_EUR"), 0).alias("Pret_Mediu_EUR"),
        F.round(F.avg("Price_per_sqm"), 0).alias("Pret_Mediu_MP_EUR")
    ).withColumnRenamed("rooms", "Numar_Camere").orderBy(
        F.col("Numar_Camere").asc()
    ))

    return gold_rooms_distribution


def build_market_summary(df_silver: DataFrame) -> DataFrame:
    """Produces a single-row global market snapshot: total listings, min/max/avg price."""
    gold_market_summary = df_silver.agg(
        F.count("listing_id").alias("Total_Anunturi"),
        F.round(F.avg("Price_EUR"), 0).alias("Pret_Mediu_EUR"),
        F.round(F.avg("Price_per_sqm"), 0).alias("Pret_Mediu_MP_EUR"),
        F.min("Price_EUR").alias("Pret_Min_EUR"),
        F.max("Price_EUR").alias("Pret_Max_EUR")
    )

    return gold_market_summary


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Silver_to_Gold_Processing") \
        .getOrCreate()
    # Avoid 200 default shuffle partitions — right-sized for ~3700 rows on 8 logical cores
    spark.conf.set("spark.sql.shuffle.partitions", "8")
    spark.sparkContext.setLogLevel("WARN")

    logger.info(f"[GOLD] Reading raw data from: {OUTPUT_DIR_SILVER}")
    df_silver = spark.read.parquet(OUTPUT_DIR_SILVER)
    silver_count = df_silver.count()
    logger.info(f"[GOLD] Rows loaded: {silver_count}")


    logger.info("[GOLD] Saving gold_by_neighborhood...")
    gold_by_neighborhood = build_by_neighborhood(df_silver)
    gold_by_neighborhood.write.mode("overwrite").parquet(f"{OUTPUT_DIR_GOLD}/by_neighborhood")
    gold_by_neighborhood.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{OUTPUT_DIR_GOLD}/by_neighborhood_csv")
    logger.info("[GOLD] Saved: by_neighborhood (Parquet + CSV)")

    logger.info("[GOLD] Saving gold_rooms_distribution...")
    gold_rooms_distribution = build_rooms_distribution(df_silver)
    gold_rooms_distribution.write.mode("overwrite").parquet(f"{OUTPUT_DIR_GOLD}/rooms_distribution")
    gold_rooms_distribution.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{OUTPUT_DIR_GOLD}/rooms_distribution_csv")
    logger.info("[GOLD] Saved: rooms_distribution (Parquet + CSV)")

    logger.info("[GOLD] Saving gold_market_summary...")
    gold_market_summary = build_market_summary(df_silver)
    gold_market_summary.write.mode("overwrite").parquet(f"{OUTPUT_DIR_GOLD}/market_summary")
    gold_market_summary.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{OUTPUT_DIR_GOLD}/market_summary_csv")
    logger.info("[GOLD] Saved: market_summary (Parquet + CSV)")

    logger.info("[GOLD] Pipeline complete.")

