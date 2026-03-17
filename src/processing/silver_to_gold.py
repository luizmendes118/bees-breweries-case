from pyspark.sql import SparkSession
from pyspark.sql.functions import count
import os
import shutil

from delta import configure_spark_with_delta_pip

from src.utils.config import SILVER_PATH, GOLD_PATH
from src.utils.logger import get_logger

logger = get_logger(__name__)


def transform_silver_to_gold():

    builder = SparkSession.builder \
        .appName("SilverToGold") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    logger.info("Reading silver dataset")

    df = spark.read.format("delta").load(SILVER_PATH)

    logger.info("Creating aggregations")

    df_gold = df.groupBy(
        "country",
        "state",
        "brewery_type"
    ).agg(
        count("*").alias("brewery_count")
    )

    if os.path.exists(GOLD_PATH):
        shutil.rmtree(GOLD_PATH)

    os.makedirs(GOLD_PATH, exist_ok=True)

    logger.info("Writing gold dataset partitioned by country")

    df_gold.write \
        .format("delta") \
        .partitionBy("country") \
        .mode("overwrite") \
        .save(GOLD_PATH)

    spark.stop()

    logger.info("Gold layer completed")
