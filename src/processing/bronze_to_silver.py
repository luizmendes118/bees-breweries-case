from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os
import shutil

from delta import configure_spark_with_delta_pip

from src.utils.config import BRONZE_PATH, SILVER_PATH
from src.utils.logger import get_logger
from src.utils.schema import brewery_schema
from src.utils.data_quality import validate_breweries

logger = get_logger(__name__)


def transform_bronze_to_silver():

    builder = SparkSession.builder \
        .appName("BronzeToSilver") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    bronze_file = f"{BRONZE_PATH}/breweries_raw.json"

    logger.info("Reading bronze data")

    df = spark.read \
        .schema(brewery_schema) \
        .json(bronze_file)

    validate_breweries(df)

    logger.info("Cleaning dataset")

    df_clean = df.select(
        col("id"),
        col("name"),
        col("brewery_type"),
        col("city"),
        col("state"),
        col("country")
    )

    df_clean = df_clean.dropDuplicates(["id"])

    df_clean = df_clean.dropna(subset=["country", "state"])

    if os.path.exists(SILVER_PATH):
        shutil.rmtree(SILVER_PATH)

    os.makedirs(SILVER_PATH, exist_ok=True)

    logger.info("Writing silver dataset partitioned by location")

    df_clean.write \
        .format("delta") \
        .partitionBy("country", "state") \
        .mode("overwrite") \
        .save(SILVER_PATH)

    spark.stop()

    logger.info("Silver layer completed")
