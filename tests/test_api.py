import requests
from pyspark.sql import SparkSession
from src.utils.config import BASE_API_URL


def test_api_status():

    response = requests.get(BASE_API_URL)

    assert response.status_code == 200


def test_spark_session():

    spark = SparkSession.builder \
        .appName("test") \
        .master("local[1]") \
        .getOrCreate()

    data = [("brewery1", "micro"), ("brewery2", "brewpub")]

    df = spark.createDataFrame(data, ["name", "brewery_type"])

    assert df.count() == 2

    spark.stop()
