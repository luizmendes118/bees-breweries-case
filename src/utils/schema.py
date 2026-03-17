from pyspark.sql.types import StructType, StructField, StringType


brewery_schema = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("brewery_type", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("country", StringType(), True)
])
