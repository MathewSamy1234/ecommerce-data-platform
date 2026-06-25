from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("GeolocationSilver") \
    .getOrCreate()

df = spark.read.option("header", True).csv(
    "hdfs://namenode:9000/ecommerce/bronze/geolocation/geolocation.csv"
)

df = (
    df
    .dropDuplicates()
    .filter(
        (col("geolocation_lat") >= -90) &
        (col("geolocation_lat") <= 90) &
        (col("geolocation_lng") >= -180) &
        (col("geolocation_lng") <= 180)
    )
)

df.write.mode("overwrite").parquet(
    "hdfs://namenode:9000/ecommerce/silver/geolocation"
)

spark.stop()