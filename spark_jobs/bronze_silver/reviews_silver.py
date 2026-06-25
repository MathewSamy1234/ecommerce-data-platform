from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("ReviewsSilver") \
    .getOrCreate()

df = spark.read.option("header", True).csv(
    "hdfs://namenode:9000/ecommerce/bronze/reviews/reviews.csv"
)

df = (
    df
    .dropDuplicates()
    .dropna(subset=["review_id"])
    .filter((col("review_score") >= 1) & (col("review_score") <= 5))
)

df.write.mode("overwrite").parquet(
    "hdfs://namenode:9000/ecommerce/silver/reviews"
)

spark.stop()