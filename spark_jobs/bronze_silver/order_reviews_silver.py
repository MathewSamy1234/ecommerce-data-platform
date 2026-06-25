from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("OrderReviewsSilver") \
    .getOrCreate()

df = spark.read.option("header", True).csv(
    "hdfs://namenode:9000/ecommerce/bronze/order_reviews/order_reviews.csv"
)

df = (
    df
    .dropDuplicates()
    .dropna(subset=["review_id","order_id"])
)

df.write.mode("overwrite").parquet(
    "hdfs://namenode:9000/ecommerce/silver/order_reviews"
)

spark.stop()