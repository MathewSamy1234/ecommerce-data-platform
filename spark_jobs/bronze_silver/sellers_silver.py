from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("SellersSilver") \
    .getOrCreate()

df = spark.read.option("header", True).csv(
    "hdfs://namenode:9000/ecommerce/bronze/sellers/sellers.csv"
)

df = (
    df
    .dropDuplicates()
    .dropna(subset=["seller_id"])
)

df.write.mode("overwrite").parquet(
    "hdfs://namenode:9000/ecommerce/silver/sellers"
)

spark.stop()