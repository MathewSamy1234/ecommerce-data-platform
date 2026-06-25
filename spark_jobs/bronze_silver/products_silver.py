from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ProductsSilver") \
    .getOrCreate()

df = spark.read.option("header", True).csv(
    "hdfs://namenode:9000/ecommerce/bronze/products/products.csv"
)

df = (
    df
    .dropDuplicates()
    .dropna(subset=["product_id"])
)

df.write.mode("overwrite").parquet(
    "hdfs://namenode:9000/ecommerce/silver/products"
)

spark.stop()