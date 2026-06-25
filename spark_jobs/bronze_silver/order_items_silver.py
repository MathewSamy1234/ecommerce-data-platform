from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("OrderItemsSilver") \
    .getOrCreate()

df = spark.read.option("header", True).csv(
    "hdfs://namenode:9000/ecommerce/bronze/order_items/order_items.csv"
)

df = (
    df
    .dropDuplicates()
    .dropna(subset=["order_id","product_id","price"])
    .filter(col("price") > 0)
)

df.write.mode("overwrite").parquet(
    "hdfs://namenode:9000/ecommerce/silver/order_items"
)

spark.stop()