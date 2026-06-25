from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("PaymentsSilver") \
    .getOrCreate()

df = spark.read.option("header", True).csv(
    "hdfs://namenode:9000/ecommerce/bronze/payments/payments.csv"
)

df = (
    df
    .dropDuplicates()
    .dropna(subset=["order_id"])
    .filter(col("payment_value") >= 0)
)

df.write.mode("overwrite").parquet(
    "hdfs://namenode:9000/ecommerce/silver/payments"
)

spark.stop()