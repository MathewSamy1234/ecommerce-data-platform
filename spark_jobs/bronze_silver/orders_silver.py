from pyspark.sql import SparkSession
from pyspark.sql.functions import col,to_timestamp

spark = SparkSession.builder \
    .appName("OrdersSilver") \
    .getOrCreate()

df = spark.read.option("header", True).csv(
    "hdfs://namenode:9000/ecommerce/bronze/orders/orders.csv"
)

df = df.dropDuplicates()

required_cols = [
    "order_id",
    "customer_id",
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date"
]

df = df.dropna(subset=required_cols)

for c in required_cols[2:]:
    df = df.withColumn(c, to_timestamp(col(c)))

df = df.filter(
    (col("order_purchase_timestamp") <= col("order_approved_at")) &
    (col("order_approved_at") <= col("order_delivered_carrier_date")) &
    (col("order_delivered_carrier_date") <= col("order_delivered_customer_date"))
)

df.write.mode("overwrite").parquet(
    "hdfs://namenode:9000/ecommerce/silver/orders"
)

spark.stop()