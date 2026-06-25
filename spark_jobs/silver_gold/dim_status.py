from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("dim_order_status")
    .getOrCreate()
)
orders = spark.read.parquet(
    "hdfs://namenode:9000/ecommerce/silver/orders"
)

dim_status = orders.select(
    "order_status"
).distinct()


from pyspark.sql.functions import monotonically_increasing_id

dim_status = dim_status.withColumn(
    "status_id",
    monotonically_increasing_id()
)


dim_status.write.mode("overwrite").parquet(
    "hdfs://namenode:9000/ecommerce/gold/dim_status"
)


