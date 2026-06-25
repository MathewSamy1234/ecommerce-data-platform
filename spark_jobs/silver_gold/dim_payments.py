
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("dimpayments")
    .getOrCreate()
)
payments = spark.read.parquet(
    "hdfs://namenode:9000/ecommerce/silver/payments"
)

dim_payments = payments.select(
    "payment_type"
).distinct()


from pyspark.sql.functions import monotonically_increasing_id

dim_payments = dim_payments.withColumn(
    "payment_id",
    monotonically_increasing_id()
)


dim_payments.write.mode("overwrite").parquet(
    "hdfs://namenode:9000/ecommerce/gold/dim_payments"
)


