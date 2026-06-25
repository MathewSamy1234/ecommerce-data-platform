
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("DimCustomer")
    .getOrCreate()
)
customers = spark.read.parquet(
    "hdfs://namenode:9000/ecommerce/silver/customers"
)

dim_customers = customers.select(
    "customer_id",
    "customer_unique_id",
    "customer_city",
    "customer_state",
    "customer_zip_code_prefix"
).dropDuplicates()

dim_customers.write.mode("overwrite").parquet(
    "hdfs://namenode:9000/ecommerce/gold/dim_customers"
)