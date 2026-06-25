from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("customers_silver") \
    .getOrCreate()

df = spark.read.option("header", True).csv(
    "hdfs://namenode:9000/ecommerce/bronze/customers/customers.csv"
)

df = df.dropDuplicates()

df = df.dropna(subset=["customer_id"])

df = df.filter(
    col("customer_zip_code_prefix")
    .rlike("^[0-9]{5,10}$")
)

df.write.mode("overwrite").parquet(
    "hdfs://namenode:9000/ecommerce/silver/customers"
)

spark.stop()