from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("dimproducts")
    .getOrCreate()
)

products=spark.read.parquet("hdfs://namenode:9000/ecommerce/silver/products")

dim_products = products.select(
    "product_id",
    "product_category_name",
    F.col("product_weight_g").cast("double").alias("product_weight_g"),
    F.col("product_length_cm").cast("double").alias("product_length_cm"),
    F.col("product_height_cm").cast("double").alias("product_height_cm"),
    F.col("product_width_cm").cast("double").alias("product_width_cm")
)

dim_products.write.mode("overwrite").parquet(
    "hdfs://namenode:9000/ecommerce/gold/dim_products"
)
