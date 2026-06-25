
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("seller")
    .getOrCreate()
)

seller=spark.read.parquet("hdfs://namenode:9000/ecommerce/silver/sellers")

dim_seller=seller.select("seller_id","seller_city","seller_state","seller_zip_code_prefix")






dim_seller.write.mode("overwrite").parquet(
    "hdfs://namenode:9000/ecommerce/gold/dim_seller"
)
