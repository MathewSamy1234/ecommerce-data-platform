
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("fact")
    .getOrCreate()
)
from pyspark.sql import functions as F

orders = spark.read.parquet(
    "hdfs://namenode:9000/ecommerce/silver/orders"
)

order_items = spark.read.parquet(
    "hdfs://namenode:9000/ecommerce/silver/order_items"
)

payments = spark.read.parquet(
    "hdfs://namenode:9000/ecommerce/silver/payments"
)

order_reviews = spark.read.parquet(
    "hdfs://namenode:9000/ecommerce/silver/order_reviews"
)

customers = spark.read.parquet(
    "hdfs://namenode:9000/ecommerce/silver/customers"
)

products = spark.read.parquet(
    "hdfs://namenode:9000/ecommerce/silver/products"
)

sellers = spark.read.parquet(
    "hdfs://namenode:9000/ecommerce/silver/sellers"
)

# =====================================================
# FACT TABLE
# =====================================================

fact = (
    order_items
        .join(orders, "order_id")
        .join(payments, "order_id", "left")
        .join(order_reviews, "order_id", "left")
        .join(customers, "customer_id")
        .join(products, "product_id")
        .join(sellers, "seller_id")
)

fact = (
    fact
        .withColumn(
            "purchase_date_sk",
            F.date_format(
                F.to_date("order_purchase_timestamp"),
                "yyyyMMdd"
            ).cast("long")
        )
        .withColumn(
            "approved_date_sk",
            F.date_format(
                F.to_date("order_approved_at"),
                "yyyyMMdd"
            ).cast("long")
        )
        .withColumn(
            "delivered_carrier_date_sk",
            F.date_format(
                F.to_date("order_delivered_carrier_date"),
                "yyyyMMdd"
            ).cast("long")
        )
        .withColumn(
            "delivered_customer_date_sk",
            F.date_format(
                F.to_date("order_delivered_customer_date"),
                "yyyyMMdd"
            ).cast("long")
        )
        .withColumn(
            "estimated_delivery_date_sk",
            F.date_format(
                F.to_date("order_estimated_delivery_date"),
                "yyyyMMdd"
            ).cast("long")
        )
)

fact = fact.select(
    "order_id",
    "order_item_id",
    "customer_id",
    "product_id",
    "seller_id",

    "purchase_date_sk",
    "approved_date_sk",
    "delivered_carrier_date_sk",
    "delivered_customer_date_sk",
    "estimated_delivery_date_sk",

    "payment_type",

    F.col("payment_installments").cast("long")
        .alias("payment_installments"),

    F.col("payment_value").cast("double")
        .alias("payment_value"),

    F.col("review_score").cast("long")
        .alias("review_score"),

    F.col("price").cast("double")
        .alias("price"),

    F.col("freight_value").cast("double")
        .alias("freight_value")
)

fact.write.mode("overwrite").parquet(
    "hdfs://namenode:9000/ecommerce/gold/fact_order_items"
)