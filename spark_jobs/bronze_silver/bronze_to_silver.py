from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

spark = SparkSession.builder \
    .appName("BronzeToSilver") \
    .getOrCreate()


# -----------------------------
# 1. CUSTOMERS TABLE
# -----------------------------
def transform_customers(df):
    df = df.dropDuplicates()

    # remove null IDs
    df = df.dropna(subset=["customer_id"])

    # zipcode validation (numeric + length check)
    df = df.filter(col("customer_zip_code_prefix").rlike("^[0-9]{5,10}$"))

    return df


# -----------------------------
# 2. ORDERS TABLE
# -----------------------------
def transform_orders(df):
    df = df.dropDuplicates()

    # required columns must not be null
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

    # convert to timestamp (ensures consistency)
    for c in required_cols[2:]:
        df = df.withColumn(c, to_timestamp(col(c)))

    # logic validation: timestamps must follow order flow
    df = df.filter(
        (col("order_purchase_timestamp") <= col("order_approved_at")) &
        (col("order_approved_at") <= col("order_delivered_carrier_date")) &
        (col("order_delivered_carrier_date") <= col("order_delivered_customer_date"))
    )

    return df


# -----------------------------
# 3. ORDER ITEMS TABLE
# -----------------------------
def transform_order_items(df):
    df = df.dropDuplicates()

    df = df.dropna(subset=["order_id", "product_id", "price"])

    # price must be positive
    df = df.filter(col("price") > 0)

    return df


# -----------------------------
# 4. GENERIC CLEANING (OTHER TABLES)
# -----------------------------
def transform_generic(df):
    df = df.dropDuplicates()

    # remove null primary keys if exist
    if "id" in df.columns:
        df = df.dropna(subset=["id"])

    return df


# -----------------------------
# MAIN PIPELINE
# -----------------------------
def process_table(name):
    input_path = f"hdfs://namenode:9000/ecommerce/bronze/{name}"
    output_path = f"hdfs://namenode:9000/ecommerce/silver/{name}"

    df = spark.read.option("header", True).csv(input_path)

    if name == "customers":
        df = transform_customers(df)

    elif name == "orders":
        df = transform_orders(df)

    elif name == "order_items":
        df = transform_order_items(df)

    else:
        df = transform_generic(df)

    df.write.mode("overwrite").parquet(output_path)


# -----------------------------
# RUN ALL TABLES
# -----------------------------
tables = [
    "customers",
    "orders",
    "order_items",
    "products",
    "payments",
    "reviews",
    "sellers",
    "geolocation"
]

for t in tables:
    process_table(t)

spark.stop()