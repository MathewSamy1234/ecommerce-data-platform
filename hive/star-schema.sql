CREATE DATABASE IF NOT EXISTS gold;
CREATE EXTERNAL TABLE IF NOT EXISTS gold.dim_date (
    DateSK            INT,
    FullDate                 DATE,
    DayName               STRING,
    DaySuffix         STRING,
    DayOfWeek         STRING,
    DayOfWeekNumber   INT,
    DOWInMonth        INT,
    DayOfYear         INT,
    WeekOfYear        INT,
    WeekOfMonth       INT,
    MonthShort             STRING,
    MonthNumber       INT,
    MonthName         STRING,
    QuarterNumber           INT,
    QuarterName       STRING,
    CalendarYear              INT,
    StandardDate      STRING,
    IsWeekend         BOOLEAN,
    HolidayText       STRING
)
STORED AS PARQUET
LOCATION '/ecommerce/gold/dim_date';


CREATE EXTERNAL TABLE IF NOT EXISTS gold.dim_payments (
  payment_type   STRING,
  payment_id     BIGINT
)
STORED AS PARQUET
LOCATION '/ecommerce/gold/dim_payments';

CREATE EXTERNAL TABLE IF NOT EXISTS gold.dim_customers (
    customer_id                STRING,
    customer_unique_id         STRING,
    customer_city             STRING,
    customer_state            STRING,
    customer_zip_code_prefix   STRING
)
STORED AS PARQUET
LOCATION '/ecommerce/gold/dim_customers';

CREATE EXTERNAL TABLE IF NOT EXISTS gold.dim_sellers (
    seller_id                 STRING,
    seller_city              STRING,
    seller_state             STRING,
    seller_zip_code_prefix    STRING
)
STORED AS PARQUET
LOCATION '/ecommerce/gold/dim_seller';

CREATE EXTERNAL TABLE IF NOT EXISTS gold.dim_status (
    status_id      BIGINT,
    order_status   STRING
)
STORED AS PARQUET
LOCATION '/ecommerce/gold/dim_status';

CREATE EXTERNAL TABLE gold.dim_products (
   product_id STRING,
    product_category_name STRING,
    product_weight_g DOUBLE,
    product_length_cm DOUBLE,
    product_height_cm DOUBLE,
    product_width_cm DOUBLE
)
STORED AS PARQUET
LOCATION '/ecommerce/gold/dim_products';

CREATE EXTERNAL TABLE IF NOT EXISTS gold.fact_order_items (
    order_id                    STRING,
    order_item_id               INT,            -- Changed to INT for sequential mapping (e.g., 1, 2, 3)

    -- Dimension Keys
    customer_id                 STRING,
    product_id                  STRING,
    seller_id                   STRING,
    status_id                   BIGINT,         -- Added to map directly to gold.dim_status
    payment_id                  BIGINT,         -- Changed from payment_type string to match gold.dim_payments ID

    -- Date Keys
    purchase_date_sk            INT,
    approved_date_sk            INT,
    delivered_carrier_date_sk   INT,
    delivered_customer_date_sk  INT,
    estimated_delivery_date_sk  INT,

    -- Metrics & Measures
    payment_installments        INT,            -- Changed to INT for numerical operations
    payment_value               DECIMAL(10,2),  -- Changed to DECIMAL for accurate currency values
    review_score                INT,            -- Changed to INT for rating aggregations (averages, etc.)
    price                       DECIMAL(10,2),  -- Changed to DECIMAL for accurate currency values
    freight_value               DECIMAL(10,2)   -- Changed to DECIMAL for accurate currency values
)
STORED AS PARQUET
LOCATION '/ecommerce/gold/fact_order_items';