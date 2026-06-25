#!/bin/bash

# Create Bronze folders

hdfs dfs -mkdir -p /ecommerce/bronze/customers
hdfs dfs -mkdir -p /ecommerce/bronze/orders
hdfs dfs -mkdir -p /ecommerce/bronze/order_items
hdfs dfs -mkdir -p /ecommerce/bronze/products
hdfs dfs -mkdir -p /ecommerce/bronze/sellers
hdfs dfs -mkdir -p /ecommerce/bronze/payments
hdfs dfs -mkdir -p /ecommerce/bronze/reviews
hdfs dfs -mkdir -p /ecommerce/bronze/geolocation
hdfs dfs -mkdir -p /ecommerce/bronze/product_category_translation

# Upload files

hdfs dfs -put -f /tmp/data/olist_customers_dataset.csv \
/ecommerce/bronze/customers/

hdfs dfs -put -f /tmp/data/olist_orders_dataset.csv \
/ecommerce/bronze/orders/

hdfs dfs -put -f /tmp/data/olist_order_items_dataset.csv \
/ecommerce/bronze/order_items/

hdfs dfs -put -f /tmp/data/olist_products_dataset.csv \
/ecommerce/bronze/products/

hdfs dfs -put -f /tmp/data/olist_sellers_dataset.csv \
/ecommerce/bronze/sellers/

hdfs dfs -put -f /tmp/data/olist_order_payments_dataset.csv \
/ecommerce/bronze/payments/

hdfs dfs -put -f /tmp/data/olist_order_reviews_dataset.csv \
/ecommerce/bronze/reviews/

hdfs dfs -put -f /tmp/data/olist_geolocation_dataset.csv \
/ecommerce/bronze/geolocation/

hdfs dfs -put -f /tmp/data/product_category_name_translation.csv \
/ecommerce/bronze/product_category_translation/