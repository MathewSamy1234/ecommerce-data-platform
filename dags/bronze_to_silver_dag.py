from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "mathe",
}

with DAG(
    dag_id="bronze_to_silver",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args
) as dag:

    customers = BashOperator(
        task_id="customers",
        bash_command="""
        docker exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark/jobs/customers_silver.py
        """
    )

    orders = BashOperator(
        task_id="orders",
        bash_command="""
        docker exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark/jobs/orders_silver.py
        """
    )

    order_items = BashOperator(
        task_id="order_items",
        bash_command="""
        docker exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark/jobs/order_items_silver.py
        """
    )

    products = BashOperator(
        task_id="products",
        bash_command="""
        docker exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark/jobs/products_silver.py
        """
    )

    payments = BashOperator(
        task_id="payments",
        bash_command="""
        docker exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark/jobs/payments_silver.py
        """
    )

    reviews = BashOperator(
        task_id="reviews",
        bash_command="""
        docker exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark/jobs/reviews_silver.py
        """
    )

    sellers = BashOperator(
        task_id="sellers",
        bash_command="""
        docker exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark/jobs/sellers_silver.py
        """
    )

    geolocation = BashOperator(
        task_id="geolocation",
        bash_command="""
        docker exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark/jobs/geolocation_silver.py
        """
    )

    order_reviews = BashOperator(
        task_id="order_reviews",
        bash_command="""
        docker exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark/jobs/order_reviews_silver.py
        """
    )

    [
        customers,
        orders,
        order_items,
        products,
        payments,
        reviews,
        sellers,
        geolocation,
        order_reviews
    ]